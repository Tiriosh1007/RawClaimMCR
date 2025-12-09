
import re
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple
import pandas as pd
import numpy as np
from collections import defaultdict

class AIALossRatioConvert:
    """
    API for converting insurer 'Group Medical Claim Ratio' Excel reports
    into a normalized CSV with aggregated benefit types and multi-period support.

    Key rules implemented:
    1) If a header contains "Claim Paid" but NOT "IBNR", then ibnr = "0%".
    2) loss_ratio is ALWAYS numeric: actual_paid_w_ibnr / actual_premium (no % string).
    3) Multi-period: detects every 'From ... To ...' period and ties product blocks to the
       closest period above them.
     4) Hospital aggregates: HMO-H+S + HMO-H+S W + HMO-WART-S -> "Hospital".
         Clinical: HMO-CLINIC -> "Clinical".
         Dental: HMO-DENTAL -> "Dental".
         Top-Up/ SMM: HMO-SMM RL -> "Top-Up/ SMM".
         Total: Total -> "Total".
    """

    def __init__(
        self,
        benefit_mapping: Optional[Dict[str, Iterable[str]]] = None,
        sheet_name: str = "Sheet1"
    ) -> None:
        # Default mapping, can be overridden
        self.benefit_mapping = benefit_mapping or {
            "Hospital": {"HMO-H+S", "HMO-H+S W", "HMO-WART-S"},
            "Clinical": {"HMO-CLINIC"},
            "Dental": {"HMO-DENTAL"},
            "Top-Up/ SMM": {"HMO-SMM RL"},
            "Total": {"Total"},
        }
        self.sheet_name = sheet_name

    # ---------------------- Static helpers ----------------------
    @staticmethod
    def _parse_period(text: str) -> Optional[Tuple[datetime.date, datetime.date]]:
        if not isinstance(text, str):
            text = str(text)
        m = re.search(
            r"From\s+(\d{1,2}/\d{1,2}/\d{2,4})\s+To\s+(\d{1,2}/\d{1,2}/\d{2,4})",
            text, re.I
        )
        if not m:
            return None
        def to_date(s: str):
            for fmt in ("%m/%d/%Y", "%m/%d/%y", "%d/%m/%Y", "%d/%m/%y"):
                try:
                    return datetime.strptime(s, fmt).date()
                except Exception:
                    pass
            return None
        return to_date(m.group(1)), to_date(m.group(2))

    @staticmethod
    def _months_inclusive(d1, d2) -> Optional[int]:
        if not d1 or not d2:
            return None
        return (d2.year - d1.year) * 12 + (d2.month - d1.month) + 1

    @staticmethod
    def _dmy_short(d) -> str:
        return "" if not d else f"{d.day}/{d.month}/{str(d.year)[-2:]}"

    @staticmethod
    def _to_float(x) -> Optional[float]:
        try:
            return float(str(x).replace(",", ""))
        except Exception:
            try:
                return float(x)
            except Exception:
                return None

    # ---------------------- Internal parsing helpers ----------------------
    def _load_raw(self, in_path: str) -> pd.DataFrame:
        return pd.read_excel(in_path, sheet_name=self.sheet_name, header=None)

    def _policy_and_client(self, raw: pd.DataFrame) -> Tuple[str, str]:
        policy_line = raw.iat[2, 2]
        m = re.match(r"\s*([A-Za-z0-9\-]+)\s+(.*)", str(policy_line).strip())
        policy_number = m.group(1) if m else str(policy_line).strip()
        client_name = (m.group(2) if m and m.group(2) else "").strip()
        return policy_number, client_name

    def _find_period_rows(self, raw: pd.DataFrame) -> List[Dict]:
        period_rows = []
        for r in range(len(raw)):
            row_vals = raw.iloc[r, :min(20, raw.shape[1])].astype(str).tolist()
            line = " ".join([x for x in row_vals if x and x.lower() != 'nan'])
            per = self._parse_period(line)
            if per:
                period_rows.append({"row": r, "start": per[0], "end": per[1]})
        period_rows.sort(key=lambda x: x["row"])
        return period_rows

    def _find_product_headers(self, raw: pd.DataFrame) -> List[int]:
        first_col = raw.iloc[:, 0].astype(str).str.strip().str.upper()
        return raw.index[first_col.eq("PRODUCT NAME")].tolist()

    def _read_product_block(self, raw: pd.DataFrame, start_r: int) -> Dict:
        """
        Read a product block starting at header row 'start_r' and return:
        {
           "items": [{"product":..., "premium":..., "claim_paid_ibnr":...}, ...],
           "ibnr": "<pct-str>" or "0%" or None,
           "end_row": <int>
        }
        """
        max_c = raw.shape[1]
        header_texts = [str(x) for x in raw.iloc[start_r, :max_c].tolist()]

        # Build a case-insensitive header map
        header_map = {}
        for c in range(max_c):
            name = raw.iat[start_r, c]
            if isinstance(name, str) and name.strip():
                header_map[name.strip().lower()] = c

        # IBNR detection per block
        ibnr_pct_block = None
        contains_claim = any("claim paid" in h.lower() for h in header_texts if isinstance(h, str))
        contains_ibnr = any(("claim paid" in h.lower() and "ibnr" in h.lower()) for h in header_texts if isinstance(h, str))
        if contains_claim:
            if contains_ibnr:
                joined = " ".join(header_texts)
                m = re.search(r"([\d\.]+)\s*%?\s*IBNR", joined, re.I)
                if m:
                    ibnr_pct_block = f"{m.group(1)}%"
            else:
                ibnr_pct_block = "0%"

        # Read block rows
        items, r = [], start_r + 1
        while r < len(raw):
            first = raw.iat[r, 0]
            second = raw.iat[r, 1] if raw.shape[1] > 1 else None
            txtline = " ".join([str(x) for x in raw.iloc[r, :min(8, max_c)].tolist() if pd.notna(x)])
            stop = (
                (pd.isna(first) and pd.isna(second)) or
                re.search(r"Period|Anniversary|Data as of|Claimant|Product Name", txtline, re.I)
            )
            if stop:
                break
            prod_name = str(first).strip() or "Total"

            def get_by_title(substr: str) -> Optional[float]:
                for key, cidx in header_map.items():
                    if substr in key:
                        return self._to_float(raw.iat[r, cidx])
                return None

            items.append({
                "product": prod_name,
                "premium": get_by_title("premium"),
                "claim_paid_ibnr": get_by_title("claim paid"),
            })
            r += 1

        return {"items": items, "ibnr": ibnr_pct_block, "end_row": r}

    @staticmethod
    def _collapse_ibnr(lst: List[str]) -> str:
        for x in lst:
            if isinstance(x, str) and x.strip():
                return x
        return "0%"

    # ---------------------- Public API ----------------------
    def parse(self, in_path: str) -> pd.DataFrame:
        """Parse the Excel file and return a normalized DataFrame."""
        raw = self._load_raw(in_path)
        policy_number, client_name = self._policy_and_client(raw)
        period_rows = self._find_period_rows(raw)
        product_hdr_rows = self._find_product_headers(raw)

        # Collect per-period totals
        by_period = defaultdict(lambda: {"premium": {}, "claim": {}, "ibnr_list": []})

        for hdr in product_hdr_rows:
            block = self._read_product_block(raw, hdr)
            items = block["items"]
            ibnr_block = block["ibnr"]

            # Attach to nearest period above
            above = [p for p in period_rows if p["row"] <= hdr]
            if not above:
                continue
            per = above[-1]
            per_key = (per["start"], per["end"])

            if ibnr_block:
                by_period[per_key]["ibnr_list"].append(ibnr_block)

            has_claim = any(it["claim_paid_ibnr"] is not None for it in items)
            has_premium = any(it["premium"] is not None for it in items)

            for it in items:
                name = it["product"]
                if has_premium and it["premium"] is not None:
                    by_period[per_key]["premium"][name] = by_period[per_key]["premium"].get(name, 0.0) + float(it["premium"])
                if has_claim and it["claim_paid_ibnr"] is not None:
                    by_period[per_key]["claim"][name] = by_period[per_key]["claim"].get(name, 0.0) + float(it["claim_paid_ibnr"])

        # Build output
        rows = []
        for (st, en), parts in sorted(by_period.items(), key=lambda kv: (kv[0][0], kv[0][1])):
            duration = self._months_inclusive(st, en)
            policy_id = f"{policy_number}_{st.year:04d}{st.month:02d}" if st else f"{policy_number}_"
            ibnr_pct = self._collapse_ibnr(parts["ibnr_list"])
            for label, names in self.benefit_mapping.items():
                prem = sum(parts["premium"].get(n, 0.0) for n in names)
                claim = sum(parts["claim"].get(n, 0.0) for n in names)
                loss_ratio = (claim / prem) if prem else np.nan  # numeric float

                rows.append({
                    "policy_id": policy_id,
                    "policy_number": policy_number,
                    "client_name": client_name,
                    "policy_start_date": self._dmy_short(st),
                    "policy_end_date": self._dmy_short(en),
                    "duration": duration if duration is not None else "",
                    "ibnr": ibnr_pct,
                    "data_as_of": self._dmy_short(en),
                    "benefit_type": label,
                    "actual_premium": round(prem, 2),
                    "actual_paid_w_ibnr": round(claim, 2),
                    "loss_ratio": loss_ratio,  # keep numeric
                })

        out_df = pd.DataFrame(rows, columns=[
            "policy_id","policy_number","client_name","policy_start_date","policy_end_date","duration","ibnr","data_as_of",
            "benefit_type","actual_premium","actual_paid_w_ibnr","loss_ratio"
        ])
        # Ensure numeric dtype for Excel friendliness
        out_df["loss_ratio"] = pd.to_numeric(out_df["loss_ratio"], errors="coerce")
        out_df["actual_premium"] = pd.to_numeric(out_df["actual_premium"], errors="coerce")
        out_df["actual_paid_w_ibnr"] = pd.to_numeric(out_df["actual_paid_w_ibnr"], errors="coerce")
        return out_df

    def to_csv(self, df: pd.DataFrame, out_path: str) -> None:
        df.to_csv(out_path, index=False)

    def run(self, in_path: str, out_path: str) -> pd.DataFrame:
        """Convenience method: parse and save CSV; returns the DataFrame."""
        df = self.parse(in_path)
        self.to_csv(df, out_path)
        return df


# ---- Example usage (uncomment to run as a script) ----
# if __name__ == "__main__":
#     parser = ClaimRatioParser()
#     in_file = "/mnt/data/MedicalBasic-0000012715 (01012022-06302024).xlsx"
#     out_file = "/mnt/data/converted_claim_ratio_api.csv"
#     df = parser.run(in_file, out_file)
#     print(f"Saved: {out_file}")
