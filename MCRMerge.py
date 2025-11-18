import pandas as pd
from importlib import import_module
from pathlib import Path
from typing import List, Dict, Optional, Tuple


class MCRMerger:
    """
    Merge MCR Excel workbooks exported by RawClaimData.mcr_pages into a single, merged MCR.

    Contract
    - Inputs: one or more MCR Excel files produced by this app (sheets like P.20_*, P.21_*, P.22_*, P.25_*, P.26_*).
    - Configuration: user-defined merge groups mapping multiple (policy_number, year, class) to a new
      (merged_policy_number, merged_year, merged_class).
    - Output: new in-memory Excel with the same core pages rebuilt from merged base measures and
      ratios recomputed (usage_ratio, per-case/claim/claimant metrics).

    Notes
    - Uses P.22_Class_BenefitType and P.25_Class_Panel_BenefitType as primary sources for base measures.
    - Falls back gracefully if some pages are missing; it will only generate sheets it can construct.
    - For outpatient panel detail, P.26_OP_Panel_Benefit is merged if available.
    """

    SUPPORTED_SHEETS = [
        'Policy_Info',
        'P.20_Policy', 'P.20_BenefitType', 'P.20_Benefit_DepType', 'P.20_Network', 'P.20_Network_BenefitType', 'P.20_Day_Prod', 'P.20_Day_Prod_Class',
        'P.21_Class', 'P.22_Class_BenefitType', 'P.22_Class_DepType',
        'P.23_IP_Benefit', 'P.23a_Class_IP_Benefit', 'P.23b_Common_Diagnosis_IP', 'P.23b_Class_Common_Diag_IP',
        'P.24_OP_Benefit', 'P.24a_Class_OP_Benefit', 'P.24d_Dental', 'P.24w_Wellness', 'P.24wc_Class_Wellness',
        'P.25_Class_Panel_BenefitType',
        'P.26_OP_Panel_Benefit', 'P.26a_OP_Class_Panel_Benefit', 'P.26b_IP_Panel_Benefit',
        'P.27_TimeSeries', 'P.27a_TimeSeries_IP', 'P.27b_TimeSeries_OP',
        'P.28_Hospital', 'P.28_Provider', 'P.28a_Provider_Benefit', 'P.28_Physician', 'P.28a_Physician_Benefit', 'P.28_Procedures', 'P.28a_Procedures_Diag', 'P.28b_Procedures_Network',
        'P.29_SP_Speciality', 'P.29_SP_Spec_Diag', 'P.29_Grp_Procedure', 'P.29_Grp_Proce_Org', 'P.29b_Grp_Proce_Network',
        'P.18a_Class_TopHosDiag', 'P.18_TopHosDiag', 'P.18b_Class_TopClinDiag', 'P.18b_TopClinDiag', 'P.18b_TopNetClinDiag',
        'P.18b_Class_IP_DayProc', 'P.18b_IP_DayProc',
    ]

    BASE_COLS = [
        "incurred_amount",
        "paid_amount",
        "no_of_cases",
        "no_of_claim_id",
        "no_of_claimants",
    ]

    NUMERIC_COLS = [
        "incurred_amount",
        "paid_amount",
        "usage_ratio",
        "no_of_cases",
        "incurred_per_case",
        "paid_per_case",
        "no_of_claimants",
        "incurred_per_claimant",
        "paid_per_claimant",
        "no_of_claim_id",
        "claim_frequency",
    ]

    def __init__(self):
        self._files: List[Dict] = []
        # catalog rows: (source_key, file, policy_number, year, class)
        self._catalog: Optional[pd.DataFrame] = None
        # remember first-seen schema (column order) per sheet to align output to RawClaimData
        self._sheet_schema: Dict[str, List[str]] = {}
        # cache inpatient benefit ordering so we only read the index once
        self._ip_benefit_order: Optional[List[str]] = None

    @staticmethod
    def _normalize_benefit_type_values(df: pd.DataFrame, col: str = 'benefit_type') -> pd.DataFrame:
        """Normalize benefit_type naming to RawClaimData conventions.

        - Map common variants to canonical labels: Hospital, Clinic, Dental, Optical, Maternity, Total
        - Leaves unknowns untouched, preserving genuine NaN values for downstream handling.
        """
        if df is None or df.empty or col not in df.columns:
            return df

        d = df.copy()
        series = d[col].astype("string")
        series = series.str.strip()
        low = series.str.casefold()

        # Normalize 'Total' style values first
        is_total = low.isin({"total", "all", "overall"})
        series.loc[is_total] = "Total"

        # Clinic variants
        is_clinic = low.str.contains(r"\bclin|\boutp|\bop\b", regex=True, na=False)
        series.loc[is_clinic & ~is_total] = "Clinic"

        # Hospital variants
        is_hosp = low.str.contains(r"hosp|\binp|\bip\b|inpatient", regex=True, na=False)
        series.loc[is_hosp & ~is_total] = "Hospital"

        # Dental
        is_dent = low.str.contains(r"dent", regex=True, na=False)
        series.loc[is_dent & ~is_total] = "Dental"

        # Optical
        is_opt = low.str.contains(r"optic|optical|vision", regex=True, na=False)
        series.loc[is_opt & ~is_total] = "Optical"

        # Maternity
        is_mat = low.str.contains(r"mat", regex=True, na=False)
        series.loc[is_mat & ~is_total] = "Maternity"

        # Restore genuine NA for blanks/placeholder strings
        na_mask = series.isna() | (series == "") | (series.str.casefold() == "nan") | (series.str.casefold() == "none")
        series.loc[na_mask] = pd.NA

        d[col] = series.astype(object)
        return d

    @staticmethod
    def _coalesce_missing_benefit_type(
        df: pd.DataFrame,
        base_cols: List[str],
        extra_group_cols: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Assign missing benefit_type entries to 'Clinic' and collapse duplicates."""
        if df is None or df.empty or 'benefit_type' not in df.columns:
            return df

        work = df.copy()
        bt = work['benefit_type']
        missing_mask = bt.isna() | bt.astype("string").str.strip().eq("") | bt.astype("string").str.casefold().isin({"nan", "none"})
        if not missing_mask.any():
            return work

        work.loc[missing_mask, 'benefit_type'] = 'Clinic'
        extra_group_cols = extra_group_cols or []
        group_cols = [c for c in ['policy_number', 'year'] + extra_group_cols + ['benefit_type'] if c in work.columns]
        if not base_cols or not group_cols:
            return work

        grouped = work.groupby(group_cols, dropna=False)[base_cols].sum().reset_index()
        return grouped

    @staticmethod
    def _ensure_benefit_type_rows(
        df: pd.DataFrame,
        base_cols: List[str],
        order: List[str],
        extra_group_cols: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Ensure each group contains rows for the canonical benefit_type order."""

        extra_group_cols = extra_group_cols or []
        required_cols = {'policy_number', 'year', 'benefit_type', *extra_group_cols}
        if df is None or df.empty or not required_cols.issubset(df.columns):
            return df

        work = df.copy()
        work['benefit_type'] = work['benefit_type'].astype("string").str.strip()

        combo_cols = ['policy_number', 'year'] + extra_group_cols
        combos = work[combo_cols].drop_duplicates(ignore_index=True)
        pieces = []
        for _, row in combos.iterrows():
            mask = (work['policy_number'] == row['policy_number']) & (work['year'] == row['year'])
            for col in extra_group_cols:
                mask &= work[col] == row[col]
            subset = work.loc[mask].copy()
            present = subset['benefit_type'].dropna().tolist()
            missing = [bt for bt in order if bt not in present]
            if missing:
                filler = pd.DataFrame({
                    'policy_number': row['policy_number'],
                    'year': row['year'],
                    'benefit_type': missing,
                })
                for col in extra_group_cols:
                    filler[col] = row[col]
                for col in base_cols:
                    if col in work.columns:
                        filler[col] = 0.0
                subset = pd.concat([subset, filler], ignore_index=True, sort=False)
            subset = MCRMerger._apply_order(subset, 'benefit_type', order)
            pieces.append(subset)

        combined = pd.concat(pieces, ignore_index=True)
        return combined

    def _build_p20_benefit_page(self, df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """Aggregate benefit type totals using required columns and filters."""
        if df is None or df.empty:
            return None

        work = self._normalize_benefit_type_values(df, 'benefit_type').copy()
        if 'benefit_type' not in work.columns:
            return None

        bt_series = work['benefit_type'].astype('string')
        missing_mask = bt_series.isna() | bt_series.str.strip().eq('') | bt_series.str.casefold().isin({'nan', 'none'})
        work.loc[missing_mask, 'benefit_type'] = 'Clinic'

        mask_total = work['benefit_type'].astype(str).str.strip().str.casefold().isin({'total', 'all', 'overall'})
        typed = work.loc[~mask_total].copy()
        if typed.empty:
            return None

        required_ids = {'policy_number', 'year', 'benefit_type'}
        if not required_ids.issubset(typed.columns):
            return None

        base_cols = ['incurred_amount', 'paid_amount', 'no_of_cases', 'no_of_claimants', 'no_of_claim_id']
        prepared_cols: List[str] = []
        for col in base_cols:
            if col not in typed.columns:
                typed[col] = 0.0
            else:
                typed[col] = pd.to_numeric(typed[col], errors='coerce').fillna(0)
            prepared_cols.append(col)

        grouped = typed.groupby(['policy_number', 'year', 'benefit_type'], dropna=False)[prepared_cols].sum().reset_index()
        if grouped.empty:
            return None

        totals = grouped.groupby(['policy_number', 'year'], dropna=False)[prepared_cols].sum().reset_index()
        totals['benefit_type'] = 'Total'
        combined = pd.concat([grouped, totals], ignore_index=True)

        combined = self._recalc_ratios(combined)
        combined = self._apply_order(combined, 'benefit_type', ["Hospital", "Clinic", "Dental", "Optical", "Maternity", "Total"])

        zero_mask = (
            combined['incurred_amount'].fillna(0).eq(0) &
            combined['paid_amount'].fillna(0).eq(0) &
            combined['no_of_cases'].fillna(0).eq(0)
        )
        combined = combined[~zero_mask]
        if combined.empty:
            return None

        needed = [
            'policy_number', 'year', 'benefit_type',
            'incurred_amount', 'paid_amount', 'usage_ratio',
            'no_of_cases', 'incurred_per_case', 'paid_per_case',
            'no_of_claimants', 'no_of_claim_id',
        ]
        for col in needed:
            if col not in combined.columns:
                combined[col] = pd.NA

        return combined[needed].reset_index(drop=True)

    @staticmethod
    def _standardize_col_order(df: pd.DataFrame) -> pd.DataFrame:
        """Return a view with a consistent column ordering across pages.

        Order priorities:
        1) Identifiers: policy_number, year, class
        2) Common dimensions: benefit_type, panel, benefit
        3) Other non-numeric descriptor columns (stable order as they appear)
        4) Base numeric columns (incurred_amount, paid_amount, no_of_cases, no_of_claim_id, no_of_claimants)
        5) Derived ratios/metrics (usage_ratio, incurred/paid per case/claimant/claim, claim_frequency)
        6) Any remaining columns
        """
        if df is None or df.empty:
            return df
        cols = list(df.columns)
        id_cols = [c for c in ["policy_number", "year", "class"] if c in cols]
        dim_cols = [c for c in ["benefit_type", "panel", "benefit"] if c in cols]
        # Other non-numeric descriptor columns (exclude already picked)
        picked = set(id_cols + dim_cols)
        other_desc = []
        for c in cols:
            if c in picked:
                continue
            if not pd.api.types.is_numeric_dtype(df[c]):
                other_desc.append(c)
        base_cols = [c for c in [
            "incurred_amount", "paid_amount", "no_of_cases", "no_of_claim_id", "no_of_claimants"
        ] if c in cols]
        ratio_cols = [c for c in [
            "usage_ratio",
            "incurred_per_case", "paid_per_case",
            "incurred_per_claimant", "paid_per_claimant",
            "incurred_per_claim", "paid_per_claim",
            "claim_frequency"
        ] if c in cols]
        # Remaining columns
        ordered = id_cols + dim_cols + other_desc + base_cols + ratio_cols
        remaining = [c for c in cols if c not in ordered]
        ordered += remaining
        return df[ordered]

    def add_file(self, file_like_or_path, name: Optional[str] = None):
        """Load an MCR workbook into memory. Accepts a file path or a file-like object.

        Stores:
        - sheets: dict of DataFrame by sheet name (only supported sheets are read if present)
        - name: file name or provided label
        """
        xls = pd.ExcelFile(file_like_or_path)
        sheets = {}
        for sn in xls.sheet_names:
            if sn in self.SUPPORTED_SHEETS:
                try:
                    df = pd.read_excel(xls, sheet_name=sn, dtype=str)
                except Exception:
                    df = pd.read_excel(xls, sheet_name=sn)
                # standardize columns (strip, lower conflicts avoided)
                df.columns = [c.strip() for c in df.columns]
                # coerce known numeric
                for col in self.NUMERIC_COLS:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                # year may be numeric or string in inputs
                if "year" in df.columns:
                    df["year"] = df["year"].astype(str)
                # collapse duplicate-named columns early to avoid downstream alignment issues
                df = MCRMerger._dedupe_columns_frame(df)
                sheets[sn] = df
                # remember schema for export alignment
                if sn not in self._sheet_schema:
                    self._sheet_schema[sn] = list(df.columns)
        self._files.append({
            "name": name or getattr(file_like_or_path, "name", "uploaded_mcr.xlsx"),
            "sheets": sheets,
        })
        self._catalog = None  # invalidate

    def _safe_cols(self, df: pd.DataFrame, cols: List[str]) -> List[str]:
        return [c for c in cols if c in df.columns]

    @staticmethod
    def _get_numeric_series(df: pd.DataFrame, col: str) -> Optional[pd.Series]:
        """Return a numeric Series for a given column name.

        - If the column doesn't exist, return None.
        - If duplicate columns exist with the same name, sum numeric duplicates; otherwise take the first.
        - Coerce to numeric dtype safely.
        """
        if col not in df.columns:
            return None
        s = df[col]
        # If duplicate columns produce a DataFrame, coalesce
        if isinstance(s, pd.DataFrame):
            # prefer summing if all numeric; else take first non-null across columns
            if all(pd.api.types.is_numeric_dtype(s[c]) for c in s.columns):
                coalesced = s.sum(axis=1, min_count=1)
            else:
                coalesced = s.bfill(axis=1).iloc[:, 0]
        else:
            coalesced = s
        return pd.to_numeric(coalesced, errors="coerce")

    @staticmethod
    def _dedupe_columns_frame(df: pd.DataFrame) -> pd.DataFrame:
        """Coalesce duplicate-named columns into a single column.

        - If duplicates are all numeric, sum row-wise (min_count=1 to preserve NaN when all are NaN)
        - Otherwise, take first non-null left-to-right.
        Returns a new DataFrame with unique column names in original order of first appearance.
        """
        if not df.columns.duplicated().any():
            return df.copy()
        cols = df.columns.tolist()
        seen = {}
        out = {}
        for i, c in enumerate(cols):
            if c in seen:
                continue
            dup_mask = [j for j, cc in enumerate(cols) if cc == c]
            if len(dup_mask) == 1:
                out[c] = df.iloc[:, dup_mask[0]]
            else:
                sub = df.iloc[:, dup_mask]
                # Determine if all numeric
                if all(pd.api.types.is_numeric_dtype(sub.iloc[:, k]) for k in range(sub.shape[1])):
                    out[c] = pd.to_numeric(sub, errors="coerce").sum(axis=1, min_count=1)
                else:
                    out[c] = sub.bfill(axis=1).iloc[:, 0]
            seen[c] = True
        return pd.DataFrame(out, index=df.index)

    @staticmethod
    def _filter_out_totals(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
        """Remove rows where any of the specified columns equals a total-like label.

        Matches case-insensitive strings like 'total' or 'all' after strip.
        """
        if df is None or df.empty:
            return df
        total_like = {"total", "all", "overall"}
        mask = pd.Series(True, index=df.index)
        for c in cols:
            if c in df.columns:
                vals = df[c].astype(str).str.strip().str.casefold()
                mask &= ~vals.isin(total_like)
        return df[mask]

    @staticmethod
    def _apply_order(
        df: pd.DataFrame,
        col: str,
        order: List[str],
        prefix_keys: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """Sort by a preferred order while preserving unknown labels.

        Values not listed in `order` are kept (not coerced to NaN) and placed after
        the known ones in their first-seen order. Sorting keeps identifier columns first.
        """
        if col not in df.columns or not order:
            return df
        d = df.copy()
        colvals = d[col].astype(str).str.strip()
        uniques = colvals.unique().tolist()
        present = [b for b in order if b in uniques]
        extras = [u for u in uniques if u not in present]
        if not present and not extras:
            return d
        categories = present + extras
        cat = pd.api.types.CategoricalDtype(categories=categories, ordered=True)
        d[col] = colvals.astype(cat)
        if prefix_keys is None:
            prefix_keys = ['policy_number', 'year']
        sort_cols: List[str] = []
        for key in prefix_keys:
            if key in d.columns and key not in sort_cols:
                sort_cols.append(key)
        if col not in sort_cols:
            sort_cols.append(col)
        return d.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)

    @staticmethod
    def _drop_all_zero(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
        """Remove rows where all specified numeric columns are zero or NaN."""
        if df is None or df.empty:
            return df
        mask = None
        for col in cols:
            if col not in df.columns:
                continue
            series = pd.to_numeric(df[col], errors='coerce').fillna(0)
            col_mask = series.ne(0)
            mask = col_mask if mask is None else (mask | col_mask)
        if mask is None:
            return df
        return df.loc[mask].reset_index(drop=True)

    @staticmethod
    def _sort_with_desc(
        df: pd.DataFrame,
        keys: List[str],
        desc_col: Optional[str] = None,
    ) -> pd.DataFrame:
        """Sort by ascending identifier keys, then optionally by a numeric column descending."""
        if df is None or df.empty:
            return df
        d = df.copy()
        sort_cols: List[str] = []
        ascending: List[bool] = []
        for key in keys:
            if key in d.columns:
                sort_cols.append(key)
                ascending.append(True)
        if desc_col and desc_col in d.columns:
            d[desc_col] = pd.to_numeric(d[desc_col], errors='coerce').fillna(0)
            sort_cols.append(desc_col)
            ascending.append(False)
        if not sort_cols:
            return d
        return d.sort_values(sort_cols, ascending=ascending, kind="mergesort").reset_index(drop=True)

    @staticmethod
    def _sum_by_keys(df: pd.DataFrame, keys: List[str], numeric_cols: List[str]) -> pd.DataFrame:
        """Group df by keys and sum numeric_cols if present; preserve other columns via groupby sum only.

        Returns empty df if no numeric columns exist.
        """
        if df is None or df.empty:
            return df
        cols = [c for c in numeric_cols if c in df.columns]
        keys_present = [k for k in keys if k in df.columns]
        if not cols or not keys_present:
            return df.drop_duplicates().reset_index(drop=True)
        g = df.groupby(keys_present, dropna=False)[cols].sum().reset_index()
        return g

    @staticmethod
    def _reorder_to_schema(df: pd.DataFrame, schema: Optional[List[str]]) -> pd.DataFrame:
        """Reorder columns to match a provided schema; append any extras at the end in existing order."""
        if df is None or df.empty or not schema:
            return df
        cols = list(df.columns)
        in_schema = [c for c in schema if c in cols]
        extras = [c for c in cols if c not in in_schema]
        return df[in_schema + extras]

    def _catalog_from_one(self, entry: Dict) -> pd.DataFrame:
        name = entry["name"]
        sheets = entry["sheets"]
        rows = []
        # Prefer class list from P.21_Class
        if "P.21_Class" in sheets:
            cdf = sheets["P.21_Class"][self._safe_cols(sheets["P.21_Class"], ["policy_number", "year", "class"])].dropna()
            for _, r in cdf.iterrows():
                rows.append({
                    "file": name,
                    "policy_number": str(r.get("policy_number", "")),
                    "year": str(r.get("year", "")),
                    "class": str(r.get("class", "")),
                })
        elif "P.22_Class_BenefitType" in sheets:
            sdf = sheets["P.22_Class_BenefitType"][self._safe_cols(sheets["P.22_Class_BenefitType"], ["policy_number", "year", "class"])].dropna()
            sdf = sdf.drop_duplicates()
            for _, r in sdf.iterrows():
                rows.append({
                    "file": name,
                    "policy_number": str(r.get("policy_number", "")),
                    "year": str(r.get("year", "")),
                    "class": str(r.get("class", "")),
                })
        else:
            # no class pages; fallback to policy-level only
            if "P.20_Policy" in sheets:
                pdf = sheets["P.20_Policy"][self._safe_cols(sheets["P.20_Policy"], ["policy_number", "year"])].dropna()
                for _, r in pdf.iterrows():
                    rows.append({
                        "file": name,
                        "policy_number": str(r.get("policy_number", "")),
                        "year": str(r.get("year", "")),
                        "class": "ALL",
                    })
        out = pd.DataFrame(rows)
        if not out.empty:
            out.insert(0, "source_key", out.apply(lambda r: f"{name}::{r['policy_number']}::{r['year']}::{r['class']}", axis=1))
        return out

    def catalog(self) -> pd.DataFrame:
        """Return a table of selectable (policy_number, year, class) items across uploaded files."""
        if self._catalog is not None:
            return self._catalog
        parts = [self._catalog_from_one(e) for e in self._files]
        cat = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["source_key", "file", "policy_number", "year", "class"])
        self._catalog = cat
        return cat

    @staticmethod
    def _recalc_ratios(df: pd.DataFrame) -> pd.DataFrame:
        # Start by collapsing any duplicate-named columns in the frame to avoid alignment issues
        d = MCRMerger._dedupe_columns_frame(df)
        # Ensure computed-target columns are not duplicated; collapse if duplicated
        def _collapse_dupe(dfin: pd.DataFrame, name: str) -> pd.DataFrame:
            if name not in dfin.columns:
                return dfin
            obj = dfin[name]
            if isinstance(obj, pd.DataFrame):
                # coalesce duplicates by taking first non-null left-to-right
                merged = obj.bfill(axis=1).iloc[:, 0]
                # drop all duplicates, keep first, then set merged as the first
                keep_first = ~dfin.columns.duplicated(keep='first')
                cols = dfin.columns
                # Build new frame with first kept columns
                dfin = dfin.loc[:, keep_first]
                # overwrite the single remaining target column
                dfin[name] = merged
            return dfin

        for tgt in [
            "usage_ratio",
            "incurred_per_case", "paid_per_case",
            "incurred_per_claimant", "paid_per_claimant",
            "incurred_per_claim", "paid_per_claim",
            "claim_frequency",
        ]:
            d = _collapse_dupe(d, tgt)
        # Prepare numeric base series safely (handles duplicate columns)
        inc = MCRMerger._get_numeric_series(d, "incurred_amount")
        paid = MCRMerger._get_numeric_series(d, "paid_amount")
        n_cases = MCRMerger._get_numeric_series(d, "no_of_cases")
        n_claimants = MCRMerger._get_numeric_series(d, "no_of_claimants")
        n_claims = MCRMerger._get_numeric_series(d, "no_of_claim_id")

        # Core ratios
        if paid is not None and inc is not None:
            base = inc.replace(0, pd.NA)
            d["usage_ratio"] = paid / base
        # Per-case
        if inc is not None and n_cases is not None:
            denom = n_cases.replace(0, pd.NA)
            d["incurred_per_case"] = inc / denom
        if paid is not None and n_cases is not None:
            denom = n_cases.replace(0, pd.NA)
            d["paid_per_case"] = paid / denom
        # Per-claimant
        if inc is not None and n_claimants is not None:
            denom = n_claimants.replace(0, pd.NA)
            d["incurred_per_claimant"] = inc / denom
        if paid is not None and n_claimants is not None:
            denom = n_claimants.replace(0, pd.NA)
            d["paid_per_claimant"] = paid / denom
        # Per-claim
        if inc is not None and n_claims is not None:
            denom = n_claims.replace(0, pd.NA)
            d["incurred_per_claim"] = inc / denom
        if paid is not None and n_claims is not None:
            denom = n_claims.replace(0, pd.NA)
            d["paid_per_claim"] = paid / denom
        # Frequency
        if n_claims is not None and n_claimants is not None:
            d["claim_frequency"] = n_claims / n_claimants.replace(0, pd.NA)
        # Remove duplicate alias columns if present
        drop_dupes = [
            "incurred_amount_per_case", "paid_amount_per_case",
            "incurred_amount_per_claimant", "paid_amount_per_claimant",
            "incurred_amount_per_claim", "paid_amount_per_claim",
        ]
        d = d.drop(columns=[c for c in drop_dupes if c in d.columns], errors="ignore")
        return d

    def _choose_group_keys(self, df: pd.DataFrame) -> List[str]:
        """Choose sensible grouping keys for an arbitrary sheet DataFrame.

        Strategy:
        - Keep columns that are non-numeric (object, bool or datetime), plus important domain columns like
          'benefit_type','benefit','panel','class','common_diagnosis_flag','day_procedure_flag','provider',
          'hospital_name','physician','procedure','diagnosis','suboffice','dep_type','age_band','data_month'.
        - Exclude technical columns like 'source_file' or any 'file' column.
        - Exclude 'policy_number' and 'year' because those are replaced by merged identifiers.
        """
        candidate = []
        for c in df.columns:
            lc = c.lower()
            if c in ("policy_number", "year", "file", "source_file"):
                continue
            if lc in ("policy_start_date", "policy_end_date", "policy_data_date"):
                # dates aren't useful grouping keys for merges
                continue
            if c in ("benefit_type", "benefit", "panel", "class", "common_diagnosis_flag", "day_procedure_flag", "provider", "hospital_name", "physician", "procedure", "diagnosis", "suboffice", "dep_type", "age_band", "data_month", "claimant"):
                candidate.append(c)
                continue
            # include non-numeric columns
            if not pd.api.types.is_numeric_dtype(df[c]):
                candidate.append(c)
        # Deduplicate while preserving order
        seen = set()
        keys = []
        for k in candidate:
            if k not in seen:
                seen.add(k)
                keys.append(k)
        return keys

    def _get_ip_benefit_order(self) -> List[str]:
        """Load the inpatient benefit ordering from benefit_indexing.xlsx when available."""
        if self._ip_benefit_order is not None:
            return self._ip_benefit_order
        candidates = [
            Path(__file__).resolve().parent / 'benefit_indexing.xlsx',
            Path.cwd() / 'benefit_indexing.xlsx',
        ]
        order: List[str] = []
        for path in candidates:
            if not path.exists():
                continue
            try:
                df = pd.read_excel(path)
                if {'gum_benefit', 'gum_benefit_type'}.issubset(df.columns):
                    mask = df['gum_benefit_type'].astype(str).isin({
                        'INPATIENT BENEFITS /HOSPITALIZATION',
                        'MATERNITY',
                        'SUPPLEMENTARY MAJOR MEDICAL',
                    })
                    order = (
                        df.loc[mask, 'gum_benefit']
                        .dropna()
                        .astype(str)
                        .drop_duplicates(keep='last')
                        .tolist()
                    )
                    break
            except Exception:
                continue
        self._ip_benefit_order = order
        return order

    def _filter_rows(self, df: pd.DataFrame, picks: List[Tuple[str, str, str]]) -> pd.DataFrame:
        """Filter df to rows matching any of (policy_number, year, class) tuples."""
        if df is None or df.empty:
            return df
        mask = pd.Series(False, index=df.index)
        # normalize to string compare
        for p, y, c in picks:
            pm = (df.get("policy_number").astype(str) == str(p)) if "policy_number" in df.columns else pd.Series(False, index=df.index)
            ym = (df.get("year").astype(str) == str(y)) if "year" in df.columns else pd.Series(False, index=df.index)
            # class may not exist on some sheets
            if "class" in df.columns:
                cm = (df.get("class").astype(str) == str(c))
                mask = mask | (pm & ym & cm)
            else:
                mask = mask | (pm & ym)
        return df[mask]

    def merge(
        self,
        groups: List[Dict],
    ) -> Dict[str, pd.DataFrame]:
        """
        Merge according to groups.
        Each group dict requires keys:
        - merged_policy_number: str
        - merged_year: str/int
        - merged_class: str
        - source_items: list of dicts with keys (policy_number, year, class)

        Returns dict of sheet_name -> merged DataFrame (union for all groups in one workbook).
        """
        # Aggregate all sheets across files first
        all_sheets: Dict[str, pd.DataFrame] = {}
        for entry in self._files:
            for sn, df in entry["sheets"].items():
                if sn not in all_sheets:
                    all_sheets[sn] = df.copy()
                else:
                    all_sheets[sn] = pd.concat([all_sheets[sn], df], ignore_index=True)

        out: Dict[str, List[pd.DataFrame]] = {sn: [] for sn in self.SUPPORTED_SHEETS}

        def sum_base(d: pd.DataFrame, by_cols: List[str]) -> pd.DataFrame:
            cols = [c for c in self.BASE_COLS if c in d.columns]
            if not cols:
                return pd.DataFrame(columns=by_cols)
            g = d.groupby(by=by_cols, dropna=False)[cols].sum().reset_index()
            return self._recalc_ratios(g)

        # Build a set of all selected (policy, year, class) tuples across groups
        selected_set = set()
        for g in groups:
            for i in g.get("source_items", []):
                selected_set.add((str(i.get("policy_number","")), str(i.get("year","")), str(i.get("class",""))))

        # Non-class sheets that we will always rebuild deterministically from class/base sheets
        # to avoid double counting (especially from 'Total' rows)
        NON_CLASS_DERIVED_SHEETS = {
            'P.20_Policy',
            'P.20_BenefitType',
            'P.20_Network',
            'P.20_Network_BenefitType',
            'P.20_Day_Prod',
            'P.23_IP_Benefit',
            'P.23b_Common_Diagnosis_IP',
            'P.24_OP_Benefit',
            'P.24w_Wellness',
            'P.26_OP_Panel_Benefit',
            'P.18_TopHosDiag',
            'P.18b_TopClinDiag',
            'P.18b_IP_DayProc',
            'P.20_Benefit_DepType',
        }

        for grp in groups:
            merged_policy = str(grp.get("merged_policy_number", "MERGED"))
            merged_year = str(grp.get("merged_year", ""))
            merged_class = str(grp.get("merged_class", "Merged"))
            items = grp.get("source_items", [])
            picks = [(str(i["policy_number"]), str(i["year"]), str(i.get("class", "ALL"))) for i in items]

            # For generality, iterate all sheets and perform merging with sensible group keys
            for sn, df in all_sheets.items():
                if df is None or df.empty:
                    continue
                # Skip non-class pages here; they'll be rebuilt from class-level pages below to avoid duplicates
                if sn in NON_CLASS_DERIVED_SHEETS:
                    continue
                # select only rows that belong to this group's picks
                sel = self._filter_rows(df, picks)
                if sel is None or sel.empty:
                    continue

                # Explicit handling for P.21_Class: collapse selected classes into the merged class per year
                if sn == 'P.21_Class':
                    s = sel.copy()
                    s['class'] = merged_class
                    # group strictly by year to avoid retaining original class splits
                    by_cols = []
                    if 'year' in s.columns:
                        by_cols = ['year']
                    cols = [c for c in self.BASE_COLS if c in s.columns]
                    if cols:
                        agg21 = s.groupby(by=by_cols, dropna=False)[cols].sum().reset_index()
                        agg21['policy_number'] = merged_policy
                        if 'year' not in agg21.columns and merged_year:
                            agg21['year'] = merged_year
                        agg21['class'] = merged_class
                        agg21 = self._recalc_ratios(agg21)
                        # reorder identifiers
                        id_first = [c for c in ['policy_number','year','class'] if c in agg21.columns]
                        other = [c for c in agg21.columns if c not in id_first]
                        out[sn].append(agg21[id_first + other])
                        continue

                # Special sheet handler: Policy_Info (emit one row per year present)
                if sn == 'Policy_Info':
                    years = sel['year'].astype(str).unique().tolist() if 'year' in sel.columns else [None]
                    for y in years:
                        ssub = sel if y is None else sel.loc[sel['year'].astype(str) == str(y)]
                        if ssub.empty:
                            continue
                        row = {}
                        row['policy_number'] = merged_policy
                        if y is not None:
                            row['year'] = str(y)
                        # metadata
                        for meta in ['insurer', 'client_name']:
                            if meta in ssub.columns:
                                vals = ssub[meta].dropna()
                                row[meta] = vals.iloc[0] if not vals.empty else None
                        # dates
                        for pdcol in ['policy_start_date', 'policy_end_date', 'policy_data_date']:
                            if pdcol in ssub.columns:
                                try:
                                    ser = pd.to_datetime(ssub[pdcol], errors='coerce')
                                    row[pdcol] = ser.min() if pdcol == 'policy_start_date' else ser.max()
                                except Exception:
                                    vals = ssub[pdcol].dropna()
                                    row[pdcol] = vals.iloc[0] if not vals.empty else None
                        # numeric indicators (averaged)
                        for n in ['data_month', 'ibnr']:
                            if n in ssub.columns:
                                try:
                                    row[n] = pd.to_numeric(ssub[n], errors='coerce').mean()
                                except Exception:
                                    row[n] = None
                        out[sn].append(pd.DataFrame([row]))
                    continue

                # For all other sheets, auto-detect grouping keys and force identifiers for selected rows
                sel = sel.copy()
                # merge selected classes into one
                sel['class'] = merged_class if 'class' in sel.columns else merged_class
                # merge selected policy into one so grouping uses the merged policy key
                if 'policy_number' in sel.columns:
                    sel['policy_number'] = merged_policy
                group_keys = self._choose_group_keys(sel)
                # remove technical and forced-id keys; we'll keep 'year' to separate per-year rows
                group_keys = [g for g in group_keys if g not in ('source_file', 'file', 'class', 'policy_number')]
                final_keys = []
                if 'year' in sel.columns:
                    final_keys.append('year')
                # add remaining keys in stable order without duplicates
                for gk in group_keys:
                    if gk != 'year' and gk not in final_keys:
                        final_keys.append(gk)

                # numeric columns to sum
                numeric_cols = [c for c in sel.columns if pd.api.types.is_numeric_dtype(sel[c])]
                if not numeric_cols:
                    # nothing numeric to aggregate; simply take unique groupings and attach merged ids
                    subset_keys = final_keys if final_keys else None
                    uniq = sel.drop_duplicates(subset=subset_keys)
                    if uniq is None or len(uniq) == 0:
                        continue
                    uniq = uniq.reset_index(drop=True)
                    uniq['policy_number'] = merged_policy
                    # keep existing year if present; class already set
                    out[sn].append(uniq)
                    continue

                agg = sel.groupby(by=final_keys, dropna=False)[numeric_cols].sum().reset_index()
                # assign merged identifiers and class on aggregated rows
                agg['policy_number'] = merged_policy
                agg['class'] = merged_class

                # recompute ratios and per-case/claimant metrics
                agg = self._recalc_ratios(agg)
                # reorder identifiers first when present
                for col in ["class", "year", "policy_number"]:
                    if col in agg.columns:
                        # move to front by reindexing
                        cols = [c for c in agg.columns if c != col]
                        agg = agg[[col] + cols]
                out[sn].append(agg)

        # Concatenate pieces per sheet
        result: Dict[str, pd.DataFrame] = {}
        for sn, parts in out.items():
            if parts:
                result[sn] = pd.concat(parts, ignore_index=True)
        # Append remainder rows (unselected classes remain independent) for class-level sheets
        for sn, df in all_sheets.items():
            if df is None or df.empty:
                continue
            if 'policy_number' in df.columns and 'year' in df.columns and 'class' in df.columns:
                temp = df.copy()
                temp['__t__'] = list(zip(temp['policy_number'].astype(str), temp['year'].astype(str), temp['class'].astype(str)))
                remainder = temp[~temp['__t__'].isin(selected_set)].drop(columns=['__t__'])
                if not remainder.empty:
                    if sn in result:
                        result[sn] = pd.concat([result[sn], remainder], ignore_index=True)
                    else:
                        result[sn] = remainder.reset_index(drop=True)
        # Normalize class-level base sheets first to prevent duplicates inflating totals
        norm_specs = {
            'P.21_Class': ['policy_number', 'year', 'class'],
            'P.22_Class_BenefitType': ['policy_number', 'year', 'class', 'benefit_type'],
            'P.22_Class_DepType': ['policy_number', 'year', 'class', 'dep_type', 'benefit_type'],
            'P.23a_Class_IP_Benefit': ['policy_number', 'year', 'class', 'benefit'],
            'P.23b_Class_Common_Diag_IP': ['policy_number', 'year', 'class', 'common_diagnosis_flag', 'benefit'],
            'P.20_Day_Prod_Class': ['policy_number', 'year', 'class', 'day_procedure_flag'],
            'P.24a_Class_OP_Benefit': ['policy_number', 'year', 'class', 'benefit'],
            'P.24wc_Class_Wellness': ['policy_number', 'year', 'class', 'benefit'],
            'P.25_Class_Panel_BenefitType': ['policy_number', 'year', 'class', 'panel', 'benefit_type'],
            'P.26a_OP_Class_Panel_Benefit': ['policy_number', 'year', 'class', 'panel', 'benefit'],
        }
        for sn, keys in norm_specs.items():
            if sn in result:
                result[sn] = self._sum_by_keys(result[sn], keys, self.BASE_COLS)
                result[sn] = self._recalc_ratios(result[sn])

        # Post-process panel-benefit class sheet for ordering and zero filtering
        p25_sheet = result.get('P.25_Class_Panel_BenefitType')
        if p25_sheet is not None and not p25_sheet.empty:
            metrics = ['incurred_amount', 'paid_amount', 'no_of_cases']
            p25 = p25_sheet.copy()
            for col in metrics:
                if col in p25.columns:
                    p25[col] = pd.to_numeric(p25[col], errors='coerce').fillna(0)
            p25 = self._recalc_ratios(p25)
            p25 = self._drop_all_zero(p25, metrics)
            p25 = self._recalc_ratios(p25)
            p25 = self._apply_order(
                p25,
                'benefit_type',
                ["Hospital", "Clinic", "Dental", "Optical", "Maternity", "Total"],
                prefix_keys=['policy_number', 'year', 'class', 'panel'],
            )
            result['P.25_Class_Panel_BenefitType'] = p25

        # Clean up class-level day procedure, benefit, and inpatient sheets before deriving policy-level results
        p22_sheet = result.get('P.22_Class_BenefitType')
        if p22_sheet is not None and not p22_sheet.empty:
            p22 = self._normalize_benefit_type_values(p22_sheet, 'benefit_type')
            metrics = ['incurred_amount', 'paid_amount', 'no_of_cases', 'no_of_claimants', 'no_of_claim_id']
            for col in metrics:
                if col in p22.columns:
                    p22[col] = pd.to_numeric(p22[col], errors='coerce').fillna(0)
            p22 = self._recalc_ratios(p22)
            p22 = self._drop_all_zero(p22, metrics)
            p22 = self._apply_order(p22, 'benefit_type', ["Hospital", "Clinic", "Dental", "Optical", "Maternity", "Total"], prefix_keys=['policy_number', 'year', 'class'])
            needed_p22 = [
                'policy_number', 'year', 'class', 'benefit_type',
                'incurred_amount', 'paid_amount', 'usage_ratio',
                'no_of_cases', 'incurred_per_case', 'paid_per_case',
                'no_of_claimants', 'no_of_claim_id',
            ]
            for col in needed_p22:
                if col not in p22.columns:
                    p22[col] = pd.NA
            result['P.22_Class_BenefitType'] = p22[needed_p22]

        p22_dep_sheet = result.get('P.22_Class_DepType')
        if p22_dep_sheet is not None and not p22_dep_sheet.empty:
            p22_dep = self._normalize_benefit_type_values(p22_dep_sheet, 'benefit_type')
            if 'no_of_claim_id' in p22_dep.columns:
                claim_series = pd.to_numeric(p22_dep['no_of_claim_id'], errors='coerce').fillna(0)
            else:
                claim_series = None
            if 'no_of_cases' in p22_dep.columns:
                cases_series = pd.to_numeric(p22_dep['no_of_cases'], errors='coerce').fillna(0)
                if claim_series is not None:
                    zero_mask = cases_series.eq(0)
                    cases_series.loc[zero_mask] = claim_series.loc[zero_mask]
                p22_dep['no_of_cases'] = cases_series
            else:
                if claim_series is not None:
                    p22_dep['no_of_cases'] = claim_series
                else:
                    p22_dep['no_of_cases'] = 0.0
            dep_metrics = ['incurred_amount', 'paid_amount', 'no_of_cases', 'no_of_claimants', 'no_of_claim_id']
            for col in dep_metrics:
                if col in p22_dep.columns:
                    p22_dep[col] = pd.to_numeric(p22_dep[col], errors='coerce').fillna(0)
                else:
                    p22_dep[col] = 0.0
            p22_dep = self._recalc_ratios(p22_dep)
            p22_dep = self._drop_all_zero(p22_dep, ['incurred_amount', 'paid_amount', 'no_of_cases'])
            p22_dep = self._recalc_ratios(p22_dep)
            p22_dep = self._apply_order(
                p22_dep,
                'benefit_type',
                ["Hospital", "Clinic", "Dental", "Optical", "Maternity", "Total"],
                prefix_keys=['policy_number', 'year', 'dep_type', 'class']
            )
            p22_dep = p22_dep.sort_values(
                by=['policy_number', 'year', 'dep_type', 'class'],
                ascending=[True, True, True, True],
                kind='mergesort'
            ).reset_index(drop=True)
            needed_p22_dep = [
                'policy_number', 'year', 'dep_type', 'class', 'benefit_type',
                'incurred_amount', 'paid_amount', 'usage_ratio',
                'no_of_cases', 'incurred_per_case', 'paid_per_case',
                'no_of_claimants', 'no_of_claim_id',
            ]
            for col in needed_p22_dep:
                if col not in p22_dep.columns:
                    p22_dep[col] = pd.NA
            result['P.22_Class_DepType'] = p22_dep[needed_p22_dep]

            dep_base_cols = ['incurred_amount', 'paid_amount', 'no_of_cases', 'no_of_claimants', 'no_of_claim_id']
            dep_summary = p22_dep[['policy_number', 'year', 'dep_type', 'benefit_type'] + dep_base_cols].copy()
            for col in dep_base_cols:
                dep_summary[col] = pd.to_numeric(dep_summary[col], errors='coerce').fillna(0)
            dep_summary = dep_summary.groupby(
                ['policy_number', 'year', 'dep_type', 'benefit_type'],
                dropna=False
            )[dep_base_cols].sum().reset_index()
            dep_summary = self._recalc_ratios(dep_summary)
            dep_summary = self._drop_all_zero(dep_summary, ['incurred_amount', 'paid_amount', 'no_of_cases'])
            dep_summary = self._recalc_ratios(dep_summary)
            dep_summary = self._apply_order(
                dep_summary,
                'benefit_type',
                ["Hospital", "Clinic", "Dental", "Optical", "Maternity", "Total"],
                prefix_keys=['policy_number', 'year', 'dep_type']
            )
            dep_summary = dep_summary.sort_values(
                by=['policy_number', 'year', 'dep_type'],
                ascending=[True, True, True],
                kind='mergesort'
            ).reset_index(drop=True)
            needed_p20_dep = [
                'policy_number', 'year', 'dep_type', 'benefit_type',
                'incurred_amount', 'paid_amount', 'usage_ratio',
                'no_of_cases', 'incurred_per_case', 'paid_per_case',
                'no_of_claimants', 'no_of_claim_id',
            ]
            for col in needed_p20_dep:
                if col not in dep_summary.columns:
                    dep_summary[col] = pd.NA
            result['P.20_Benefit_DepType'] = dep_summary[needed_p20_dep]

        p23a_sheet = result.get('P.23a_Class_IP_Benefit')
        if p23a_sheet is not None and not p23a_sheet.empty:
            p23a = p23a_sheet.copy()
            claim_series = None
            if 'no_of_claim_id' in p23a.columns:
                claim_series = pd.to_numeric(p23a['no_of_claim_id'], errors='coerce')
            if 'no_of_cases' in p23a.columns:
                cases_series = pd.to_numeric(p23a['no_of_cases'], errors='coerce')
                if claim_series is not None:
                    zero_mask = cases_series.fillna(0).eq(0)
                    cases_series.loc[zero_mask] = claim_series.fillna(0).loc[zero_mask]
                p23a['no_of_cases'] = cases_series.fillna(0)
            elif claim_series is not None:
                p23a['no_of_cases'] = claim_series.fillna(0)
            else:
                p23a['no_of_cases'] = 0
            metrics = ['incurred_amount', 'paid_amount', 'no_of_cases', 'no_of_claim_id', 'no_of_claimants']
            for col in metrics:
                if col in p23a.columns:
                    p23a[col] = pd.to_numeric(p23a[col], errors='coerce').fillna(0)
            p23a = p23a.fillna(0)
            p23a = self._recalc_ratios(p23a)
            drop_cols_p23a = [c for c in ['incurred_amount', 'paid_amount', 'no_of_cases'] if c in p23a.columns]
            p23a = self._drop_all_zero(p23a, drop_cols_p23a)
            p23a = self._recalc_ratios(p23a)
            order = self._get_ip_benefit_order()
            if order:
                p23a = self._apply_order(p23a, 'benefit', order, prefix_keys=['policy_number', 'year', 'class'])
            needed_p23a = [
                'policy_number', 'year', 'class', 'benefit',
                'incurred_amount', 'paid_amount', 'usage_ratio',
                'no_of_cases', 'incurred_per_case', 'paid_per_case',
                'no_of_claim_id', 'no_of_claimants',
                'incurred_per_claim', 'paid_per_claim',
                'incurred_per_claimant', 'paid_per_claimant',
                'claim_frequency',
            ]
            for col in needed_p23a:
                if col not in p23a.columns:
                    p23a[col] = pd.NA
            result['P.23a_Class_IP_Benefit'] = p23a[needed_p23a]

            day_class_sheet = result.get('P.20_Day_Prod_Class')
            if day_class_sheet is not None and not day_class_sheet.empty:
                day_class_clean = day_class_sheet.copy()
                day_base_cols = ['incurred_amount', 'paid_amount', 'no_of_cases', 'no_of_claim_id']
                for col in day_base_cols:
                    if col in day_class_clean.columns:
                        day_class_clean[col] = pd.to_numeric(day_class_clean[col], errors='coerce').fillna(0)
                    else:
                        day_class_clean[col] = 0.0
                day_class_clean = self._recalc_ratios(day_class_clean)
                day_class_clean = self._drop_all_zero(day_class_clean, ['incurred_amount', 'paid_amount', 'no_of_cases'])
                day_class_clean = self._recalc_ratios(day_class_clean)
                needed_day_class = [
                    'policy_number', 'year', 'class', 'day_procedure_flag',
                    'incurred_amount', 'paid_amount', 'usage_ratio',
                    'no_of_cases', 'no_of_claim_id',
                ]
                for col in needed_day_class:
                    if col not in day_class_clean.columns:
                        day_class_clean[col] = pd.NA
                result['P.20_Day_Prod_Class'] = day_class_clean[needed_day_class]

        # Derive policy/benefit/network pages from class-level bases to ensure consistency
        p20_benefit = self._build_p20_benefit_page(result.get('P.22_Class_BenefitType'))
        if p20_benefit is None:
            p20_benefit = self._build_p20_benefit_page(all_sheets.get('P.22_Class_BenefitType'))
        if p20_benefit is None:
            p20_benefit = self._build_p20_benefit_page(result.get('P.25_Class_Panel_BenefitType'))
        if p20_benefit is None:
            p20_benefit = self._build_p20_benefit_page(all_sheets.get('P.20_BenefitType'))
        if p20_benefit is not None:
            result['P.20_BenefitType'] = p20_benefit

        # Build P.20_Day_Prod by collapsing class-level page
        day_class = result.get('P.20_Day_Prod_Class')
        if (day_class is None or day_class.empty) and 'P.20_Day_Prod_Class' in all_sheets:
            day_class = all_sheets.get('P.20_Day_Prod_Class')
        if day_class is not None and not day_class.empty:
            day_df = day_class.copy()
            day_base_cols = ['incurred_amount', 'paid_amount', 'no_of_cases', 'no_of_claim_id']
            for col in day_base_cols:
                if col in day_df.columns:
                    day_df[col] = pd.to_numeric(day_df[col], errors='coerce').fillna(0)
                else:
                    day_df[col] = 0.0
            group_keys = [c for c in ['policy_number', 'year', 'day_procedure_flag'] if c in day_df.columns]
            if len(group_keys) >= 2:
                day_grouped = day_df.groupby(group_keys, dropna=False)[day_base_cols].sum().reset_index()
                day_grouped = self._recalc_ratios(day_grouped)
                day_grouped = self._drop_all_zero(day_grouped, ['incurred_amount', 'paid_amount', 'no_of_cases'])
                needed_dp = [
                    'policy_number', 'year', 'day_procedure_flag',
                    'incurred_amount', 'paid_amount', 'usage_ratio',
                    'no_of_cases', 'no_of_claim_id',
                ]
                for col in needed_dp:
                    if col not in day_grouped.columns:
                        day_grouped[col] = pd.NA
                result['P.20_Day_Prod'] = day_grouped[needed_dp]

        # Build P.20_Policy from merged class page; fallback to legacy if unavailable
        p21_sheet = result.get('P.21_Class')
        if p21_sheet is not None and not p21_sheet.empty:
            id_cols = [c for c in ['policy_number', 'year'] if c in p21_sheet.columns]
            value_cols = [c for c in ['incurred_amount', 'paid_amount', 'no_of_claimants'] if c in p21_sheet.columns]
            if len(id_cols) == 2 and {'incurred_amount', 'paid_amount'}.issubset(set(value_cols)):
                p20_policy = p21_sheet.groupby(id_cols, dropna=False)[value_cols].sum().reset_index()
                for col in ['incurred_amount', 'paid_amount', 'no_of_claimants']:
                    if col not in p20_policy.columns:
                        p20_policy[col] = pd.NA
                    else:
                        p20_policy[col] = pd.to_numeric(p20_policy[col], errors='coerce')
                denom = p20_policy['incurred_amount'].replace(0, pd.NA)
                p20_policy['usage_ratio'] = p20_policy['paid_amount'] / denom
                p20_policy = p20_policy[['policy_number', 'year', 'incurred_amount', 'paid_amount', 'usage_ratio', 'no_of_claimants']]
                result['P.20_Policy'] = p20_policy
        if 'P.20_Policy' not in result:
            legacy_p20 = all_sheets.get('P.20_Policy')
            if legacy_p20 is not None and not legacy_p20.empty:
                legacy = legacy_p20.copy()
                if 'incurred_amount' in legacy.columns:
                    legacy['incurred_amount'] = pd.to_numeric(legacy['incurred_amount'], errors='coerce')
                if 'paid_amount' in legacy.columns:
                    legacy['paid_amount'] = pd.to_numeric(legacy['paid_amount'], errors='coerce')
                if 'usage_ratio' not in legacy.columns:
                    paid = pd.to_numeric(legacy.get('paid_amount'), errors='coerce')
                    inc = pd.to_numeric(legacy.get('incurred_amount'), errors='coerce').replace(0, pd.NA)
                    legacy['usage_ratio'] = paid / inc
                else:
                    legacy['usage_ratio'] = pd.to_numeric(legacy['usage_ratio'], errors='coerce')
                if 'no_of_claimants' not in legacy.columns:
                    legacy['no_of_claimants'] = pd.NA
                needed = [c for c in ['policy_number', 'year', 'incurred_amount', 'paid_amount', 'usage_ratio', 'no_of_claimants'] if c in legacy.columns]
                if {'policy_number', 'year', 'incurred_amount', 'paid_amount', 'usage_ratio', 'no_of_claimants'}.issubset(set(needed)):
                    result['P.20_Policy'] = legacy[['policy_number', 'year', 'incurred_amount', 'paid_amount', 'usage_ratio', 'no_of_claimants']]

        # From P.25 -> P.20_Network and P.20_Network_BenefitType
        p25 = result.get('P.25_Class_Panel_BenefitType', None)
        if p25 is not None and not p25.empty:
            p25 = self._normalize_benefit_type_values(p25, 'benefit_type')
            required_net_cols = ['incurred_amount', 'paid_amount', 'no_of_cases', 'no_of_claimants', 'no_of_claim_id']
            for col in required_net_cols:
                if col not in p25.columns:
                    p25[col] = 0.0
                else:
                    p25[col] = pd.to_numeric(p25[col], errors='coerce').fillna(0)

            tmask = True
            if 'benefit_type' in p25.columns:
                tmask = ~p25['benefit_type'].astype(str).str.strip().str.casefold().isin({"total", "all", "overall"})
            p25_typed = p25[tmask]

            # P.20_Network from typed only (across benefit types)
            if {'policy_number','year','panel'}.issubset(p25_typed.columns):
                p20_net = p25_typed.groupby(['policy_number','year','panel'], dropna=False)[['incurred_amount', 'paid_amount']].sum().reset_index()
                p20_net = self._recalc_ratios(p20_net)
                needed_net = ['policy_number', 'year', 'panel', 'incurred_amount', 'paid_amount', 'usage_ratio']
                for col in needed_net:
                    if col not in p20_net.columns:
                        p20_net[col] = pd.NA
                result['P.20_Network'] = p20_net[needed_net]

            # P.20_Network_BenefitType typed + totals computed from typed
            if {'policy_number','year','panel','benefit_type'}.issubset(p25_typed.columns):
                agg_cols = ['incurred_amount', 'paid_amount', 'no_of_cases', 'no_of_claimants', 'no_of_claim_id']
                p20_nbt_typed = p25_typed.groupby(['policy_number','year','panel','benefit_type'], dropna=False)[agg_cols].sum().reset_index()
                p20_nbt_typed = self._coalesce_missing_benefit_type(p20_nbt_typed, agg_cols, extra_group_cols=['panel'])
                p20_nbt_typed = self._recalc_ratios(p20_nbt_typed)
                p20_nbt_tot = p20_nbt_typed.groupby(['policy_number','year','panel'], dropna=False)[agg_cols].sum().reset_index()
                p20_nbt_tot['benefit_type'] = 'Total'
                p20_nbt_tot = self._recalc_ratios(p20_nbt_tot)
                p20_nbt = pd.concat([p20_nbt_typed, p20_nbt_tot], ignore_index=True)
                p20_nbt = self._ensure_benefit_type_rows(p20_nbt, agg_cols, ["Hospital", "Clinic", "Dental", "Optical", "Maternity", "Total"], extra_group_cols=['panel'])
                p20_nbt = self._recalc_ratios(p20_nbt)
                # drop benefit types where both panel and non-panel contributions are zero
                if {'policy_number', 'year', 'benefit_type'}.issubset(p20_nbt.columns):
                    zero_keys = (
                        p20_nbt.groupby(['policy_number', 'year', 'benefit_type'], dropna=False)[['incurred_amount', 'paid_amount']]
                        .sum()
                        .reset_index()
                    )
                    zero_keys = zero_keys[
                        zero_keys['incurred_amount'].fillna(0).eq(0)
                        & zero_keys['paid_amount'].fillna(0).eq(0)
                    ]
                    if not zero_keys.empty:
                        drop_keys = zero_keys[['policy_number', 'year', 'benefit_type']].assign(__drop__=True)
                        p20_nbt = p20_nbt.merge(drop_keys, on=['policy_number', 'year', 'benefit_type'], how='left')
                        p20_nbt = p20_nbt[p20_nbt['__drop__'] != True].drop(columns='__drop__')
                p20_nbt = self._apply_order(p20_nbt, 'benefit_type', ["Hospital", "Clinic", "Dental", "Optical", "Maternity", "Total"], prefix_keys=['policy_number', 'year', 'panel'])
                needed_nbt = [
                    'policy_number', 'year', 'panel', 'benefit_type',
                    'incurred_amount', 'paid_amount', 'usage_ratio',
                    'no_of_cases', 'incurred_per_case', 'paid_per_case',
                    'no_of_claimants', 'no_of_claim_id',
                ]
                for col in needed_nbt:
                    if col not in p20_nbt.columns:
                        p20_nbt[col] = pd.NA
                result['P.20_Network_BenefitType'] = p20_nbt[needed_nbt]
        else:
            # Fallback if class-panel sheet not available: rebuild from existing network-benefit sheet if present, excluding totals
            nbt_src = all_sheets.get('P.20_Network_BenefitType', None)
            if nbt_src is not None and not nbt_src.empty:
                nbt_src = self._normalize_benefit_type_values(nbt_src, 'benefit_type')
                n_cols_all = [c for c in ['incurred_amount', 'paid_amount', 'no_of_cases', 'no_of_claimants', 'no_of_claim_id'] if c in nbt_src.columns]
                if n_cols_all:
                    tmask = True
                    if 'benefit_type' in nbt_src.columns:
                        tmask = ~nbt_src['benefit_type'].astype(str).str.strip().str.casefold().isin({"total", "all", "overall"})
                    nbt_typed = nbt_src[tmask].copy()
                    if {'policy_number','year','panel','benefit_type'}.issubset(set(nbt_typed.columns)):
                        nbt_typed = nbt_typed.groupby(['policy_number','year','panel','benefit_type'], dropna=False)[n_cols_all].sum().reset_index()
                        nbt_typed = self._coalesce_missing_benefit_type(nbt_typed, n_cols_all, extra_group_cols=['panel'])
                        nbt_typed = self._recalc_ratios(nbt_typed)
                        nbt_tot = nbt_typed.groupby(['policy_number','year','panel'], dropna=False)[n_cols_all].sum().reset_index()
                        nbt_tot['benefit_type'] = 'Total'
                        nbt_tot = self._recalc_ratios(nbt_tot)
                        nbt = pd.concat([nbt_typed, nbt_tot], ignore_index=True)
                        nbt = self._ensure_benefit_type_rows(nbt, n_cols_all, ["Hospital", "Clinic", "Dental", "Optical", "Maternity", "Total"], extra_group_cols=['panel'])
                        nbt = self._recalc_ratios(nbt)
                        if {'policy_number', 'year', 'benefit_type'}.issubset(nbt.columns):
                            zero_keys = (
                                nbt.groupby(['policy_number', 'year', 'benefit_type'], dropna=False)[['incurred_amount', 'paid_amount']]
                                .sum()
                                .reset_index()
                            )
                            zero_keys = zero_keys[
                                zero_keys['incurred_amount'].fillna(0).eq(0)
                                & zero_keys['paid_amount'].fillna(0).eq(0)
                            ]
                            if not zero_keys.empty:
                                drop_keys = zero_keys[['policy_number', 'year', 'benefit_type']].assign(__drop__=True)
                                nbt = nbt.merge(drop_keys, on=['policy_number', 'year', 'benefit_type'], how='left')
                                nbt = nbt[nbt['__drop__'] != True].drop(columns='__drop__')
                        nbt = self._apply_order(nbt, 'benefit_type', ["Hospital", "Clinic", "Dental", "Optical", "Maternity", "Total"], prefix_keys=['policy_number', 'year', 'panel'])
                        needed_nbt = [
                            'policy_number', 'year', 'panel', 'benefit_type',
                            'incurred_amount', 'paid_amount', 'usage_ratio',
                            'no_of_cases', 'incurred_per_case', 'paid_per_case',
                            'no_of_claimants', 'no_of_claim_id',
                        ]
                        for col in needed_nbt:
                            if col not in nbt.columns:
                                nbt[col] = pd.NA
                        result['P.20_Network_BenefitType'] = nbt[needed_nbt]
                    if {'policy_number','year','panel'}.issubset(set(nbt_src.columns)):
                        p20net = nbt_src[tmask].groupby(['policy_number','year','panel'], dropna=False)[['incurred_amount', 'paid_amount']].sum().reset_index()
                        p20net = self._recalc_ratios(p20net)
                        needed_net = ['policy_number', 'year', 'panel', 'incurred_amount', 'paid_amount', 'usage_ratio']
                        for col in needed_net:
                            if col not in p20net.columns:
                                p20net[col] = pd.NA
                        result['P.20_Network'] = p20net[needed_net]

        # From P.23a_Class_IP_Benefit -> P.23_IP_Benefit (drop class)
        p23a = result.get('P.23a_Class_IP_Benefit', None)
        if p23a is not None and not p23a.empty:
            p23af = self._filter_out_totals(p23a, ["benefit"]) if 'benefit' in p23a.columns else p23a
            # Work with numeric base columns only
            ip_cols = [c for c in self.BASE_COLS if c in p23af.columns]
            if not ip_cols or 'benefit' not in p23af.columns:
                pass
            else:
                work = p23af.copy()
                work = work.drop(columns=['class'], errors='ignore')
                # Normalise case counts, falling back to claim_id when missing
                claim_series = None
                if 'no_of_claim_id' in work.columns:
                    claim_series = pd.to_numeric(work['no_of_claim_id'], errors='coerce')
                if 'no_of_cases' in work.columns:
                    cases_series = pd.to_numeric(work['no_of_cases'], errors='coerce')
                    if claim_series is not None:
                        zero_mask = cases_series.fillna(0).eq(0)
                        cases_series.loc[zero_mask] = claim_series.fillna(0).loc[zero_mask]
                    work['no_of_cases'] = cases_series.fillna(0)
                elif claim_series is not None:
                    work['no_of_cases'] = claim_series.fillna(0)
                    ip_cols.append('no_of_cases')
                else:
                    work['no_of_cases'] = 0
                    if 'no_of_cases' not in ip_cols:
                        ip_cols.append('no_of_cases')

                for col in ip_cols:
                    work[col] = pd.to_numeric(work[col], errors='coerce').fillna(0)

                groupers = [c for c in ['policy_number', 'year', 'benefit'] if c in work.columns]
                if len(groupers) < 3:
                    pass
                else:
                    p23 = work.groupby(groupers, dropna=False)[ip_cols].sum().reset_index()
                    p23.columns = [c.strip() if isinstance(c, str) else c for c in p23.columns]

                    for col in ['incurred_amount', 'paid_amount', 'no_of_cases']:
                        if col in p23.columns:
                            p23[col] = pd.to_numeric(p23[col], errors='coerce')

                    if {'incurred_amount', 'paid_amount', 'no_of_cases'}.issubset(p23.columns):
                        zero_mask = (
                            p23['incurred_amount'].fillna(0).eq(0)
                            | p23['paid_amount'].fillna(0).eq(0)
                            | p23['no_of_cases'].fillna(0).eq(0)
                        )
                        p23 = p23.loc[~zero_mask].reset_index(drop=True)

                    if p23.empty:
                        result.pop('P.23_IP_Benefit', None)
                    else:
                        p23 = self._recalc_ratios(p23)
                        order = self._get_ip_benefit_order()
                        if order:
                            p23 = self._apply_order(p23, 'benefit', order, prefix_keys=['policy_number', 'year'])
                        else:
                            ben_order = work['benefit'].astype(str).dropna().unique().tolist()
                            p23 = self._apply_order(p23, 'benefit', ben_order, prefix_keys=['policy_number', 'year'])

                        needed_p23 = [
                            'policy_number', 'year', 'benefit',
                            'incurred_amount', 'paid_amount', 'usage_ratio',
                            'no_of_cases', 'incurred_per_case', 'paid_per_case',
                            'no_of_claim_id', 'no_of_claimants',
                            'incurred_per_claim', 'paid_per_claim',
                            'incurred_per_claimant', 'paid_per_claimant',
                            'claim_frequency',
                        ]
                        for col in needed_p23:
                            if col not in p23.columns:
                                p23[col] = pd.NA
                        p23 = p23[needed_p23]
                        result['P.23_IP_Benefit'] = p23

        # From P.23b_Class_Common_Diag_IP -> P.23b sheets
        p23b_class = result.get('P.23b_Class_Common_Diag_IP', None)
        if p23b_class is not None and not p23b_class.empty:
            class_cols = ['incurred_amount', 'paid_amount', 'no_of_cases', 'no_of_claim_id', 'no_of_claimants']
            work = p23b_class.copy()
            for col in class_cols:
                if col in work.columns:
                    work[col] = pd.to_numeric(work[col], errors='coerce').fillna(0)
            work = self._recalc_ratios(work)
            zero_cols = [c for c in ['incurred_amount', 'paid_amount', 'no_of_cases'] if c in work.columns]
            work = self._drop_all_zero(work, zero_cols)
            work = self._recalc_ratios(work)
            order_bt = ["Hospital", "Clinic", "Dental", "Optical", "Maternity", "Total"]
            work = self._apply_order(work, 'benefit', order_bt, prefix_keys=['policy_number', 'year', 'class', 'common_diagnosis_flag'])
            needed_class = [
                'policy_number', 'year', 'class', 'common_diagnosis_flag', 'benefit',
                'incurred_amount', 'paid_amount', 'usage_ratio',
                'no_of_claim_id', 'no_of_claimants',
                'incurred_per_claim', 'paid_per_claim',
                'incurred_per_claimant', 'paid_per_claimant',
                'claim_frequency',
            ]
            for col in needed_class:
                if col not in work.columns:
                    work[col] = pd.NA
            result['P.23b_Class_Common_Diag_IP'] = work[needed_class]

            group_keys = [c for c in ['policy_number', 'year', 'common_diagnosis_flag', 'benefit'] if c in work.columns]
            agg_cols = [c for c in ['incurred_amount', 'paid_amount', 'no_of_cases', 'no_of_claim_id', 'no_of_claimants'] if c in work.columns]
            if group_keys and agg_cols:
                agg = work.groupby(group_keys, dropna=False)[agg_cols].sum().reset_index()
                agg = self._recalc_ratios(agg)
                zero_cols_agg = [c for c in ['incurred_amount', 'paid_amount', 'no_of_cases'] if c in agg.columns]
                agg = self._drop_all_zero(agg, zero_cols_agg)
                agg = self._recalc_ratios(agg)
                agg = self._apply_order(agg, 'benefit', order_bt, prefix_keys=['policy_number', 'year', 'common_diagnosis_flag'])
                needed_agg = [
                    'policy_number', 'year', 'common_diagnosis_flag', 'benefit',
                    'incurred_amount', 'paid_amount', 'usage_ratio',
                    'no_of_claim_id', 'no_of_claimants',
                    'incurred_per_claim', 'paid_per_claim',
                    'incurred_per_claimant', 'paid_per_claimant',
                    'claim_frequency',
                ]
                for col in needed_agg:
                    if col not in agg.columns:
                        agg[col] = pd.NA
                result['P.23b_Common_Diagnosis_IP'] = agg[needed_agg]

        # Sort class-level outpatient benefit sheet
        p24a_class_sheet = result.get('P.24a_Class_OP_Benefit')
        if p24a_class_sheet is not None and not p24a_class_sheet.empty:
            p24a_cls = p24a_class_sheet.copy()
            if 'no_of_cases' in p24a_cls.columns:
                p24a_cls['no_of_cases'] = pd.to_numeric(p24a_cls['no_of_cases'], errors='coerce').fillna(0)
            p24a_cls = self._sort_with_desc(p24a_cls, ['policy_number', 'year', 'class'], 'no_of_cases')
            result['P.24a_Class_OP_Benefit'] = p24a_cls

        # From P.24a_Class_OP_Benefit -> P.24_OP_Benefit (drop class)
        p24a = result.get('P.24a_Class_OP_Benefit', None)
        if p24a is not None and not p24a.empty:
            p24af = self._filter_out_totals(p24a, ["benefit"]) if 'benefit' in p24a.columns else p24a
            op_cols = [c for c in self.BASE_COLS if c in p24af.columns]
            groupers = [c for c in ['policy_number','year','benefit'] if c in p24af.columns]
            if op_cols and len(groupers) >= 2:
                p24 = p24af.groupby(groupers, dropna=False)[op_cols].sum().reset_index()
                p24 = self._recalc_ratios(p24)
                if 'benefit' in p24.columns and 'benefit' in p24af.columns:
                    ben_order = p24af['benefit'].astype(str).dropna().unique().tolist()
                    p24 = self._apply_order(p24, 'benefit', ben_order)
                if 'no_of_cases' in p24.columns:
                    p24['no_of_cases'] = pd.to_numeric(p24['no_of_cases'], errors='coerce').fillna(0)
                p24 = self._sort_with_desc(p24, ['policy_number', 'year'], 'no_of_cases')
                result['P.24_OP_Benefit'] = p24

        # From P.24wc_Class_Wellness -> P.24w_Wellness (drop class)
        p24wc = result.get('P.24wc_Class_Wellness', None)
        if p24wc is not None and not p24wc.empty:
            p24wcf = self._filter_out_totals(p24wc, ["benefit"]) if 'benefit' in p24wc.columns else p24wc
            wl_cols = [c for c in self.BASE_COLS if c in p24wcf.columns]
            groupers = [c for c in ['policy_number','year','benefit'] if c in p24wcf.columns]
            if wl_cols and len(groupers) >= 2:
                p24w = p24wcf.groupby(groupers, dropna=False)[wl_cols].sum().reset_index()
                p24w = self._recalc_ratios(p24w)
                if 'benefit' in p24w.columns and 'benefit' in p24wcf.columns:
                    ben_order = p24wcf['benefit'].astype(str).dropna().unique().tolist()
                    p24w = self._apply_order(p24w, 'benefit', ben_order)
                result['P.24w_Wellness'] = p24w

        # From P.26a_OP_Class_Panel_Benefit -> P.26_OP_Panel_Benefit (drop class)
        p26a = result.get('P.26a_OP_Class_Panel_Benefit', None)
        if p26a is not None and not p26a.empty:
            p26af = self._filter_out_totals(p26a, ["benefit"]) if 'benefit' in p26a.columns else p26a
            # BASE_COLS already includes 'no_of_claim_id'; avoid duplicates
            c_cols = [c for c in self.BASE_COLS if c in p26af.columns]
            groupers = [c for c in ['policy_number','year','panel','benefit'] if c in p26af.columns]
            if c_cols and len(groupers) >= 3:
                p26 = p26af.groupby(groupers, dropna=False)[c_cols].sum().reset_index()
                p26 = self._recalc_ratios(p26)
                if 'benefit' in p26.columns and 'benefit' in p26af.columns:
                    ben_order = p26af['benefit'].astype(str).dropna().unique().tolist()
                    p26 = self._apply_order(p26, 'benefit', ben_order)
                if 'paid_amount' in p26.columns:
                    p26['paid_amount'] = pd.to_numeric(p26['paid_amount'], errors='coerce').fillna(0)
                p26 = self._sort_with_desc(p26, ['policy_number', 'year', 'panel'], 'paid_amount')
                result['P.26_OP_Panel_Benefit'] = p26

        p26a_sheet = result.get('P.26a_OP_Class_Panel_Benefit')
        if p26a_sheet is not None and not p26a_sheet.empty:
            p26a_sorted = p26a_sheet.copy()
            if 'paid_amount' in p26a_sorted.columns:
                p26a_sorted['paid_amount'] = pd.to_numeric(p26a_sorted['paid_amount'], errors='coerce').fillna(0)
            p26a_sorted = self._sort_with_desc(p26a_sorted, ['policy_number', 'year', 'class', 'panel'], 'paid_amount')
            result['P.26a_OP_Class_Panel_Benefit'] = p26a_sorted

        def _build_top_diag_from_class(
            source: Optional[pd.DataFrame],
            index_cols: List[str],
        ) -> Optional[pd.DataFrame]:
            if source is None or source.empty or not index_cols:
                return None
            work = source.copy()
            # Ensure identifier columns exist and normalise types
            missing_keys = [col for col in index_cols if col not in work.columns]
            if missing_keys:
                return None
            for col in index_cols:
                work[col] = work[col].astype(str)
            # Standardise case count column across variants
            if 'no_of_case' in work.columns and 'no_of_cases' not in work.columns:
                work['no_of_cases'] = work['no_of_case']
            base_cols = ['incurred_amount', 'paid_amount', 'no_of_claim_id', 'no_of_claimants']
            for col in base_cols:
                if col in work.columns:
                    work[col] = pd.to_numeric(work[col], errors='coerce').fillna(0)
                else:
                    work[col] = 0.0
            if 'no_of_cases' in work.columns:
                work['no_of_cases'] = pd.to_numeric(work['no_of_cases'], errors='coerce').fillna(0)
            elif 'no_of_claim_id' in work.columns:
                work['no_of_cases'] = pd.to_numeric(work['no_of_claim_id'], errors='coerce').fillna(0)
            else:
                work['no_of_cases'] = 0.0

            agg_cols = ['incurred_amount', 'paid_amount', 'no_of_claim_id', 'no_of_claimants', 'no_of_cases']
            agg = work.groupby(index_cols, dropna=False)[agg_cols].sum().reset_index()
            for col in ['incurred_amount', 'paid_amount', 'no_of_cases']:
                agg[col] = pd.to_numeric(agg[col], errors='coerce').fillna(0)
            zero_mask = (
                agg['incurred_amount'].eq(0)
                & agg['paid_amount'].eq(0)
                & agg['no_of_cases'].eq(0)
            )
            agg = agg.loc[~zero_mask].reset_index(drop=True)
            if agg.empty:
                return None
            sort_cols = index_cols + ['paid_amount']
            ascending = [True] * len(index_cols) + [False]
            agg = agg.sort_values(
                by=sort_cols,
                ascending=ascending,
                kind='mergesort'
            ).reset_index(drop=True)
            needed_cols = index_cols + [
                'incurred_amount', 'paid_amount',
                'no_of_cases', 'no_of_claim_id', 'no_of_claimants'
            ]
            for col in needed_cols:
                if col not in agg.columns:
                    agg[col] = pd.NA
            return agg[needed_cols]

        p18a_prepared = _build_top_diag_from_class(
            result.get('P.18a_Class_TopHosDiag'),
            ['policy_number', 'year', 'diagnosis'],
        )
        if p18a_prepared is None:
            p18a_prepared = _build_top_diag_from_class(
                all_sheets.get('P.18a_Class_TopHosDiag'),
                ['policy_number', 'year', 'diagnosis'],
            )
        if p18a_prepared is not None:
            result['P.18a_Class_TopHosDiag'] = p18a_prepared.copy()
            result['P.18_TopHosDiag'] = p18a_prepared.copy()

        p18b_prepared = _build_top_diag_from_class(
            result.get('P.18b_Class_TopClinDiag'),
            ['policy_number', 'year', 'diagnosis'],
        )
        if p18b_prepared is None:
            p18b_prepared = _build_top_diag_from_class(
                all_sheets.get('P.18b_Class_TopClinDiag'),
                ['policy_number', 'year', 'diagnosis'],
            )
        if p18b_prepared is not None:
            result['P.18b_Class_TopClinDiag'] = p18b_prepared.copy()
            result['P.18b_TopClinDiag'] = p18b_prepared.copy()

        p18b_dp_indexes = ['policy_number', 'year', 'day_procedure_flag', 'diagnosis']
        p18b_dp_prepared = _build_top_diag_from_class(
            result.get('P.18b_Class_IP_DayProc'),
            p18b_dp_indexes,
        )
        if p18b_dp_prepared is None:
            p18b_dp_prepared = _build_top_diag_from_class(
                all_sheets.get('P.18b_Class_IP_DayProc'),
                p18b_dp_indexes,
            )
        if p18b_dp_prepared is not None:
            result['P.18b_Class_IP_DayProc'] = p18b_dp_prepared.copy()
            result['P.18b_IP_DayProc'] = p18b_dp_prepared.copy()
        # Final de-duplication across all sheets to avoid duplicated unselected class records
        for sn in list(result.keys()):
            try:
                result[sn] = result[sn].drop_duplicates().reset_index(drop=True)
            except Exception:
                pass
        return result

    def export_excel(self, merged_sheets: Dict[str, pd.DataFrame]) -> bytes:
        """Write provided sheet DataFrames to a new Excel file and return its bytes."""
        from io import BytesIO
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            try:
                styles_module = import_module("openpyxl.styles")
                NamedStyle = getattr(styles_module, "NamedStyle")
                Alignment = getattr(styles_module, "Alignment")
                Font = getattr(styles_module, "Font")
                Border = getattr(styles_module, "Border")
                PatternFill = getattr(styles_module, "PatternFill")
                Protection = getattr(styles_module, "Protection")
            except (ImportError, AttributeError):
                named_style = None
            else:
                book = writer.book
                existing = {getattr(ns, "name", ns) for ns in book.named_styles}
                if "num" not in existing:
                    named_style = NamedStyle(name="num")
                    named_style.number_format = "#,##0"
                    named_style.alignment = Alignment(horizontal="center", vertical="center")
                    named_style.font = Font(name="Univers", size=14)
                    named_style.border = Border()
                    named_style.fill = PatternFill(fill_type=None)
                    named_style.protection = Protection(locked=False, hidden=False)
                    book.add_named_style(named_style)
                else:
                    named_style = None
            # Write in a stable order first, then any remaining sheets
            written = set()
            for sn in self.SUPPORTED_SHEETS:
                if sn in merged_sheets:
                    # align to first-seen schema when available
                    schema = self._sheet_schema.get(sn)
                    df_out = self._reorder_to_schema(merged_sheets[sn], schema)
                    # Preserve original column order if schema is known; otherwise standardize
                    if not schema:
                        df_out = self._standardize_col_order(df_out)
                    df_out.to_excel(writer, sheet_name=sn, index=False)
                    written.add(sn)
            for sn, df in merged_sheets.items():
                if sn not in written:
                    schema = self._sheet_schema.get(sn)
                    df_out = self._reorder_to_schema(df, schema)
                    if not schema:
                        df_out = self._standardize_col_order(df_out)
                    df_out.to_excel(writer, sheet_name=sn[:31], index=False)
            # ensure at least one sheet exists to avoid empty/corrupted file
            if not written:
                pd.DataFrame({"note": ["No merged data generated"]}).to_excel(writer, sheet_name="Merged_Index", index=False)
        return output.getvalue()
