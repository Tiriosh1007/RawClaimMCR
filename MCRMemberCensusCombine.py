"""Utilities for combining an MCR workbook with member census data.

This module reads an existing MCR Excel workbook (exported from the RawClaimData
workflow) and a member census summary, aligns the two using policy and class
mappings, injects member counts into each relevant MCR sheet, and calculates
incidence-rate metrics where possible.
"""

from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from io import BytesIO
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
from openpyxl.styles import Alignment, Border, Font, NamedStyle, PatternFill, Protection


@dataclass
class PolicyMatchSuggestion:
    """Suggested mapping between an MCR policy number and a census policy."""

    mcr_policy: str
    matched_member_policy: Optional[str]
    match_score: float

    def as_dict(self) -> Dict[str, object]:
        return {
            "mcr_policy": self.mcr_policy,
            "matched_member_policy": self.matched_member_policy,
            "match_score": self.match_score,
        }


@dataclass
class ClassMatchSuggestion:
    """Suggested mapping between a census class and an MCR class."""

    member_policy: str
    member_class: str
    member_year: Optional[str]
    member_class_id: str
    mcr_policy: str
    mcr_class: Optional[str]
    mcr_year: Optional[str]
    mcr_class_id: Optional[str]
    match_score: float
    missing_mcr_class: bool = False
    suggested_mcr_class: Optional[str] = None
    suggested_match_score: float = 0.0

    def as_dict(self) -> Dict[str, object]:
        return {
            "member_policy": self.member_policy,
            "member_class": self.member_class,
            "member_year": self.member_year,
            "member_class_id": self.member_class_id,
            "mcr_policy": self.mcr_policy,
            "mcr_class": self.mcr_class,
            "mcr_year": self.mcr_year,
            "mcr_class_id": self.mcr_class_id,
            "match_score": self.match_score,
            "missing_mcr_class": self.missing_mcr_class,
            "suggested_mcr_class": self.suggested_mcr_class,
            "suggested_match_score": self.suggested_match_score,
        }


class MCRMemberCensusCombiner:
    """Combine MCR workbook sheets with member census counts."""

    MEMBER_COUNT_COL = "member_count"
    _MATCH_PLACEHOLDER = "__UNMATCHED__"
    NOT_IN_MCR_LABEL = "No Matching Class"
    SHEET_IDENTIFIER_OVERRIDES = {
            "P.30_OP_FreqClaimant": ["policy_number", "year", "class", "dep_type"],
    }

    def __init__(
        self,
        mcr_file: BytesIO,
        member_file: BytesIO,
        mcr_filename: Optional[str] = None,
        member_filename: Optional[str] = None,
    ) -> None:
        self._mcr_filename = mcr_filename or "mcr.xlsx"
        self._member_filename = member_filename or "member_census.xlsx"
        self.mcr_sheets: Dict[str, pd.DataFrame] = self._read_mcr_workbook(mcr_file)
        self.member_raw: pd.DataFrame = self._load_member_census(member_file, member_filename)
        self.warnings: List[str] = []
        self._combined_sheets: Optional[Dict[str, pd.DataFrame]] = None

        self.mcr_policy_info = self.mcr_sheets.get("Policy_Info", pd.DataFrame())
        self.mcr_policy_years = self._extract_policy_years()
        self.mcr_policy_classes = self._extract_mcr_classes()
        self.mcr_classes_by_policy = self._collect_mcr_classes_by_policy()
        self.member_policy_options = self._collect_member_policies()
        self.member_classes_by_policy = self._collect_member_classes()

    # ------------------------------------------------------------------
    # Public suggestion helpers
    # ------------------------------------------------------------------
    def suggest_policy_matches(self) -> pd.DataFrame:
        """Return default policy mapping suggestions as a DataFrame."""

        suggestions: List[PolicyMatchSuggestion] = []
        mcr_policies = self.mcr_policy_years.keys()
        for mcr_policy in sorted(mcr_policies):
            best_policy, score = self._best_match(mcr_policy, self.member_policy_options)
            suggestions.append(
                PolicyMatchSuggestion(
                    mcr_policy=mcr_policy,
                    matched_member_policy=best_policy,
                    match_score=round(score, 3),
                )
            )
        if not suggestions:
            return pd.DataFrame(columns=["mcr_policy", "matched_member_policy", "match_score"])
        return pd.DataFrame([s.as_dict() for s in suggestions])

    def suggest_class_matches(self, policy_mapping: pd.DataFrame) -> pd.DataFrame:
        """Return default class mapping suggestions as a DataFrame."""

        expected_cols = {"mcr_policy", "matched_member_policy"}
        if not expected_cols.issubset(policy_mapping.columns):
            raise ValueError("policy_mapping must include 'mcr_policy' and 'matched_member_policy'.")

        # Normalise mapping to dict using raw member policy names
        policy_map = {
            str(row.matched_member_policy): str(row.mcr_policy)
            for row in policy_mapping.itertuples(index=False)
            if getattr(row, "matched_member_policy", None)
        }

        suggestions: List[ClassMatchSuggestion] = []
        if self.mcr_policy_classes.empty or self.member_raw.empty:
            return pd.DataFrame(columns=[
                "member_policy",
                "member_class",
                "member_year",
                "mcr_policy",
                "mcr_class",
                "mcr_year",
                "match_score",
            ])

        def _normalised_lookup(values: Iterable[str]) -> Dict[str, str]:
            lookup: Dict[str, str] = {}
            for item in values:
                key = self._normalise_key(item)
                if not key:
                    continue
                lookup[key] = str(item)
            return lookup

        for member_policy, mcr_policy in policy_map.items():
            mcr_policy_str = str(mcr_policy)
            member_policy_str = str(member_policy)

            member_subset = self.member_raw.loc[
                self.member_raw["policy_number_raw"].astype(str) == member_policy_str
            ][["class_raw", "year"]].copy()
            if member_subset.empty:
                continue
            member_subset["class_raw"] = member_subset["class_raw"].astype(str).str.strip()
            member_subset["class_raw"] = member_subset["class_raw"].replace({"": np.nan, "nan": np.nan})
            member_subset.dropna(subset=["class_raw"], inplace=True)
            member_subset["year"] = member_subset["year"].astype(str).str.extract(r"(\d{4})", expand=False)
            member_subset.drop_duplicates(inplace=True)
            member_subset.sort_values(by=["class_raw", "year"], inplace=True)
            member_subset["member_class_id"] = member_policy_str + "_" + member_subset["class_raw"].astype(str)

            mcr_subset = self.mcr_policy_classes.loc[
                self.mcr_policy_classes["mcr_policy"].astype(str) == mcr_policy_str
            ][["mcr_class", "mcr_year", "mcr_class_id"]].copy()
            mcr_subset["mcr_class"] = mcr_subset["mcr_class"].astype(str).str.strip()
            mcr_subset["mcr_class"] = mcr_subset["mcr_class"].replace({"": np.nan, "nan": np.nan})
            mcr_subset.dropna(subset=["mcr_class"], inplace=True)
            mcr_classes = mcr_subset["mcr_class"].unique().tolist()
            mcr_lookup = _normalised_lookup(mcr_classes)
            mcr_year_lookup = {
                self._normalise_key(row.mcr_class): row.mcr_year
                for row in mcr_subset.itertuples(index=False)
                if self._normalise_key(row.mcr_class)
            }
            mcr_id_lookup = {
                self._normalise_key(row.mcr_class): row.mcr_class_id
                for row in mcr_subset.itertuples(index=False)
                if self._normalise_key(row.mcr_class)
            }

            for record in member_subset.itertuples(index=False):
                census_class = str(record.class_raw)
                census_year = getattr(record, "year", None)
                member_class_id = str(getattr(record, "member_class_id"))
                best_candidate, best_score = self._best_match_raw(census_class, mcr_classes)
                best_candidate_norm = self._normalise_key(best_candidate) if best_candidate else ""
                census_norm = self._normalise_key(census_class)
                exact_match = None
                if best_candidate and best_candidate_norm == census_norm and census_norm in mcr_lookup:
                    exact_match = mcr_lookup[census_norm]
                match_year = mcr_year_lookup.get(best_candidate_norm if exact_match else census_norm)
                matched_id = None
                if exact_match:
                    matched_id = mcr_id_lookup.get(census_norm) or (mcr_policy_str + "_" + exact_match)

                suggestions.append(
                    ClassMatchSuggestion(
                        member_policy=member_policy_str,
                        member_class=census_class,
                        member_year=census_year,
                        member_class_id=member_class_id,
                        mcr_policy=mcr_policy_str,
                        mcr_class=exact_match,
                        mcr_year=match_year,
                        mcr_class_id=matched_id,
                        match_score=1.0 if exact_match else 0.0,
                        missing_mcr_class=exact_match is None,
                        suggested_mcr_class=best_candidate,
                        suggested_match_score=round(best_score, 3),
                    )
                )

        if not suggestions:
            return pd.DataFrame(columns=[
                "member_policy",
                "member_class",
                "member_year",
                "mcr_policy",
                "mcr_class",
                "mcr_year",
                "match_score",
            ])

        return pd.DataFrame([s.as_dict() for s in suggestions])

    # ------------------------------------------------------------------
    # Public integration API
    # ------------------------------------------------------------------
    def combine(self, policy_mapping: pd.DataFrame, class_mapping: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Merge member census data into the MCR workbook using the supplied mappings."""

        required_policy_cols = {"mcr_policy", "matched_member_policy"}
        if not required_policy_cols.issubset(policy_mapping.columns):
            raise ValueError("policy_mapping is missing required columns.")

        required_class_cols = {
            "mcr_policy",
            "mcr_class",
            "mcr_class_id",
            "member_policy",
            "member_class",
            "member_class_id",
        }
        missing_class_cols = required_class_cols.difference(class_mapping.columns)
        if missing_class_cols:
            raise ValueError(
                "class_mapping is missing required columns: " + ", ".join(sorted(missing_class_cols))
            )

        # Reset warnings per run
        self.warnings = []

        mapped_members = self._apply_mappings(policy_mapping, class_mapping)
        aggregates = self._build_member_aggregates(mapped_members)

        combined_sheets: Dict[str, pd.DataFrame] = {}
        for sheet_name, sheet_df in self.mcr_sheets.items():
            combined_sheets[sheet_name] = self._merge_members_into_sheet(sheet_name, sheet_df.copy(), aggregates)

        self._combined_sheets = combined_sheets
        return combined_sheets

    def export(self) -> bytes:
        """Export the most recent combined workbook as Excel bytes."""

        if self._combined_sheets is None:
            raise ValueError("combine() must be called before export().")

        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            for sheet_name, df in self._combined_sheets.items():
                safe_name = sheet_name[:31]
                df.to_excel(writer, sheet_name=safe_name, index=False)

            wb = writer.book

            num_style = NamedStyle(name="num")
            num_style.number_format = "#,##0"
            num_style.alignment = Alignment(horizontal="center", vertical="center")
            num_style.font = Font(name="Univers", size=14)
            num_style.border = Border()
            num_style.fill = PatternFill(fill_type=None)
            num_style.protection = Protection(locked=False, hidden=False)

            existing_styles = [s.name for s in wb._named_styles]
            if num_style.name not in existing_styles:
                wb.add_named_style(num_style)
            else:
                num_style = next(s for s in wb._named_styles if s.name == num_style.name)
        output.seek(0)
        return output.getvalue()

    def preview_sheet(self, sheet_name: str, rows: int = 20) -> pd.DataFrame:
        """Return a head of the requested sheet for preview purposes."""

        if self._combined_sheets is None:
            return pd.DataFrame()
        df = self._combined_sheets.get(sheet_name)
        if df is None:
            return pd.DataFrame()
        return df.head(rows)

    # ------------------------------------------------------------------
    # Internal helpers: reading and normalisation
    # ------------------------------------------------------------------
    def _read_mcr_workbook(self, file_like: BytesIO) -> Dict[str, pd.DataFrame]:
        try:
            sheets = pd.read_excel(file_like, sheet_name=None)
        except ValueError:
            # Excel writer may require seeking to start
            file_like.seek(0)
            sheets = pd.read_excel(file_like, sheet_name=None)
        cleaned: Dict[str, pd.DataFrame] = {}
        for name, df in sheets.items():
            df = self._normalize_dataframe(df)
            cleaned[name] = df
        return cleaned

    def _load_member_census(self, file_like: BytesIO, filename: Optional[str]) -> pd.DataFrame:
        file_like.seek(0)
        file_ext = (filename or "").lower()
        if file_ext.endswith(".csv"):
            df = pd.read_csv(file_like)
        else:
            df = pd.read_excel(file_like)
        if df.empty:
            raise ValueError("The member census file is empty.")
        df = self._normalize_dataframe(df)

        return self._standardize_member_dataframe(df)

    @staticmethod
    def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        """Trim whitespace and normalise column names."""

        df = df.copy()
        df.columns = [str(col).strip() for col in df.columns]
        for col in df.columns:
            if pd.api.types.is_string_dtype(df[col]):
                df[col] = df[col].astype(str).str.strip()
        return df

    # ------------------------------------------------------------------
    # Internal helpers: extraction and suggestions
    # ------------------------------------------------------------------
    def _extract_policy_years(self) -> Dict[str, List[str]]:
        """Create mapping of policy_number -> list of years appearing in the MCR."""

        sources: List[pd.DataFrame] = []
        possible_sources = [
            ("Policy_Info", ["policy_number", "year"]),
            ("P.20_Policy", ["policy_number", "year"]),
            ("P.21_Class", ["policy_number", "year"]),
        ]
        for sheet_name, columns in possible_sources:
            df = self.mcr_sheets.get(sheet_name)
            if df is None or df.empty:
                continue
            if not set(columns).issubset(df.columns):
                continue
            sources.append(df[columns].dropna())

        policy_years: Dict[str, List[str]] = {}
        for src in sources:
            frame = src.copy()
            frame["policy_number"] = frame["policy_number"].astype(str)
            frame["year"] = frame["year"].astype(str)
            for row in frame.itertuples(index=False):
                policy_years.setdefault(row.policy_number, []).append(row.year)

        # Deduplicate whilst preserving order
        for policy, years in policy_years.items():
            seen = set()
            policy_years[policy] = [y for y in years if not (y in seen or seen.add(y))]
        return policy_years

    def _extract_mcr_classes(self) -> pd.DataFrame:
        """Extract policy/year/class combinations from the MCR sheets."""

        class_sources = ["P.21_Class", "P.22_Class_BenefitType"]
        for sheet_name in class_sources:
            df = self.mcr_sheets.get(sheet_name)
            if df is None or df.empty:
                continue
            if not {"policy_number", "class"}.issubset(df.columns):
                continue
            work = df.copy()
            work["policy_number"] = work["policy_number"].ffill()
            available_cols = [col for col in ["policy_number", "year", "class"] if col in work.columns]
            subset = work[available_cols].dropna(subset=["policy_number", "class"]).copy()
            subset["policy_number"] = subset["policy_number"].astype(str).str.strip()
            subset["class"] = subset["class"].astype(str).str.strip()
            if "year" not in subset.columns:
                subset["year"] = np.nan
            else:
                subset["year"] = subset["year"].astype(str).str.extract(r"(\d{4})", expand=False)
            subset.rename(
                columns={
                    "policy_number": "mcr_policy",
                    "class": "mcr_class",
                    "year": "mcr_year",
                },
                inplace=True,
            )
            subset["mcr_class_id"] = subset["mcr_policy"].astype(str) + "_" + subset["mcr_class"].astype(str)
            subset.drop_duplicates(inplace=True)
            return subset.reset_index(drop=True)
        return pd.DataFrame(columns=["mcr_policy", "mcr_year", "mcr_class", "mcr_class_id"])

    def _collect_mcr_classes_by_policy(self) -> Dict[str, List[str]]:
        if self.mcr_policy_classes.empty:
            return {}
        grouped = self.mcr_policy_classes.dropna(subset=["mcr_policy", "mcr_class"])
        grouped["mcr_policy"] = grouped["mcr_policy"].astype(str)
        grouped["mcr_class"] = grouped["mcr_class"].astype(str)
        mapping: Dict[str, List[str]] = {}
        for policy, frame in grouped.groupby("mcr_policy"):
            classes = frame["mcr_class"].dropna().astype(str).str.strip()
            classes = classes.replace({"": np.nan}).dropna().unique().tolist()
            classes.sort()
            mapping[str(policy)] = classes
        return mapping

    def _collect_member_policies(self) -> List[str]:
        if self.member_raw.empty:
            return []
        return sorted(self.member_raw["policy_number_raw"].dropna().unique().astype(str))

    def _collect_member_classes(self) -> Dict[str, List[str]]:
        if self.member_raw.empty:
            return {}
        by_policy: Dict[str, List[str]] = {}
        grouped = self.member_raw.dropna(subset=["policy_number_raw"]).groupby("policy_number_raw")
        for policy, frame in grouped:
            classes = (
                frame["class_raw"].dropna().astype(str).str.strip().replace({"": np.nan}).dropna().unique().tolist()
            )
            classes.sort()
            by_policy[str(policy)] = classes
        return by_policy

    # ------------------------------------------------------------------
    # Internal helpers: matching and mapping
    # ------------------------------------------------------------------
    @staticmethod
    def _normalise_key(value: object) -> str:
        return str(value).strip().lower() if value is not None else ""

    def _best_match_raw(self, needle: str, haystack: Iterable[str]) -> Tuple[Optional[str], float]:
        """Return the raw best fuzzy match (without score thresholding)."""

        if not haystack:
            return None, 0.0
        needle_key = self._normalise_key(needle)
        if not needle_key:
            return None, 0.0
        best_candidate: Optional[str] = None
        best_score = 0.0
        for candidate in haystack:
            candidate_key = self._normalise_key(candidate)
            if not candidate_key:
                continue
            score = SequenceMatcher(None, needle_key, candidate_key).ratio()
            if score > best_score:
                best_score = score
                best_candidate = candidate
        if best_candidate is None:
            return None, 0.0
        return best_candidate, best_score

    def _best_match(self, needle: str, haystack: Iterable[str]) -> Tuple[Optional[str], float]:
        """Return best fuzzy match from haystack for needle using a confidence threshold."""

        candidate, score = self._best_match_raw(needle, haystack)
        if candidate is None or score < 0.4:
            return None, score if score > 0 else 0.0
        return candidate, score

    def _apply_mappings(
        self,
        policy_mapping: pd.DataFrame,
        class_mapping: pd.DataFrame,
    ) -> pd.DataFrame:
        """Apply the supplied mappings to the raw member census data."""

        members = self.member_raw.copy()

        policy_mapping = policy_mapping.copy()
        policy_mapping["matched_member_policy"] = policy_mapping["matched_member_policy"].astype(str)
        policy_mapping["mcr_policy"] = policy_mapping["mcr_policy"].astype(str)
        policy_map = dict(
            zip(policy_mapping["matched_member_policy"], policy_mapping["mcr_policy"])
        )
        members["policy_number"] = members["policy_number_raw"].map(policy_map)
        members = members.dropna(subset=["policy_number"])

        class_mapping = class_mapping.copy()
        class_mapping["member_policy"] = class_mapping["member_policy"].astype(str).str.strip()
        class_mapping["member_class"] = class_mapping["member_class"].astype(str).str.strip()
        class_mapping["member_class_id"] = class_mapping["member_class_id"].astype(str).str.strip()
        class_mapping["mcr_policy"] = class_mapping["mcr_policy"].astype(str).str.strip()
        class_mapping["mcr_class"] = class_mapping["mcr_class"].astype(str).str.strip()
        class_mapping["mcr_class_id"] = class_mapping["mcr_class_id"].astype(str).str.strip()

        class_id_map = {
            row.member_class_id: (row.mcr_class, row.mcr_class_id)
            for row in class_mapping.itertuples(index=False)
        }

        def _map_class(row: pd.Series) -> Tuple[str, Optional[str]]:
            member_class_id = str(row.get("member_class_id"))
            mapped = class_id_map.get(member_class_id)
            if mapped:
                return mapped[0], mapped[1]
            key = (str(row["policy_number_raw"]), str(row["class_raw"]))
            fallback = class_id_map.get(f"{key[0]}_{key[1]}")
            if fallback:
                return fallback[0], fallback[1]
            return str(row["class_raw"]), None

        mapped_classes = members.apply(_map_class, axis=1, result_type="expand")
        mapped_classes.columns = ["class", "mcr_class_id"]
        members["class"] = mapped_classes["class"]
        members["mcr_class_id"] = mapped_classes["mcr_class_id"]
        members["class"] = members["class"].replace({"nan": np.nan}).astype(str).str.strip()
        members.loc[members["class"].isin(["", "nan", "None"]), "class"] = np.nan
        members["mcr_class_id"] = members["mcr_class_id"].astype(str).replace({"nan": np.nan, "None": np.nan}).str.strip()

        members["dep_type"] = members["dep_type_raw"].astype(str).str.upper().str.strip()
        members.loc[members["dep_type"].isin(["", "nan", "NONE", "NAN"]), "dep_type"] = np.nan

        # Ensure year is populated; if census year missing but MCR has single year, fill in
        members["year"] = members["year"].astype(str).replace({"nan": np.nan})
        missing_year_mask = members["year"].isna()
        if missing_year_mask.any():
            filled_years = []
            for row in members.loc[missing_year_mask].itertuples():
                policy_years = self.mcr_policy_years.get(str(row.policy_number), [])
                filled_years.append(policy_years[0] if len(policy_years) == 1 else np.nan)
            members.loc[missing_year_mask, "year"] = filled_years

        members["year"] = members["year"].astype(str).str.extract(r"(\d{4})", expand=False)

        members[self.MEMBER_COUNT_COL] = pd.to_numeric(members[self.MEMBER_COUNT_COL], errors="coerce").fillna(0)

        members = members.dropna(subset=["policy_number"])
        return members

    # ------------------------------------------------------------------
    # Internal helpers: aggregation and merging
    # ------------------------------------------------------------------
    def _build_member_aggregates(self, members: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Generate member count aggregates at multiple granularities."""

        work = members.copy()

        # Clean class and dep values for grouping
        for col in ["policy_number", "class", "dep_type", "year"]:
            if col in work.columns:
                work[col] = work[col].astype(str).replace({"nan": np.nan, "None": np.nan})
                work[col] = work[col].str.strip()

        aggregates: Dict[str, pd.DataFrame] = {}

        def _aggregate(group_cols: List[str]) -> pd.DataFrame:
            present = [col for col in group_cols if col in work.columns]
            if not present:
                return pd.DataFrame(columns=group_cols + [self.MEMBER_COUNT_COL])
            grouped = (
                work.dropna(subset=[present[0]])
                .groupby(present, dropna=False)[self.MEMBER_COUNT_COL]
                .sum()
                .reset_index()
            )
            return grouped[present + [self.MEMBER_COUNT_COL]]

        aggregates["policy_year"] = _aggregate(["policy_number", "year"])
        aggregates["policy_year_class"] = _aggregate(["policy_number", "year", "class"])
        aggregates["policy_year_dep"] = _aggregate(["policy_number", "year", "dep_type"])
        aggregates["policy_year_class_dep"] = _aggregate(
            ["policy_number", "year", "class", "dep_type"]
        )

        return aggregates

    def _merge_members_into_sheet(
        self,
        sheet_name: str,
        df: pd.DataFrame,
        aggregates: Dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """Merge the appropriate aggregate into a sheet and compute incidence rates."""

        if df.empty:
            return df

        df = df.copy()
        if sheet_name == "P.30_OP_FreqClaimant":
            # Standardise column names for the new frequent claimant page so member counts can merge cleanly
            rename_map = {}
            for col in df.columns:
                key = str(col).strip().lower().replace(" ", "_")
                if key == "policy_number":
                    rename_map[col] = "policy_number"
                elif key == "year":
                    rename_map[col] = "year"
                elif key in {"class", "plan"}:
                    rename_map[col] = "class"
                elif key in {"dep_type", "dep_type_group", "dep", "dep_group"}:
                    rename_map[col] = "dep_type"
                elif key in {"no_of_claimants", "no_of_claimant"}:
                    rename_map[col] = "no_of_claimants"
                elif key in {"no_of_cases", "cases"}:
                    rename_map[col] = "no_of_cases"
            if rename_map:
                df.rename(columns=rename_map, inplace=True)

        override_identifiers = self.SHEET_IDENTIFIER_OVERRIDES.get(sheet_name)
        if override_identifiers:
            identifier_cols = [col for col in override_identifiers if col in df.columns]
        else:
            identifier_cols = [col for col in ["policy_number", "year", "class", "dep_type"] if col in df.columns]
        if not identifier_cols:
            return df

        for col in identifier_cols:
            if col == "year":
                df[col] = df[col].astype(str).str.extract(r"(\d{4})", expand=False)
            else:
                df[col] = df[col].astype(str).str.strip()
                df.loc[df[col].isin(["nan", "None", ""]), col] = np.nan

        if {"class", "dep_type"}.issubset(identifier_cols):
            lookup_key = "policy_year_class_dep"
        elif "class" in identifier_cols:
            lookup_key = "policy_year_class"
        elif "dep_type" in identifier_cols:
            lookup_key = "policy_year_dep"
        else:
            lookup_key = "policy_year"

        lookup = aggregates.get(lookup_key)
        if lookup is None or lookup.empty:
            self.warnings.append(
                f"{sheet_name}: No member census data available for columns {identifier_cols}."
            )
            return df

        merge_cols = [col for col in identifier_cols if col in lookup.columns]
        if not merge_cols:
            return df

        extra_dim_cols = [col for col in ["policy_number", "year", "class", "dep_type"] if col in lookup.columns]
        if set(merge_cols) != set(extra_dim_cols):
            grouped_lookup = (
                lookup.groupby(merge_cols, dropna=False)[self.MEMBER_COUNT_COL]
                .sum()
                .reset_index()
            )
        else:
            grouped_lookup = lookup.copy()

        df = df.merge(grouped_lookup, on=merge_cols, how="left")

        if "class" in merge_cols:
            df = self._insert_missing_class_rows(df, grouped_lookup, merge_cols)

        member_counts = pd.to_numeric(df[self.MEMBER_COUNT_COL], errors="coerce")
        zero_mask = member_counts.fillna(0) == 0
        if zero_mask.any():
            missing_rows = df.loc[zero_mask, merge_cols].drop_duplicates()
            if not missing_rows.empty:
                self.warnings.append(
                    f"{sheet_name}: {len(missing_rows)} row(s) missing member counts after merge."
                )

        denominator = member_counts.replace({0: np.nan})

        def _safe_ratio(numerator_col: str, output_col: str) -> None:
            if numerator_col not in df.columns or output_col in df.columns:
                return
            numerator = pd.to_numeric(df[numerator_col], errors="coerce")
            df[output_col] = numerator / denominator

        _safe_ratio("no_of_cases", "incidence_rate_case")
        _safe_ratio("no_of_claim_id", "incidence_rate_claim")
        _safe_ratio("no_of_claimants", "incidence_rate_claimant")

        if "member_census_class" in df.columns:
            df.drop(columns=["member_census_class"], inplace=True)

        return df

    def _insert_missing_class_rows(
        self,
        df: pd.DataFrame,
        grouped_lookup: pd.DataFrame,
        merge_cols: List[str],
    ) -> pd.DataFrame:
        """Ensure classes absent from the original MCR sheet are reintroduced with census counts."""

        if grouped_lookup is None or grouped_lookup.empty:
            return df

        sentinel = "__MCR_CLASS_SENTINEL__"
        existing_keys = set(
            tuple(row)
            for row in df[merge_cols].fillna(sentinel).itertuples(index=False, name=None)
        )

        if "member_census_class" not in df.columns:
            df["member_census_class"] = np.nan
        columns_order = list(df.columns)

        new_rows: List[Dict[str, object]] = []
        lookup_records = grouped_lookup.to_dict(orient="records")

        for record in lookup_records:
            key = tuple(
                sentinel if pd.isna(record.get(col)) else record.get(col)
                for col in merge_cols
            )
            if key in existing_keys:
                continue
            existing_keys.add(key)
            new_row = {col: np.nan for col in columns_order}
            for col in columns_order:
                if col in record:
                    new_row[col] = record[col]
            census_class = record.get("class")
            if census_class is not None:
                new_row["class"] = census_class
                new_row["member_census_class"] = census_class
            new_rows.append(new_row)

        if not new_rows:
            return df

        new_rows_df = pd.DataFrame(new_rows, columns=columns_order)
        combined = pd.concat([df, new_rows_df], ignore_index=True)
        combined.loc[combined["member_census_class"].isna(), "member_census_class"] = combined.loc[
            combined["member_census_class"].isna(), "class"
        ]
        return combined

    # ------------------------------------------------------------------
    # Member census normalisation
    # ------------------------------------------------------------------
    @staticmethod
    def _normalise_column_name(name: str) -> str:
        return (
            str(name)
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
            .replace(".", "_")
        )

    def _standardize_member_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create a standardised member DataFrame with original and mapped columns."""

        normalised = {self._normalise_column_name(col): col for col in df.columns}

        def _col(*aliases: str) -> Optional[str]:
            for alias in aliases:
                if alias in normalised:
                    return normalised[alias]
            return None

        policy_col = _col(
            "policy_number",
            "policy_no",
            "policyid",
            "policy_id",
            "policy_holder_no",
            "cont_no",
            "polno",
        )
        if policy_col is None:
            raise ValueError("Member census file is missing a policy number column.")

        class_col = _col("class", "plan", "coverage", "tier", "cls_id", "med_hp_cls", "medical_tier")
        dep_type_col = _col("dep_type", "dependent_type", "relationship", "dep", "mbr_type", "dep_type_raw")
        year_col = _col("year", "policy_year")
        start_date_col = _col("policy_start_date", "start_date", "eff_date", "policy_eff_date")
        member_count_col = _col("member_count", "count", "members", "total_members", "num")

        # Identify wide dependent columns (EE, SP, CH, etc.)
        dep_wide_cols = [
            normalised[name]
            for name in normalised
            if name in {"ee", "employee", "sp", "spouse", "ch", "child"}
        ]

        work = df.copy()
        work[policy_col] = work[policy_col].astype(str).str.strip()
        if class_col:
            work[class_col] = work[class_col].astype(str).str.strip()
        else:
            class_col = "__class"
            work[class_col] = np.nan
        if dep_type_col:
            work[dep_type_col] = work[dep_type_col].astype(str).str.strip()

        if year_col:
            work[year_col] = work[year_col].astype(str).str.extract(r"(\d{4})", expand=False)
        elif start_date_col and start_date_col in work.columns:
            parsed_dates = pd.to_datetime(work[start_date_col], errors="coerce")
            work["__year"] = parsed_dates.dt.year.astype("Int64").astype(str)
            year_col = "__year"
        else:
            work["__year"] = np.nan
            year_col = "__year"

        if member_count_col and member_count_col in work.columns:
            work[member_count_col] = pd.to_numeric(work[member_count_col], errors="coerce").fillna(0)
        else:
            work["__count"] = 1
            member_count_col = "__count"

        if dep_type_col:
            subset = work[[policy_col, class_col, dep_type_col, year_col, member_count_col]].copy()
            subset.columns = [
                "policy_number_raw",
                "class_raw",
                "dep_type_raw",
                "year",
                self.MEMBER_COUNT_COL,
            ]
        elif dep_wide_cols:
            id_cols = [policy_col, class_col, year_col]
            melt_df = work.melt(
                id_vars=id_cols,
                value_vars=dep_wide_cols,
                var_name="dep_type_raw",
                value_name=self.MEMBER_COUNT_COL,
            )
            melt_df[self.MEMBER_COUNT_COL] = pd.to_numeric(
                melt_df[self.MEMBER_COUNT_COL], errors="coerce"
            ).fillna(0)
            melt_df.rename(
                columns={
                    policy_col: "policy_number_raw",
                    class_col: "class_raw",
                    year_col: "year",
                },
                inplace=True,
            )
            melt_df["year"] = melt_df[year_col]
            subset = melt_df[[
                "policy_number_raw",
                "class_raw",
                "dep_type_raw",
                "year",
                self.MEMBER_COUNT_COL,
            ]]
        else:
            subset = work[[policy_col, class_col, year_col, member_count_col]].copy()
            subset.insert(2, "dep_type_raw", np.nan)
            subset.columns = [
                "policy_number_raw",
                "class_raw",
                "dep_type_raw",
                "year",
                self.MEMBER_COUNT_COL,
            ]

        standardized = subset.copy()
        standardized[self.MEMBER_COUNT_COL] = pd.to_numeric(
            standardized[self.MEMBER_COUNT_COL], errors="coerce"
        ).fillna(0)
        return standardized