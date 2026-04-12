"""

Area name → numeric code resolution for all three table layouts.
"""

from __future__ import annotations

import pandas as pd
from loguru import logger
from voo.address import parse_address
from voo.locup import setup_lookup, get_district_code, get_province_code

COUNTRY = "NP"

# ---------------------------------------------------------------------------
# Public resolution functions (one per layout)
# ---------------------------------------------------------------------------

def resolve_flat(long_df: pd.DataFrame) -> pd.DataFrame:
    """
    Flat tables carry an area_code column and an area_name column.

    """
    df = long_df.copy()
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["code"]  = df["area_code"].astype(str).str.strip()
    df.dropna(subset=["value"], inplace=True)

    non_empty = df["code"][df["code"].isin(["", "nan"]) == False].unique()  # noqa: E712


        # Case B: ordinal index — resolve from area_name via voo
    logger.debug(
        f"resolve_flat: area_code column is ordinal (values {sorted(non_empty)[:8]}…), "
        f"resolving by area_name instead"
    )
    setup_lookup()
    names        = df["area_name"].astype(str).str.strip().unique()
    name_to_code = resolve_area_names(names)

    unmatched = [n for n in names if n and n not in name_to_code]
    if unmatched:
        logger.warning(f"Unmatched area names in flat table (dropped): {unmatched}")

    df["code"] = df["area_name"].astype(str).str.strip().map(name_to_code)
    df.dropna(subset=["code"], inplace=True)
    df["code"] = df["code"].astype(str)

    df["feature"] = df["code"].apply(feature_from_code)
    df["country"] = COUNTRY
    return df

def resolve_grouped(long_df: pd.DataFrame) -> pd.DataFrame:
    """
    Grouped tables carry area names (Nepal, Koshi, Taplejung …).
    Resolves each to a numeric code via the voo cascade.
    """
    df = long_df.copy()
    df["value"]    = pd.to_numeric(df["value"], errors="coerce")
    df["area_name"]= df["area_name"].astype(str).str.strip()
    df.dropna(subset=["value"], inplace=True)

    setup_lookup()
    name_to_code = resolve_area_names(df["area_name"].unique())
    unmatched = [n for n in df["area_name"].unique() if n not in name_to_code]
    if unmatched:
        logger.warning(f"Unmatched area names (dropped): {unmatched}")

    df["code"] = df["area_name"].map(name_to_code)
    df.dropna(subset=["code"], inplace=True)
    df["code"]    = df["code"].astype(str)
    df["feature"] = df["code"].apply(feature_from_code)
    df["country"] = COUNTRY
    return df


def resolve_hierarchical(long_df: pd.DataFrame) -> pd.DataFrame:
    """
    Hierarchical tables carry district names in a dedicated column.
    Resolve district → 3-digit code, derive province from first digit,
    generate palika codes as district_code + 2-digit suffix (sorted within district).
    """
    df = long_df.copy()
    for col in ["province", "district", "palika", "sex", "breakdown", "sector"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df.dropna(subset=["value"], inplace=True)

    setup_lookup()
    unique_districts = df[df["district"] != ""]["district"].unique()
    district_to_code = _resolve_districts(unique_districts)

    unmatched = [n for n in unique_districts if n not in district_to_code]
    if unmatched:
        logger.warning(f"Unmatched districts (dropped): {unmatched}")

    df["district_code"] = df["district"].map(district_to_code)
    df["province_code"] = df["district_code"].apply(
        lambda x: x[0] if pd.notna(x) and x else ""
    )

    df["palika_code"] = ""
    for dist_code, grp in df[df["district_code"].notna()].groupby("district_code"):
        palikas = sorted(p for p in grp["palika"].unique() if p)
        pmap    = {p: f"{dist_code}{str(i+1).zfill(2)}" for i, p in enumerate(palikas)}
        df.loc[grp.index, "palika_code"] = grp["palika"].map(pmap).fillna("")

    df = df[~((df["district"] != "") & df["district_code"].isna())].copy()
    return df


# ---------------------------------------------------------------------------
# voo resolution — cascaded name resolver
# ---------------------------------------------------------------------------

def resolve_area_names(names) -> dict[str, str]:
    """
    Resolve area name strings to numeric code strings.

    Returns {original_name: code_string} for every name that resolved.
    Unresolvable names (zones, institutional, etc.) are omitte
    """
    result: dict[str, str] = {}
    for name in names:
        if not name or name.lower() in ("nan", "none", ""):
            continue
        code = (
            _try_national(name)
            or _try_province(name)
            or _try_district(name)
            or _try_palika(name)
        )
        if code:
            result[name] = code
    return result


def feature_from_code(code: str) -> str:
    """Map a numeric code string to its warehouse feature string by length."""
    s = str(code).strip()
    if s == "0":
        return "PCL"
    return {1: "ADM1", 3: "ADM2", 5: "ADM3", 7: "ADM4"}.get(len(s), "PCL")

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _is_numeric_code(s: str) -> bool:
    try:
        return int(str(s).strip()) >= 0
    except (ValueError, TypeError):
        return False


def _try_national(name: str) -> str | None:
    if name.strip().lower() in {"nepal", "np", "national", "country"}:
        return "0"
    return None


def _try_province(name: str) -> str | None:
    code = get_province_code(name.lower())
    return str(code) if code else None


def _try_district(name: str) -> str | None:
    code = parse_address({"address": name}, {"resolve": "district"})
    if code is None:
        code = get_district_code(name)
    if code and len(str(code)) == 3:
        return str(code)
    return None


def _try_palika(name: str) -> str | None:
    code = parse_address({"address": name}, {"resolve": "palika"})
    if code and len(str(code)) == 5:
        return str(code)
    return None


def _resolve_districts(names) -> dict[str, str]:
    """Resolve district names only — used by resolve_hierarchical."""
    result: dict[str, str] = {}
    for name in names:
        if not name:
            continue
        code = parse_address({"address": name}, {"resolve": "district"})
        if code is None:
            code = get_district_code(name)
        if code:
            result[name] = str(code)
    return result