from __future__ import annotations
import re
import pandas as pd
from loguru import logger
from voo.address import parse_address
from voo.locup import setup_lookup, get_district_code, get_province_code

COUNTRY = "NP"


def feature_from_code(code: str) -> str:
    s = str(code).strip()
    if s == "0":
        return "PCL"
    return {1: "ADM1", 3: "ADM2", 5: "ADM3", 7: "ADM4"}.get(len(s), "PCL")


def attach_codes(df: pd.DataFrame, name_col: str) -> pd.DataFrame:
    """Resolve area names to codes. Drops rows that can't be resolved."""
    df = df.copy()
    df[name_col] = df[name_col].astype(str).str.strip()
    setup_lookup()
    name_to_code = _resolve_area_names(df[name_col].unique())
    unmatched = [n for n in df[name_col].unique() if n not in name_to_code]
    if unmatched:
        logger.warning(f"Unmatched area names (dropped): {unmatched}")
    df["code"]    = df[name_col].map(name_to_code)
    df.dropna(subset=["code"], inplace=True)
    df["code"]    = df["code"].astype(str)
    df["feature"] = df["code"].apply(feature_from_code)
    df["country"] = COUNTRY
    return df


def resolve_districts(names) -> dict[str, str]:
    setup_lookup()
    result: dict[str, str] = {}
    for name in names:
        if not name:
            continue
        code = parse_address({"address": name}, {"resolve": "district"}) or get_district_code(name)
        if code:
            result[name] = str(code)
    return result


def _resolve_area_names(names) -> dict[str, str]:
    result: dict[str, str] = {}
    for name in names:
        s = str(name).strip()
        if not s or s.lower() in ("nan", "none", ""):
            continue
        code = _national(s) or _province(s) or _district(s) or _palika(s)
        if code:
            result[name] = code
    return result


def _national(name: str) -> str | None:
    return "0" if name.strip().lower() in {"nepal", "np", "national", "country"} else None

def _province(name: str) -> str | None:
    cleaned = re.sub(r"\s+province$", "", name.strip().lower()).strip()
    code    = get_province_code(cleaned)
    return str(code) if code else None

def _district(name: str) -> str | None:
    code = parse_address({"address": name}, {"resolve": "district"}) or get_district_code(name)
    return str(code) if (code and len(str(code)) == 3) else None

def _palika(name: str) -> str | None:
    code = parse_address({"address": name}, {"resolve": "palika"})
    return str(code) if (code and len(str(code)) == 5) else None