from __future__ import annotations
import pandas as pd
from slugify import slugify
from rowllect.utils.dates import to_timecode_timevalue
from rowllect.utils.eav import finalize_eav_format

COUNTRY = "NP"
_EMPTY  = {"", "none", "nan", "null", "-", "n/a"}


def slug(val: str) -> str:
    s = str(val).strip().lower()
    return "" if (not s or s in _EMPTY) else slugify(str(val).strip())


def finalize(df: pd.DataFrame, year: int = 2021) -> pd.DataFrame:
    timecode, timevalue = to_timecode_timevalue(year)
    df["timecode"]  = timecode
    df["timevalue"] = timevalue
    return finalize_eav_format(df)


def build_eav_rows(
    df     : pd.DataFrame,
    prefix : str,
    dims   : list[str],
    meta_fn: callable,
) -> pd.DataFrame:
    # indicator = prefix / slug(dim1) / slug(dim2) / ...  (empty slugs skipped)
    records = []
    for _, row in df.iterrows():
        parts = [prefix] + [s for d in dims if (s := slug(str(row.get(d, ""))))]
        records.append({
            "code"     : row["code"],
            "country"  : COUNTRY,
            "feature"  : row["feature"],
            "indicator": "/".join(parts),
            "value"    : row["value"],
            "Meta"     : meta_fn(row),
        })
    return finalize(pd.DataFrame(records))