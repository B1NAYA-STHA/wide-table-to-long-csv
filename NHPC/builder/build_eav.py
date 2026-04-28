from __future__ import annotations

import pandas as pd
from loguru import logger
from slugify import slugify

from rowllect.aggregate.location_aggregator import LocationAggregator
from rowllect.utils.dates import to_timecode_timevalue
from rowllect.utils.eav import finalize_eav_format
from .resolve import feature_from_code

COUNTRY = "NP"


def _slug(val: str) -> str:
    """Slugify a string. Returns '' if empty — callers filter out empty parts."""
    return slugify(str(val).strip()) if val and str(val).strip() else ""


def build_eav(
    clean_df        : pd.DataFrame,
    layout          : str,
    indicator_prefix: str,
    breakdown_label : str,
) -> pd.DataFrame:
    if layout == "hierarchical":
        return build_eav_hierarchical(clean_df, indicator_prefix, breakdown_label)
    if layout == "grouped":
        return build_eav_grouped(clean_df, indicator_prefix)
    return build_eav_flat(clean_df, indicator_prefix)


def build_eav_flat(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    records = []
    for _, row in df.iterrows():
        ind_label = row["indicator"]
        ind_slug  = _slug(ind_label) or "metric"
        parts     = [p for p in [prefix, ind_slug] if p]
        records.append({
            "code"     : row["code"],
            "country"  : COUNTRY,
            "feature"  : row["feature"],
            "indicator": "/".join(parts),
            "value"    : row["value"],
            "Meta"     : [{"label": ind_label, "slug": ind_slug, "category": "Metric"}],
        })
    return _finalize(pd.DataFrame(records))


def build_eav_grouped(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    records = []
    for _, row in df.iterrows():
        cat_label = str(row.get("category") or "")
        ind_label = row["indicator"]
        cat_slug  = _slug(cat_label)
        ind_slug  = _slug(ind_label) or "metric"
        parts     = [p for p in [prefix, cat_slug, ind_slug] if p]
        records.append({
            "code"     : row["code"],
            "country"  : COUNTRY,
            "feature"  : row["feature"],
            "indicator": "/".join(parts),
            "value"    : row["value"],
            "Meta"     : [
                {"label": cat_label, "slug": cat_slug, "category": "Category"},
                {"label": ind_label, "slug": ind_slug, "category": "Metric"},
            ],
        })
    return _finalize(pd.DataFrame(records))


def build_eav_hierarchical(
    df             : pd.DataFrame,
    prefix         : str,
    breakdown_label: str,
) -> pd.DataFrame:
    combos        = df[["sex", "breakdown", "sector"]].drop_duplicates()
    indicator_map : dict[tuple, str] = {}
    meta_map      : dict[tuple, list] = {}

    for _, row in combos.iterrows():
        sex_label = str(row["sex"]).strip()
        bd_label  = str(row["breakdown"]).strip()
        sec_label = str(row["sector"]).strip()

        sex_slug = _slug(sex_label)
        bd_slug  = _slug(bd_label)
        sec_slug = _slug(sec_label)

        # Only include non-empty slugs — no "none" placeholders
        parts = [p for p in [prefix, sex_slug, bd_slug, sec_slug] if p]

        key = (sex_label, bd_label, sec_label)
        indicator_map[key] = "/".join(parts)

        # Only include non-empty dims in meta
        meta = []
        if sex_label:
            meta.append({"label": sex_label, "slug": sex_slug, "category": "Sex"})
        if bd_label:
            meta.append({"label": bd_label, "slug": bd_slug, "category": breakdown_label})
        if sec_label:
            meta.append({"label": sec_label, "slug": sec_slug, "category": "Sector"})
        meta_map[key] = meta

    palika_rows = df[df["palika_code"] != ""].copy()
    if palika_rows.empty:
        logger.warning("build_eav_hierarchical: no palika-level rows found")
        return pd.DataFrame()

    palika_rows["indicator"] = palika_rows.apply(
        lambda r: indicator_map.get(
            (str(r["sex"]).strip(), str(r["breakdown"]).strip(), str(r["sector"]).strip()), ""
        ),
        axis=1,
    )

    eav_input = pd.DataFrame({
        "code"     : palika_rows["palika_code"].values,
        "country"  : COUNTRY,
        "feature"  : "ADM3",
        "indicator": palika_rows["indicator"].values,
        "value"    : palika_rows["value"].values,
    })

    loc_agg = LocationAggregator(eav_input, method="sum", start_level="ADM3")
    eav_df  = loc_agg.aggregate()

    ind_to_meta    = {v: meta_map[k] for k, v in indicator_map.items()}
    eav_df["Meta"] = eav_df["indicator"].map(ind_to_meta)

    return _finalize(eav_df)


def _finalize(df: pd.DataFrame) -> pd.DataFrame:
    """Attach timecode/timevalue and call finalize_eav_format."""
    timecode, timevalue = to_timecode_timevalue(2021)
    df["timecode"]  = timecode
    df["timevalue"] = timevalue
    return finalize_eav_format(df)