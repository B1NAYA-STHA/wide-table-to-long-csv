"""
Build warehouse EAV DataFrames from cleaned long-format DataFrames.

Layout routing
--------------
  flat         → build_eav_flat()         one indicator per value column
  grouped      → build_eav_grouped()      indicator = prefix/category/metric
  hierarchical → build_eav_hierarchical() indicator = prefix/sex/breakdown/sector
                 
"""

from __future__ import annotations

import pandas as pd
from loguru import logger
from slugify import slugify

from rowllect.aggregate.location_aggregator import LocationAggregator
from rowllect.utils.dates import to_timecode_timevalue
from rowllect.utils.eav import finalize_eav_format

from .resolve import feature_from_code

COUNTRY = "NP"

_NONE_SLUG = "none"   # slug used for empty/null dimension values


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

def build_eav(
    clean_df        : pd.DataFrame,
    layout          : str,
    indicator_prefix: str,
    breakdown_label : str,
    census_year     : int,
) -> pd.DataFrame:
    if layout == "hierarchical":
        return build_eav_hierarchical(clean_df, indicator_prefix, breakdown_label, census_year)
    if layout == "grouped":
        return build_eav_grouped(clean_df, indicator_prefix, census_year)
    return build_eav_flat(clean_df, indicator_prefix, census_year)


# ---------------------------------------------------------------------------
# Flat
# ---------------------------------------------------------------------------

def build_eav_flat(df: pd.DataFrame, prefix: str, year: int) -> pd.DataFrame:
    """
    One EAV row per (code, indicator).
    Indicator = prefix / slugified column name.
    """
    timecode, timevalue = to_timecode_timevalue(year)
    records = []
    for _, row in df.iterrows():
        ind_label = row["indicator"]
        ind_slug  = slugify(ind_label) or _NONE_SLUG
        records.append({
            "code"     : row["code"],
            "country"  : COUNTRY,
            "feature"  : row["feature"],
            "indicator": f"{prefix}/{ind_slug}" if prefix else ind_slug,
            "value"    : row["value"],
            "timecode" : timecode,
            "timevalue": timevalue,
            "Meta"     : [{"label": ind_label, "slug": ind_slug, "category": "Metric"}],
        })
    return finalize_eav_format(pd.DataFrame(records))


# ---------------------------------------------------------------------------
# Grouped
# ---------------------------------------------------------------------------

def build_eav_grouped(df: pd.DataFrame, prefix: str, year: int) -> pd.DataFrame:
    """
    Indicator = prefix / category_slug / metric_slug.
    e.g.  nso-census/indv05/total/oct-14
    """
    timecode, timevalue = to_timecode_timevalue(year)
    records = []
    for _, row in df.iterrows():
        cat_label = row.get("category", "") or ""
        ind_label = row["indicator"]
        cat_slug  = slugify(cat_label) or _NONE_SLUG
        ind_slug  = slugify(ind_label) or _NONE_SLUG
        parts     = [p for p in [prefix, cat_slug, ind_slug] if p]
        records.append({
            "code"     : row["code"],
            "country"  : COUNTRY,
            "feature"  : row["feature"],
            "indicator": "/".join(parts),
            "value"    : row["value"],
            "timecode" : timecode,
            "timevalue": timevalue,
            "Meta"     : [
                {"label": cat_label, "slug": cat_slug, "category": "Category"},
                {"label": ind_label, "slug": ind_slug, "category": "Metric"},
            ],
        })
    return finalize_eav_format(pd.DataFrame(records))


# ---------------------------------------------------------------------------
# Hierarchical
# ---------------------------------------------------------------------------

def build_eav_hierarchical(
    df              : pd.DataFrame,
    prefix          : str,
    breakdown_label : str,
    year            : int,
) -> pd.DataFrame:
    """
    Indicator = prefix / sex_slug / breakdown_slug / sector_slug.

    e.g.  nso-census/indv25-gandaki/female/none/foreign-country
          nso-census/indv54-karnali/male/literate/usually-active

    Flow:
      1. Pre-build indicator strings and Meta dicts for every unique combo.
      2. Filter to palika-level rows (palika_code != '').
      3. Feed into LocationAggregator(sum, ADM3) → rolls up to ADM2/ADM1/PCL.
      4. Attach Meta, timecode, timevalue.
    """
    timecode, timevalue = to_timecode_timevalue(year)

    combos        = df[["sex", "breakdown", "sector"]].drop_duplicates()
    indicator_map : dict[tuple, str] = {}
    meta_map      : dict[tuple, list] = {}

    for _, row in combos.iterrows():
        sex_label  = str(row["sex"]).strip()
        bd_label   = str(row["breakdown"]).strip()
        sec_label  = str(row["sector"]).strip()

        sex_slug = slugify(sex_label)  or _NONE_SLUG
        bd_slug  = slugify(bd_label)   or _NONE_SLUG
        sec_slug = slugify(sec_label)  or _NONE_SLUG

        key = (sex_label, bd_label, sec_label)
        parts = [p for p in [prefix, sex_slug, bd_slug, sec_slug] if p]
        indicator_map[key] = "/".join(parts)
        meta_map[key] = [
            {"label": sex_label, "slug": sex_slug, "category": "Sex"},
            {"label": bd_label,  "slug": bd_slug,  "category": breakdown_label},
            {"label": sec_label, "slug": sec_slug,  "category": "Sector"},
        ]

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

    ind_to_meta        = {v: meta_map[k] for k, v in indicator_map.items()}
    eav_df["Meta"]     = eav_df["indicator"].map(ind_to_meta)
    eav_df["timecode"] = timecode
    eav_df["timevalue"]= timevalue

    return finalize_eav_format(eav_df)