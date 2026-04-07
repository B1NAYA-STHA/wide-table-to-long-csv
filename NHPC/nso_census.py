"""
Universal NSO census pipeline.

Handles flat, grouped and hierarchical table layouts through a common flow:
  pull()     Download file bytes; auto-detect or use injected parser.
  process()  parse → resolve area codes → build EAV DataFrame.

Every intermediate DataFrame (parsed, clean, eav) is saved as CSV 

Usage
-----
  # Auto-detect from URL4
  pipeline = NSOCensusPipeline(
      url='https://…/table01.csv',
      indicator_prefix='nso-census/households',
  )
  results = await pipeline.run(debug=True)

  # From catalog (recommended)
  from catalog import Catalog
  cat  = Catalog().fetch()
  res  = cat.find(table=2, province='koshi')[0]
  pipeline = NSOCensusPipeline(url=res.download_url,
                               indicator_prefix=res.indicator_prefix)

  # CLI
  python nso_census.py --table 2 --province koshi
  python nso_census.py --resource-id bcdc88f8-… --debug --push
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

import pandas as pd
from loguru import logger
from slugify import slugify
from voo.address import parse_address
from voo.locup import setup_lookup, get_district_code, get_province_code

from rowllect.aggregate.location_aggregator import LocationAggregator
from rowllect.sources.base import BasePipeline
from rowllect.utils.dates import to_timecode_timevalue
from rowllect.utils.eav import finalize_eav_format

from parsers.base import BaseTableParser, TableSchema
from factory import get_parser
from catalog import Catalog, CatalogResource

COUNTRY     = "NP"
CENSUS_YEAR = 2021
OUTPUT_DIR  = Path("./output")


# ---------------------------------------------------------------------------
# NSOCensusPipeline
# ---------------------------------------------------------------------------

class NSOCensusPipeline(BasePipeline):
    """
    Universal pipeline for NSO Nepal census tables.

    Accepts any URL (CSV or XLSX); auto-detects the layout and parser.
    """

    def __init__(
        self,
        url             : str,
        indicator_prefix: str  = "nso-census",
        breakdown_label : str  = "Category",
        census_year     : int  = CENSUS_YEAR,
        parser          : BaseTableParser | None = None,
        out_dir         : Path | str | None      = None,
    ):
        self.url              = url
        self.indicator_prefix = indicator_prefix
        self.breakdown_label  = breakdown_label
        self.census_year      = census_year
        self._parser          = parser
        self._out_dir         = Path(out_dir) if out_dir else None

        self._content: bytes | None      = None
        self._schema : TableSchema | None = None

    # ── BasePipeline interface ────────────────────────────────────────────────

    async def pull(self) -> pd.DataFrame:
        """Download file bytes and detect schema. Returns a sentinel DataFrame."""
        self._content = await self.fetch_url(self.url, verify_ssl=False)

        if self._parser is None:
            self._parser = get_parser(self._content)

        self._schema = self._parser.schema(self._content)
        logger.info(
            f"[{self.name}] layout={self._schema.layout!r}  "
            f"parser={type(self._parser).__name__}  "
            f"subject={self._schema.subject!r}"
        )
        logger.info(
            f"[{self.name}] dims={self._schema.dim_names}  "
            f"values={self._schema.value_names}"
        )
        return pd.DataFrame({"_loaded": [True]})

    def process(self, _raw_df: pd.DataFrame, debug=False) -> list[pd.DataFrame]:
        """Parse → resolve → EAV. Saves CSVs at each stage."""
        if self._content is None or self._schema is None:
            raise RuntimeError("process() called before pull()")

        out_dir = self._resolve_out_dir()

        # Stage 1: parse
        long_df = self._parser.to_long(self._content, self._schema)
        _save_csv(long_df, out_dir, "parsed.csv")
        logger.info(f"[{self.name}] parsed: {long_df.shape}")

        # Stage 2: resolve area codes
        layout = self._schema.layout
        if layout == "hierarchical":
            clean_df = _resolve_hierarchical(long_df)
        elif layout == "grouped":
            clean_df = _resolve_grouped(long_df)
        else:
            clean_df = _resolve_flat(long_df)

        _save_csv(clean_df, out_dir, "clean.csv")
        logger.info(f"[{self.name}] clean: {clean_df.shape}")

        # Stage 3: EAV
        eav_df = _to_eav(
            clean_df,
            layout           = layout,
            indicator_prefix = self.indicator_prefix,
            breakdown_label  = self.breakdown_label,
            census_year      = self.census_year,
        )
        _save_csv(eav_df, out_dir, "eav.csv")
        logger.info(
            f"[{self.name}] eav: {eav_df.shape}  "
            f"indicators={eav_df['indicator'].nunique()}  "
            f"features={sorted(eav_df['feature'].unique())}"
        )

        return [eav_df]

    def _resolve_out_dir(self) -> Path:
        """Derive output directory from URL stem if not explicitly set."""
        if self._out_dir:
            return self._out_dir
        stem = Path(self.url.split("/")[-1].split("?")[0]).stem or "output"
        return OUTPUT_DIR / stem


# ---------------------------------------------------------------------------
# Area resolution
# ---------------------------------------------------------------------------

def _resolve_flat(long_df: pd.DataFrame) -> pd.DataFrame:
    """
    Flat tables may carry either:
      (a) a numeric code column (e.g. Table 01: prov_dist_code = 1, 101 …)
      (b) only an area name column with no pre-existing code
          (e.g. Table 34: Province name = 'Koshi', 'Madhesh' …)

    For case (a) the code is used as-is; feature is derived from code length.
    For case (b) the area name is resolved to a numeric code via the same
    voo cascade as grouped tables: Nepal→'0'/PCL, province→ADM1, district→ADM2.
    """
    df = long_df.copy()
    df["value"]    = pd.to_numeric(df["value"], errors="coerce")
    df["code"]     = df["area_code"].astype(str).str.strip()
    df.dropna(subset=["value"], inplace=True)

    non_empty_codes = df["code"][df["code"] != ""].unique()
    all_numeric = len(non_empty_codes) > 0 and all(
        _is_numeric_code(c) for c in non_empty_codes
    )

    if not all_numeric:
        setup_lookup()
        name_to_code = _voo_resolve_area_names(non_empty_codes)
        unmatched = [n for n in non_empty_codes if n not in name_to_code]
        if unmatched:
            logger.warning(f"Unmatched area names in flat table (dropped): {unmatched}")
        df["code"] = df["code"].map(name_to_code)
        df.dropna(subset=["code"], inplace=True)
        df["code"] = df["code"].astype(str)

    df["feature"] = df["code"].apply(_feature_from_code)
    df["country"] = COUNTRY
    return df


def _is_numeric_code(s: str) -> bool:
    """True when s is a non-negative integer string (a census area code)."""
    try:
        return int(str(s).strip()) >= 0
    except (ValueError, TypeError):
        return False


def _resolve_grouped(long_df: pd.DataFrame) -> pd.DataFrame:
    """
    Geographic zones that voo cannot resolve (Urban Municipalities,
    Rural Municipalities, Mountain, Hill, Tarai) are dropped with a warning.
    """
    df = long_df.copy()
    df["value"]     = pd.to_numeric(df["value"], errors="coerce")
    df["area_name"] = df["area_name"].astype(str).str.strip()
    df.dropna(subset=["value"], inplace=True)

    setup_lookup()
    name_to_code = _voo_resolve_area_names(df["area_name"].unique())

    unmatched = [n for n in df["area_name"].unique() if n not in name_to_code]
    if unmatched:
        logger.warning(f"Unmatched area names (dropped): {unmatched}")

    df["code"] = df["area_name"].map(name_to_code)
    df.dropna(subset=["code"], inplace=True)
    df["code"]    = df["code"].astype(str)
    df["feature"] = df["code"].apply(_feature_from_code)
    df["country"] = COUNTRY
    return df


def _resolve_hierarchical(long_df: pd.DataFrame) -> pd.DataFrame:
    """
    Hierarchical tables carry district names.
    Resolve district → 3-digit code, derive province from first digit,
    generate palika codes as district_code + 2-digit suffix.
    """
    df = long_df.copy()
    for col in ["province", "district", "palika", "sex", "breakdown", "sector"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df.dropna(subset=["value"], inplace=True)

    setup_lookup()
    unique_districts  = df[df["district"] != ""]["district"].unique()
    district_to_code  = _voo_resolve_bulk(unique_districts, level="district")
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
        pmap    = {p: f"{dist_code}{str(i + 1).zfill(2)}" for i, p in enumerate(palikas)}
        df.loc[grp.index, "palika_code"] = grp["palika"].map(pmap).fillna("")

    df = df[~((df["district"] != "") & df["district_code"].isna())].copy()
    return df


def _feature_from_code(code: str) -> str:
    
    s = str(code).strip()
    if s == "0":
        return "PCL"
    return {1: "ADM1", 3: "ADM2", 5: "ADM3", 7: "ADM4"}.get(len(s), "PCL")


def _voo_resolve_area_names(names) -> dict:
    result: dict[str, str] = {}

    for name in names:
        if not name:
            continue

        # 1. National aggregate
        if name.strip().lower() == "nepal":
            result[name] = "0"
            continue

        # 2. Province
        code = get_province_code(name.lower())
        if code:
            result[name] = code
            continue

        # 3. District — try parse_address first (handles romanisation),
        #    then bare get_district_code as fallback
        code = parse_address({"address": name}, {"resolve": "district"})
        if code is None:
            code = get_district_code(name)
        if code and len(str(code)) == 3:
            result[name] = str(code)
            continue

        # 4. Palika
        code = parse_address({"address": name}, {"resolve": "palika"})
        if code and len(str(code)) == 5:
            result[name] = str(code)
            continue

    return result


def _voo_resolve_bulk(names, level: str = "district") -> dict:
    """
    Resolve district (or palika) names to numeric code strings.

    Used by _resolve_hierarchical for district-name-only lookups.
    For mixed-level name resolution (flat, grouped) use _voo_resolve_area_names.
    """
    result: dict[str, str] = {}
    for name in names:
        if not name:
            continue
        code = parse_address({"address": name}, {"resolve": level})
        if code is None:
            code = get_district_code(name)
        if code:
            result[name] = str(code)
    return result


# ---------------------------------------------------------------------------
# EAV construction
# ---------------------------------------------------------------------------

def _to_eav(clean_df, layout, indicator_prefix, breakdown_label, census_year):
    if layout == "hierarchical":
        return _eav_hierarchical(clean_df, indicator_prefix, breakdown_label, census_year)
    if layout == "grouped":
        return _eav_grouped(clean_df, indicator_prefix, census_year)
    return _eav_flat(clean_df, indicator_prefix, census_year)


def _eav_flat(df: pd.DataFrame, prefix: str, year: int) -> pd.DataFrame:
    timecode, timevalue = to_timecode_timevalue(year)
    records = []
    for _, row in df.iterrows():
        slug = slugify(row["indicator"])
        records.append({
            "code"     : row["code"],
            "country"  : COUNTRY,
            "feature"  : row["feature"],
            "indicator": f"{prefix}/{slug}" if prefix else slug,
            "value"    : row["value"],
            "timecode" : timecode,
            "timevalue": timevalue,
            "Meta"     : [{"label": row["indicator"], "slug": slug, "category": "Metric"}],
        })
    return finalize_eav_format(pd.DataFrame(records))


def _eav_grouped(df: pd.DataFrame, prefix: str, year: int) -> pd.DataFrame:
    timecode, timevalue = to_timecode_timevalue(year)
    records = []
    for _, row in df.iterrows():
        cat_slug = slugify(row.get("category", ""))
        ind_slug = slugify(row["indicator"])
        parts    = [p for p in [prefix, cat_slug, ind_slug] if p]
        records.append({
            "code"     : row["code"],
            "country"  : COUNTRY,
            "feature"  : row["feature"],
            "indicator": "/".join(parts),
            "value"    : row["value"],
            "timecode" : timecode,
            "timevalue": timevalue,
            "Meta"     : [
                {"label": row.get("category", ""), "slug": cat_slug, "category": "Category"},
                {"label": row["indicator"],         "slug": ind_slug, "category": "Metric"},
            ],
        })
    return finalize_eav_format(pd.DataFrame(records))


def _eav_hierarchical(df: pd.DataFrame, prefix: str,
                      breakdown_label: str, year: int) -> pd.DataFrame:
    timecode, timevalue = to_timecode_timevalue(year)

    combos        = df[["sex", "breakdown", "sector"]].drop_duplicates()
    indicator_map = {}
    meta_map      = {}

    for _, row in combos.iterrows():
        key = (row["sex"], row["breakdown"], row["sector"])
        parts = [p for p in [
            prefix,
            slugify(row["sex"]),
            slugify(row["breakdown"]),
            slugify(row["sector"]),
        ] if p]
        indicator_map[key] = "/".join(parts)
        meta_map[key] = [
            {"label": row["sex"],       "slug": slugify(row["sex"]),       "category": "Sex"},
            {"label": row["breakdown"], "slug": slugify(row["breakdown"]),  "category": breakdown_label},
            {"label": row["sector"],    "slug": slugify(row["sector"]),     "category": "Sector"},
        ]

    palika_rows = df[df["palika_code"] != ""].copy()
    palika_rows["indicator"] = palika_rows.apply(
        lambda r: indicator_map[(r["sex"], r["breakdown"], r["sector"])], axis=1
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


# ---------------------------------------------------------------------------
# CSV helper
# ---------------------------------------------------------------------------

def _save_csv(df: pd.DataFrame, out_dir: Path, filename: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / filename
    df.to_csv(path, index=False, encoding="utf-8-sig")
    logger.info(f"  → {path}  ({len(df):,} rows)")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _cli():
    """
    Run any NSO catalog resource through the pipeline from the command line.

    Lookup (pick one):
      --resource-id UUID      exact resource UUID from catalog
      --table N               table number, e.g. 2 for indv02
      --keyword TEXT          substring search on resource name / filename

    Optional filters (combined with --table or --keyword):
      --province NAME         e.g. koshi, madhesh, bagmati …
      --format CSV|XLSX

    Pipeline options:
      --prefix SLUG           override auto-derived indicator prefix
      --breakdown LABEL       breakdown dimension label (default: Category)
      --year N                census year (default: 2021)
      --out-dir PATH          output directory (default: ./output/<stem>/)
      --push                  push EAV results to warehouse DB
      --debug                 also pass debug=True to pipeline.run()

    Examples
    --------
      python nso_census.py --table 2 --province koshi
      python nso_census.py --resource-id bcdc88f8-… --push
      python nso_census.py --keyword "household relation" --debug
      python nso_census.py --table 16 --province koshi \\
          --prefix nso-census/occupation --breakdown Occupation
    """
    from dotenv import load_dotenv
    load_dotenv()

    p = argparse.ArgumentParser(
        description="Run any NSO CKAN census resource through the pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_cli.__doc__,
    )

    lookup = p.add_mutually_exclusive_group(required=True)
    lookup.add_argument("--resource-id", metavar="UUID",
                        help="Exact resource UUID")
    lookup.add_argument("--table", type=int, metavar="N",
                        help="Table number (e.g. 2)")
    lookup.add_argument("--keyword", metavar="TEXT",
                        help="Keyword search on name/filename")

    p.add_argument("--province",   metavar="NAME",
                   help="Province filter: koshi | madhesh | bagmati | …")
    p.add_argument("--format",     choices=["CSV", "XLSX"],
                   help="Format filter")
    p.add_argument("--prefix",     metavar="SLUG",
                   help="Override EAV indicator prefix")
    p.add_argument("--breakdown",  default="Category", metavar="LABEL",
                   help="Breakdown dimension label (default: Category)")
    p.add_argument("--year",       type=int, default=CENSUS_YEAR, metavar="N",
                   help=f"Census year (default: {CENSUS_YEAR})")
    p.add_argument("--out-dir",    metavar="PATH",
                   help="Output directory (default: ./output/<stem>/)")
    p.add_argument("--push",       action="store_true",
                   help="Push EAV results to warehouse DB")
    p.add_argument("--debug",      action="store_true",
                   help="Pass debug=True to pipeline.run()")

    args = p.parse_args()

    # Resolve resource from catalog
    cat = Catalog().fetch()

    if args.resource_id:
        resource = cat.get(args.resource_id)
    else:
        matches = cat.find(
            keyword  = args.keyword or "",
            table    = args.table,
            fmt      = args.format,
            province = args.province,
        )
        if not matches:
            p.error(
                f"No resources matched: table={args.table!r} "
                f"province={args.province!r} keyword={args.keyword!r}"
            )
        if len(matches) > 1:
            logger.warning(
                f"{len(matches)} resources matched — using first: "
                f"{matches[0].filename!r}"
            )
        resource = matches[0]

    logger.info(f"Resource : {resource.filename!r}  ({resource.format})")
    logger.info(f"URL      : {resource.download_url}")

    pipeline = NSOCensusPipeline(
        url              = resource.download_url,
        indicator_prefix = args.prefix or resource.indicator_prefix,
        breakdown_label  = args.breakdown,
        census_year      = args.year,
        out_dir          = args.out_dir,
    )

    results = asyncio.run(pipeline.run(push_to_db=args.push, debug=args.debug))
    logger.info(f"Done — {len(results)} EAV DataFrame(s) produced")


if __name__ == "__main__":
    _cli()