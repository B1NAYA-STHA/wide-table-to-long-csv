"""
NSOCensusPipeline — downloads one NSO census resource, parses it,
resolves area codes, builds EAV, and saves output CSVs.

Output files written to output/<resource_stem>/
  original.<ext>   raw downloaded file  (when save_original=True)
  parsed.csv       long DataFrame from parser
  clean.csv        after voo area-code resolution
  eav.csv          final EAV DataFrame
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from loguru import logger

from rowllect.sources.base import BasePipeline

from parsers.base import BaseTableParser, TableSchema
from factory import get_parser
from builder.resolve import resolve_flat, resolve_grouped, resolve_hierarchical
from builder.build_eav import build_eav
from builder.catalog import CatalogResource

OUTPUT_DIR  = Path("./output")
CENSUS_YEAR = 2021

class NSOCensusPipeline(BasePipeline):
    """
    Universal pipeline for any NSO Nepal census table.

    Saves parsed.csv, clean.csv and eav.csv to output/<stem>/ automatically.
    When save_original=True the raw downloaded file is also written as
    original.<ext> (preserving the original file extension).
    """

    def __init__(
        self,
        url             : str,
        indicator_prefix: str  = "nso-census",
        breakdown_label : str  = "Category",
        census_year     : int  = CENSUS_YEAR,
        parser          : BaseTableParser | None = None,
        out_dir         : Path | str | None      = None,
        save_original   : bool = False,
    ):
        self.url              = url
        self.indicator_prefix = indicator_prefix
        self.breakdown_label  = breakdown_label
        self.census_year      = census_year
        self.save_original    = save_original
        self._parser          = parser
        self._out_dir         = Path(out_dir) if out_dir else None
        self._content         : bytes | None      = None
        self._schema          : TableSchema | None = None

    @classmethod
    def from_resource(
        cls,
        resource       : CatalogResource,
        breakdown_label: str  = "Category",
        census_year    : int  = CENSUS_YEAR,
        out_dir        : Path | str | None = None,
        save_original  : bool = False,
    ) -> "NSOCensusPipeline":
        """Convenience constructor that takes a CatalogResource directly."""
        return cls(
            url              = resource.download_url,
            indicator_prefix = resource.indicator_prefix,
            breakdown_label  = breakdown_label,
            census_year      = census_year,
            out_dir          = out_dir,
            save_original    = save_original,
        )

    # ── BasePipeline interface ─────────────────────────────────────────────

    async def pull(self) -> pd.DataFrame:
        self._content = await self.fetch_url(self.url, verify_ssl=False)
        if self._parser is None:
            self._parser = get_parser(self._content)
        self._schema = self._parser.schema(self._content)
        logger.info(
            f"[{self.name}] layout={self._schema.layout!r}  "
            f"parser={type(self._parser).__name__}  "
            f"subject={self._schema.subject!r}"
        )
        return pd.DataFrame({"_loaded": [True]})

    def process(self, _raw_df: pd.DataFrame, debug: bool = False) -> list[pd.DataFrame]:
        if self._content is None or self._schema is None or self._parser is None:
            raise RuntimeError("process() called before pull()")

        out_dir = self._out_dir or (OUTPUT_DIR / Path(self.url.split("/")[-1]).stem)
        out_dir.mkdir(parents=True, exist_ok=True)

        # Stage 0 — save the original downloaded file if requested
        if self.save_original:
            _save_original(self._content, self.url, out_dir)

        # Stage 1 — parse raw bytes → long DataFrame
        long_df = self._parser.to_long(self._content, self._schema)
        _save_csv(long_df, out_dir, "parsed.csv")
        logger.info(f"[{self.name}] parsed  : {long_df.shape}")

        # Stage 2 — resolve area names → numeric voo codes
        layout = self._schema.layout
        if layout == "hierarchical":
            clean_df = resolve_hierarchical(long_df)
        elif layout == "grouped":
            clean_df = resolve_grouped(long_df)
        else:
            clean_df = resolve_flat(long_df)

        _save_csv(clean_df, out_dir, "clean.csv")
        logger.info(f"[{self.name}] clean   : {clean_df.shape}")

        # Stage 3 — build EAV
        eav_df = build_eav(
            clean_df,
            layout           = layout,
            indicator_prefix = self.indicator_prefix,
            breakdown_label  = self.breakdown_label,
            census_year      = self.census_year,
        )
        _save_csv(eav_df, out_dir, "eav.csv")
        logger.info(
            f"[{self.name}] eav     : {eav_df.shape}  "
            f"indicators={eav_df['indicator'].nunique()}  "
            f"features={sorted(eav_df['feature'].unique())}"
        )
        return [eav_df]

# ---------------------------------------------------------------------------
# File helpers
# ---------------------------------------------------------------------------

def _save_original(content: bytes, url: str, out_dir: Path) -> None:
    """
    Write the raw downloaded bytes to out_dir/original.<ext>.

    The extension is taken from the URL filename (e.g. '.xlsx', '.csv')
    so the saved file can be opened directly in Excel or any other tool.
    """
    url_filename = url.split("/")[-1].split("?")[0]   # strip query string
    ext          = Path(url_filename).suffix or ".bin"
    dest         = out_dir / f"original{ext}"
    dest.write_bytes(content)
    logger.info(f"  → {dest}  ({len(content):,} bytes, original file)")


def _save_csv(df: pd.DataFrame, out_dir: Path, filename: str) -> None:
    path = out_dir / filename
    df.to_csv(path, index=False, encoding="utf-8-sig")
    logger.info(f"  → {path}  ({len(df):,} rows)")