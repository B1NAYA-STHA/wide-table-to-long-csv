from __future__ import annotations
from pathlib import Path
import re

import pandas as pd
from loguru import logger

from constants import PACKAGE_IDS
from fetcher.nso_fetch import NSOFetcher
from parsers.factory import get_parser
from builder.resolve import resolve_flat, resolve_grouped, resolve_hierarchical
from builder.build_eav import build_eav
from rowllect.warehouse.indicators import insert_indicators

DATA_DIR = Path("./data")


def _folder_name(resource: dict) -> str:
    name = resource.get("title_string") or resource.get("name") or resource["id"]
    name = name.strip().lower()
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"[\s]+", "_", name)
    return name


class NSOCensusPipeline:

    def __init__(self):
        self.fetcher = NSOFetcher()

    def pull(self) -> None:
        """Fetch and cache resource metadata for all packages in constants.py."""
        logger.info(f"Pulling metadata for {len(PACKAGE_IDS)} package(s)")
        self.fetcher.pull(PACKAGE_IDS)

    def process(self, resource_id: str, push_to_db: bool = False) -> pd.DataFrame:
        """Download and process a single table by resource_id."""
        resource, package_id = self.fetcher.find_resource(resource_id, PACKAGE_IDS)
        url     = self.fetcher.get_url(resource_id, PACKAGE_IDS)
        content = self.fetcher._get_raw(url)
        logger.info(f"[{resource_id}] downloaded {len(content):,} bytes")

        parser   = get_parser(content)
        schema   = parser.schema(content)
        long_df  = parser.to_long(content, schema)
        logger.info(f"[{resource_id}] layout={schema.layout!r}  parsed={long_df.shape}")

        if schema.layout == "hierarchical":
            clean_df = resolve_hierarchical(long_df)
        elif schema.layout == "grouped":
            clean_df = resolve_grouped(long_df)
        else:
            clean_df = resolve_flat(long_df)

        eav_df = build_eav(
            clean_df,
            layout           = schema.layout,
            indicator_prefix = f"nso-census/{resource_id[:8]}",
            breakdown_label  = "Category",
        )

        # Always save all stages under data/<table_name>/
        out = DATA_DIR / _folder_name(resource)
        out.mkdir(parents=True, exist_ok=True)
        ext = Path(url.split("/")[-1]).suffix or ".bin"
        (out / f"original{ext}").write_bytes(content)
        long_df.to_csv(out / "parsed.csv",  index=False, encoding="utf-8-sig")
        clean_df.to_csv(out / "clean.csv",  index=False, encoding="utf-8-sig")
        eav_df.to_csv(out / "eav.csv",      index=False, encoding="utf-8-sig")
        logger.info(f"[{resource_id}] saved → {out}/")

        logger.info(f"[{resource_id}] eav={eav_df.shape}  indicators={eav_df['indicator'].nunique()}")

        if push_to_db:
            result = insert_indicators(eav_df)
            logger.info(f"Pushed: {result}")

        return eav_df

    def run(self, resource_id: str, push_to_db: bool = False) -> pd.DataFrame:
        return self.process(resource_id, push_to_db=push_to_db)