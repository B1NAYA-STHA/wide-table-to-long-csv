from __future__ import annotations
import re
from pathlib import Path

import pandas as pd
from loguru import logger

from constants import PACKAGE_IDS, PACKAGE_META
from fetcher.nso_fetch import NSOFetcher
from parsers.factory import get_layout
from rowllect.warehouse.indicators import insert_indicators

DATA_DIR = Path("./data")


def _folder_name(resource: dict) -> str:
    name = resource.get("title_string") or resource.get("name") or resource["id"]
    return re.sub(r"\s+", "_", re.sub(r"[^\w\s-]", "", name.strip().lower()))


class NSOCensusPipeline:

    def __init__(self):
        self.fetcher = NSOFetcher()

    def pull(self) -> None:
        logger.info(f"Pulling metadata for {len(PACKAGE_IDS)} package(s)")
        self.fetcher.pull(PACKAGE_IDS)

    def process(self, resource_id: str, push_to_db: bool = False) -> pd.DataFrame:
        resource, package_id = self.fetcher.find_resource(resource_id, PACKAGE_IDS)
        url     = self.fetcher.get_url(resource_id, PACKAGE_IDS)
        content = self.fetcher._get_raw(url)
        logger.info(f"[{resource_id}] {len(content):,} bytes | package={package_id}")

        layout   = get_layout(content)
        long_df  = layout.parse(content)
        clean_df = layout.resolve(long_df)
        eav_df   = layout.to_eav(clean_df, indicator_prefix=resource["indicator_prefix"])
        logger.info(
            f"[{resource_id}] {layout.name} → "
            f"eav={eav_df.shape} indicators={eav_df['indicator'].nunique()}"
        )

        out = DATA_DIR / _folder_name(resource)
        out.mkdir(parents=True, exist_ok=True)
        ext = Path(url.split("/")[-1]).suffix or ".bin"
        (out / f"original{ext}").write_bytes(content)
        long_df.to_csv(out  / "parsed.csv",  index=False, encoding="utf-8-sig")
        clean_df.to_csv(out / "clean.csv",   index=False, encoding="utf-8-sig")
        eav_df.to_csv(out   / "eav.csv",     index=False, encoding="utf-8-sig")
        logger.info(f"[{resource_id}] saved → {out}/")

        if push_to_db:
            logger.info(f"Pushed: {insert_indicators(eav_df)}")

        return eav_df

    def process_package(self, package_id: str, push_to_db: bool = False) -> None:
        resources = self.fetcher.get_resources(package_id)
        if not resources:
            logger.warning(f"No resources found for package {package_id}")
            return

        category = PACKAGE_META.get(package_id, {}).get("category", "unknown")
        logger.info(f"Package {package_id} ({category}) — {len(resources)} resource(s)")

        failed = []
        for r in resources:
            try:
                self.process(r["id"], push_to_db=push_to_db)
            except Exception as e:
                logger.error(f"[{r['id']}] failed: {e}")
                failed.append(r["id"])

        logger.info(f"Package {package_id} done. {len(resources) - len(failed)}/{len(resources)} succeeded.")
        if failed:
            logger.warning(f"Failed: {failed}")

    def run(self, resource_id: str, push_to_db: bool = False) -> pd.DataFrame:
        return self.process(resource_id, push_to_db=push_to_db)