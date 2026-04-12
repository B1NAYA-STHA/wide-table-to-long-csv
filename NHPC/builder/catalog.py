"""
Fetch, cache and query the NSO Nepal CKAN resource catalog.

The API returns all resources in a single POST request.

Download URL formula
--------------------
  https://data.nsonepal.gov.np/dataset/{package_id}/resource/{id}/download/{url}
"""

from __future__ import annotations

import json
import re
import ssl
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

import pandas as pd
from loguru import logger

PACKAGE_ID = "28cc1367-d99b-4911-b43c-b4f2e1c8f5f7"

_API_URL   = "https://data.nsonepal.gov.np/gridtemplate/solr_resource_search"
_DL_BASE   = "https://data.nsonepal.gov.np/dataset/{package_id}/resource/{id}/download/{url}"
_CACHE_DIR = Path.home() / ".rowllect" / "cache"

# Both directions for province lookup
_NUM_TO_PROV = {
    "1": "koshi", "2": "madhesh", "3": "bagmati",
    "4": "gandaki", "5": "lumbini", "6": "karnali", "7": "sudurpashchim",
}
_PROV_NAMES = {
    "koshi": "koshi", "kosi": "koshi",
    "madhesh": "madhesh", "madhes": "madhesh",
    "bagmati": "bagmati",
    "gandaki": "gandaki",
    "lumbini": "lumbini",
    "karnali": "karnali",
    "sudurpashchim": "sudurpashchim", "sudura paschima": "sudurpashchim",
    "sudurpaschim": "sudurpashchim", "far west": "sudurpashchim",
}


def _parse_province(description: str, filename: str) -> str | None:
    """
    Derive the lowercase province name from description and/or filename.

    Handles all observed description formats:
      "Province-1"  "Province 1"  "Province-6"
      "Koshi"  "Karnali"  "Karnali Province"
      ""  (falls through to filename)

    Falls back to filename pattern: 'indv02-koshi.xlsx' → 'koshi'.
    """
    desc = (description or "").strip()

    if desc:
        # Format: "Province-N" or "Province N" or "Province N: ..."
        m = re.search(r"\bprovince[-\s]+(\d)\b", desc, re.IGNORECASE)
        if m:
            return _NUM_TO_PROV.get(m.group(1))

        # Format: plain province name in description ("Koshi", "Karnali Province")
        desc_lower = desc.lower()
        for key, canonical in _PROV_NAMES.items():
            if key in desc_lower:
                return canonical

    # Fallback: province name embedded in filename ("indv02-koshi.xlsx")
    m = re.search(
        r"-(koshi|kosi|madhesh|bagmati|gandaki|lumbini|karnali|sudurpashchim|sudurpaschim)",
        filename, re.IGNORECASE,
    )
    if m:
        return _PROV_NAMES.get(m.group(1).lower())

    return None


@dataclass
class CatalogResource:
    """One resource entry from the NSO CKAN catalog."""

    raw: dict = field(repr=False)

    @property
    def id(self) -> str:
        return self.raw["id"]

    @property
    def package_id(self) -> str:
        return self.raw["package_id"]

    @property
    def name(self) -> str:
        return (self.raw.get("title_string") or self.raw.get("name") or "").strip()

    @property
    def description(self) -> str:
        return (self.raw.get("description") or "").strip()

    @property
    def format(self) -> str:
        return (self.raw.get("format") or "").upper()

    @property
    def filename(self) -> str:
        """Bare filename, e.g. 'indv25-gandaki.xlsx'."""
        return (self.raw.get("url") or "").strip()

    @property
    def position(self) -> int:
        return int(self.raw.get("position", 0))

    @property
    def download_url(self) -> str:
        """Full HTTPS download URL."""
        return _DL_BASE.format(
            package_id=self.package_id,
            id=self.id,
            url=self.filename,
        )

    @property
    def table_number(self) -> int | None:
        """'indv02-koshi.xlsx' → 2,  'table-34-….csv' → 34."""
        m = re.search(r"(?:indv|table[-_]?)(\d+)", self.filename, re.IGNORECASE)
        return int(m.group(1)) if m else None

    @property
    def province(self) -> str | None:
        """Lowercase province name derived from description and filename."""
        return _parse_province(self.description, self.filename)

    @property
    def indicator_prefix(self) -> str:
        """
        EAV indicator prefix built from the filename stem.

        'indv25-gandaki.xlsx' → 'nso-census/indv25-gandaki'

        The downstream EAV builder appends dimension slugs, giving e.g.:
          nso-census/indv25-gandaki/female/none/foreign-country
        """
        stem = Path(self.filename).stem.lower()
        stem = re.sub(r"[^\w-]", "-", stem)
        stem = re.sub(r"-{2,}", "-", stem).strip("-")
        return f"nso-census/{stem}"

    def __repr__(self) -> str:
        return (
            f"CatalogResource(table={self.table_number}, fmt={self.format!r}, "
            f"province={self.province!r}, name={self.name[:55]!r})"
        )


class Catalog:
    """
    Full resource catalog for one NSO CKAN package.

    cat = Catalog().fetch()
    cat.find(province='koshi')          # all Koshi resources
    cat.get('bcdc88f8-…').download_url  # URL for one resource
    cat.to_df()                         # DataFrame of all resources
    """

    def __init__(self, package_id: str = PACKAGE_ID, cache_dir: Path | None = None):
        self.package_id = package_id
        self._cache_dir = Path(cache_dir or _CACHE_DIR)
        self._resources: list[CatalogResource] = []

    @property
    def _cache_path(self) -> Path:
        return self._cache_dir / f"catalog_{self.package_id}.json"

    def fetch(self, force: bool = False) -> "Catalog":
        """Load catalog from disk cache, or POST the API if not cached."""
        if not force and self._cache_path.exists():
            logger.info(f"Catalog: loading from cache ({self._cache_path})")
            with open(self._cache_path, encoding="utf-8") as f:
                data = json.load(f)
            self._resources = [CatalogResource(raw=r) for r in data]
            logger.info(f"Catalog: {len(self._resources)} resources loaded")
            return self

        logger.info(f"Catalog: fetching from API (package={self.package_id})")
        data = self._api_post()
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        with open(self._cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        self._resources = [CatalogResource(raw=r) for r in data]
        logger.info(f"Catalog: {len(self._resources)} resources → {self._cache_path}")
        return self

    def _api_post(self) -> list[dict]:
        payload = json.dumps({"keyword": "", "package_id": self.package_id}).encode()
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode    = ssl.CERT_NONE
        req = urllib.request.Request(
            _API_URL, data=payload, method="POST",
            headers={
                "Content-Type"    : "application/json;charset=UTF-8",
                "Accept"          : "application/json, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "User-Agent"      : "Mozilla/5.0 rowllect-catalog",
            },
        )
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8")).get("data", [])

    def find(
        self,
        keyword : str        = "",
        province: str | None = None,
        fmt     : str | None = None,
    ) -> list[CatalogResource]:
        """
        Filter resources. All criteria are ANDed.

        keyword:  Case-insensitive substring on name + filename.
        province: Province name, e.g. 'koshi'.
        fmt:      'CSV' or 'XLSX'.
        """
        if not self._resources:
            raise RuntimeError("Catalog empty — call fetch() first")

        results = self._resources
        if keyword:
            kw = keyword.lower()
            results = [r for r in results
                       if kw in r.name.lower() or kw in r.filename.lower()]
        if province:
            prov = province.lower()
            results = [r for r in results if r.province and prov in r.province]
        if fmt:
            results = [r for r in results if r.format == fmt.upper()]

        return sorted(results, key=lambda r: r.position)

    def get(self, resource_id: str) -> CatalogResource:
        """Return one resource by UUID. Raises KeyError if not found."""
        for r in self._resources:
            if r.id == resource_id:
                return r
        raise KeyError(f"Resource not found: {resource_id!r}")

    def __len__(self) -> int:
        return len(self._resources)

    def __iter__(self) -> Iterator[CatalogResource]:
        return iter(self._resources)

    def to_df(self) -> pd.DataFrame:
        """All resources as a DataFrame."""
        rows = [
            {
                "id"              : r.id,
                "name"            : r.name,
                "format"          : r.format,
                "filename"        : r.filename,
                "table_number"    : r.table_number,
                "province"        : r.province,
                "description"     : r.description,
                "download_url"    : r.download_url,
                "indicator_prefix": r.indicator_prefix,
                "position"        : r.position,
            }
            for r in self._resources
        ]
        return pd.DataFrame(rows).sort_values("position").reset_index(drop=True)