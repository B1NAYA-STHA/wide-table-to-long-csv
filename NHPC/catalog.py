"""
Fetch, cache and query the NSO Nepal CKAN resource catalog.

The API returns all resources in a single POST request.

Download URL formula
--------------------
  https://data.nsonepal.gov.np/dataset/{package_id}/resource/{id}/download/{url}

Usage
-----
  from catalog import Catalog

  cat = Catalog().fetch()           # loads from cache if available
  cat.find(table=2, province='koshi')   # → [CatalogResource, …]
  cat.get('bcdc88f8-…').download_url    # full URL for NSOCensusPipeline
  cat.to_df()                           # all resources as a DataFrame
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

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PACKAGE_ID = "28cc1367-d99b-4911-b43c-b4f2e1c8f5f7"

_API_URL  = "https://data.nsonepal.gov.np/gridtemplate/solr_resource_search"
_DL_BASE  = "https://data.nsonepal.gov.np/dataset/{package_id}/resource/{id}/download/{url}"
_CACHE_DIR = Path.home() / ".rowllect" / "cache"

_PROVINCE_MAP = {
    "1": "koshi", "2": "madhesh", "3": "bagmati",
    "4": "gandaki", "5": "lumbini", "6": "karnali", "7": "sudurpashchim",
}


# ---------------------------------------------------------------------------
# CatalogResource
# ---------------------------------------------------------------------------

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
        """Bare filename only, e.g. 'indv02-koshi.xlsx'."""
        return (self.raw.get("url") or "").strip()

    @property
    def position(self) -> int:
        return int(self.raw.get("position", 0))

    @property
    def download_url(self) -> str:
        """Full HTTPS download URL constructed from package_id + id + filename."""
        return _DL_BASE.format(
            package_id=self.package_id,
            id=self.id,
            url=self.filename,
        )

    @property
    def table_number(self) -> int | None:
        """Parse table number from filename: 'indv02-koshi.xlsx' → 2, 'table-34-…csv' → 34."""
        m = re.search(r"(?:indv|table[-_]?)(\d+)", self.filename, re.IGNORECASE)
        return int(m.group(1)) if m else None

    @property
    def province(self) -> str | None:
        """
        Derive province name from description ('Province-1') or filename ('indv02-koshi').
        Returns lowercase province name or None.
        """
        m = re.search(r"province[-\s]?(\d)", self.description, re.IGNORECASE)
        if m:
            return _PROVINCE_MAP.get(m.group(1))
        m = re.search(
            r"-(koshi|madhesh|bagmati|gandaki|lumbini|karnali|sudurpashchim)",
            self.filename, re.IGNORECASE,
        )
        return m.group(1).lower() if m else None

    @property
    def indicator_prefix(self) -> str:
        """Auto-derived EAV prefix: 'indv02-koshi.xlsx' → 'nso-census/indv02-koshi'."""
        stem = Path(self.filename).stem.lower()
        stem = re.sub(r"[^\w-]", "-", stem)
        stem = re.sub(r"-{2,}", "-", stem).strip("-")
        return f"nso-census/{stem}"

    def __repr__(self) -> str:
        return (
            f"CatalogResource(table={self.table_number}, fmt={self.format!r}, "
            f"province={self.province!r}, name={self.name[:55]!r})"
        )


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

class Catalog:
    """
    Full resource catalog for one NSO CKAN package.
    The response is cached to ~/.rowllect/cache/catalog_<package_id>.json.

    Methods
    -------
    fetch(force)   Load catalog (from cache or API).
    find(...)      Filter by table, province, format, or keyword.
    get(id)        Look up one resource by UUID.
    to_df()        All resources as a pandas DataFrame.
    """

    def __init__(self, package_id: str = PACKAGE_ID, cache_dir: Path | None = None):
        self.package_id  = package_id
        self._cache_dir  = Path(cache_dir or _CACHE_DIR)
        self._resources: list[CatalogResource] = []

    @property
    def _cache_path(self) -> Path:
        return self._cache_dir / f"catalog_{self.package_id}.json"

    # ── fetch ─────────────────────────────────────────────────────────────────

    def fetch(self, force: bool = False) -> "Catalog":
        """
        Load the catalog. Reads from disk cache if available, otherwise POSTs
        the API once to get all resources.

        Args:
            force: Re-download even if cached.

        """
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
        logger.info(f"Catalog: {len(self._resources)} resources cached → {self._cache_path}")
        return self

    def _api_post(self) -> list[dict]:
        """POST the CKAN search API once; return the flat list of resource dicts."""
        payload = json.dumps({
            "keyword"   : "",
            "package_id": self.package_id,
        }).encode("utf-8")

        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode    = ssl.CERT_NONE

        req = urllib.request.Request(
            _API_URL,
            data    = payload,
            method  = "POST",
            headers = {
                "Content-Type"    : "application/json;charset=UTF-8",
                "Accept"          : "application/json, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "User-Agent"      : "Mozilla/5.0 rowllect-catalog",
            },
        )
        with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
            body = json.loads(resp.read().decode("utf-8"))

        return body.get("data", [])

    # ── query ─────────────────────────────────────────────────────────────────

    def find(
        self,
        keyword : str       = "",
        table   : int | None = None,
        fmt     : str | None = None,
        province: str | None = None,
    ) -> list[CatalogResource]:
        """
        Filter resources. All supplied criteria are ANDed.

        Args:
            keyword:  Substring match on name + filename (case-insensitive).
            table:    Table number, e.g. 2 matches 'indv02-koshi.xlsx'.
            fmt:      'CSV' or 'XLSX'.
            province: Province name, e.g. 'koshi'.

        Returns:
            List of matching CatalogResource objects sorted by position.
        """
        if not self._resources:
            raise RuntimeError("Catalog is empty — call fetch() first")

        results = self._resources

        if keyword:
            kw = keyword.lower()
            results = [r for r in results
                       if kw in r.name.lower() or kw in r.filename.lower()]
        if table is not None:
            results = [r for r in results if r.table_number == table]
        if fmt:
            results = [r for r in results if r.format == fmt.upper()]
        if province:
            prov = province.lower()
            results = [r for r in results if r.province and prov in r.province]

        return sorted(results, key=lambda r: r.position)

    def get(self, resource_id: str) -> CatalogResource:
        """Return one resource by its UUID. Raises KeyError if not found."""
        for r in self._resources:
            if r.id == resource_id:
                return r
        raise KeyError(f"Resource not found: {resource_id!r}")

    def __len__(self) -> int:
        return len(self._resources)

    def __iter__(self) -> Iterator[CatalogResource]:
        return iter(self._resources)

    # ── export ────────────────────────────────────────────────────────────────

    def to_df(self) -> pd.DataFrame:
        """All resources as a DataFrame with derived columns."""
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