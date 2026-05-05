from __future__ import annotations
import json
import re
from pathlib import Path
import requests

_API_URL      = "https://data.nsonepal.gov.np/gridtemplate/solr_resource_search"
_DOWNLOAD_URL = "https://data.nsonepal.gov.np/dataset/{package_id}/resource/{id}/download/{filename}"
_CACHE_DIR    = Path.home() / ".rowllect" / "cache"


def _indicator_prefix(filename: str) -> str:
    stem = Path(filename).stem.lower()
    stem = re.sub(r"[^\w-]", "-", stem)
    stem = re.sub(r"-{2,}", "-", stem).strip("-")
    return f"nso-census/{stem}"


class NSOFetcher:

    def get_resources(self, package_id: str) -> list[dict]:
        cache_file = _CACHE_DIR / f"{package_id}.json"
        if cache_file.exists():
            return json.loads(cache_file.read_text(encoding="utf-8"))

        resp = requests.post(_API_URL, json={"keyword": "", "package_id": package_id}, timeout=30)
        resp.raise_for_status()
        resources = resp.json().get("data", [])
        if not resources:
            raise ValueError(f"No resources found for package_id: {package_id}")

        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(json.dumps(resources, ensure_ascii=False, indent=2), encoding="utf-8")
        return resources

    def pull(self, package_ids: list[str]) -> None:
        for pid in package_ids:
            self.get_resources(pid)

    def find_resource(self, resource_id: str, package_ids: list[str]) -> tuple[dict, str]:
        for pid in package_ids:
            for r in self.get_resources(pid):
                if r["id"] == resource_id:
                    r.setdefault("indicator_prefix", _indicator_prefix(r.get("url", resource_id)))
                    r.setdefault("title_string", r.get("title_string") or r.get("name", ""))
                    return r, pid
        raise ValueError(f"Resource {resource_id!r} not found in any package")

    def get_url(self, resource_id: str, package_ids: list[str]) -> str:
        r, pid = self.find_resource(resource_id, package_ids)
        return _DOWNLOAD_URL.format(package_id=pid, id=r["id"], filename=r["url"])

    def build_urls(self, package_id: str) -> list[dict]:
        return [
            {
                "id"  : r["id"],
                "name": r.get("title_string") or r.get("name", ""),
                "url" : _DOWNLOAD_URL.format(package_id=package_id, id=r["id"], filename=r["url"]),
            }
            for r in self.get_resources(package_id)
        ]

    def _get_raw(self, url: str) -> bytes:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        return resp.content