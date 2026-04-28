from __future__ import annotations
from abc import ABC, abstractmethod


class BaseFetcher(ABC):
    """
    Fetches raw file bytes for a list of package IDs from a data portal.
    Implement _fetch_package(package_id) for each portal.
    """

    @abstractmethod
    def _build_url(self, package_id: str) -> str:
        """Return the download URL for a given package ID."""

    @abstractmethod
    def _get_raw(self, url: str) -> bytes:
        """Fetch raw bytes from a URL."""

    def pull(self, package_ids: list[str]) -> dict[str, bytes]:
        """
        Pull raw bytes for each package_id.
        Returns {package_id: bytes}.
        """
        return {pid: self._get_raw(self._build_url(pid)) for pid in package_ids}