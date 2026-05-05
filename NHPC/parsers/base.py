from __future__ import annotations
from abc import ABC, abstractmethod
import pandas as pd


class BaseLayout(ABC):
    """
    Contract every layout must satisfy.

    To add a new layout:
      1. Subclass this in the appropriate family package.
      2. Implement detect(), parse(), resolve(), to_eav().
      3. Decorate with @register.
      4. Import the module in parsers/factory.py so the decorator fires.

    Detection order = registration order. Register specific layouts before
    generic ones. FlatLayout is the fallback and must be registered last.
    """

    name: str = "unnamed"

    @abstractmethod
    def detect(self, rows: list, title_rows: set) -> bool:
        """Return True if this layout matches the raw rows."""

    @abstractmethod
    def parse(self, raw_bytes: bytes) -> pd.DataFrame:
        """Raw bytes → long-form DataFrame."""

    @abstractmethod
    def resolve(self, long_df: pd.DataFrame) -> pd.DataFrame:
        """Add code / feature / country columns. Drop unresolvable rows."""

    @abstractmethod
    def to_eav(self, clean_df: pd.DataFrame, indicator_prefix: str) -> pd.DataFrame:
        """Clean DataFrame → finalised EAV DataFrame."""