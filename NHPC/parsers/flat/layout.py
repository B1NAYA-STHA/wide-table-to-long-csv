from __future__ import annotations
import pandas as pd
from parsers.registry import register
from ._base import FlatBase, _parse_rows, _AUTO


@register
class FlatLayout(FlatBase):
    """
    Standard flat table: one row per area, columns are indicators.
    Fallback layout — detect() always returns True, must be registered last.
    """

    name = "flat"

    def detect(self, rows: list, title_rows: set) -> bool:
        return True

    def parse(self, raw_bytes: bytes, sheet_name: str | None = None) -> pd.DataFrame:
        return _parse_rows(raw_bytes, name_col=_AUTO, sheet_name=sheet_name)