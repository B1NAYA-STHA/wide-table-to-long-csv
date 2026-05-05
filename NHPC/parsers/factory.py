from __future__ import annotations
from builder._detect import read_xlsx_rows, read_csv_rows
from .base import BaseLayout

# Import order = detection priority. Most specific first, flat fallback last.
import parsers.hierarchical.sex_paired  # noqa: F401
import parsers.hierarchical.sex_row     # noqa: F401
import parsers.hierarchical.no_sex      # noqa: F401
import parsers.flat.national            # noqa: F401
import parsers.flat.transposed          # noqa: F401
import parsers.flat.layout              # noqa: F401

from .registry import get_registry

_XLSX_MAGIC = (b"PK\x03\x04", b"PK\x05\x06")


def get_layout(raw_bytes: bytes) -> BaseLayout:
    is_xlsx          = raw_bytes[:4] in _XLSX_MAGIC
    rows, title_rows = read_xlsx_rows(raw_bytes) if is_xlsx else read_csv_rows(raw_bytes)
    for layout in get_registry():
        if layout.detect(rows, title_rows):
            return layout
    from parsers.flat.layout import FlatLayout
    return FlatLayout()