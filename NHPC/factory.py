"""
rowllect/parsers/factory.py
---------------------------
Auto-select the right parser for a raw file.

Detection order:
  1. Run layout detection on the rows.
  2. 'hierarchical' -> HierarchicalParser
  3. 'flat' with grouped area pattern -> GroupedParser
  4. otherwise -> FlatParser
"""

from __future__ import annotations

from parsers._detect import (
    clean, read_csv_rows, read_xlsx_rows,
    detect_header_block, collapse_headers,
    detect_column_roles, detect_layout,
)
from parsers.base import BaseTableParser
from parsers.flat import FlatParser
from parsers.grouped import GroupedParser
from parsers.hierarchical import HierarchicalParser


def get_parser(raw_bytes: bytes) -> BaseTableParser:
    """Inspect raw bytes and return the most appropriate parser instance."""
    is_xlsx = raw_bytes[:4] in (b"PK\x03\x04", b"PK\x05\x06")
    rows, title_rows = read_xlsx_rows(raw_bytes) if is_xlsx else read_csv_rows(raw_bytes)

    h_start, h_end, d_start = detect_header_block(rows, title_rows)
    col_names = collapse_headers(rows, h_start, h_end)
    data_rows = [r for r in rows[d_start:] if any(clean(c) for c in r)]

    if not data_rows:
        return FlatParser()

    try:
        dim_cols, value_cols, _, _ = detect_column_roles(col_names, data_rows)
    except ValueError:
        return FlatParser()

    layout = detect_layout(data_rows, dim_cols, value_cols)

    if layout == "hierarchical":
        return HierarchicalParser()
    if _is_grouped(data_rows, dim_cols):
        return GroupedParser()
    return FlatParser()


def _is_grouped(data_rows: list, dim_cols: list) -> bool:
    """
    True when the first dim column repeats the same area name across consecutive
    rows while the second dim column varies (Total / Male / Female pattern).
    """
    if len(dim_cols) < 2:
        return False

    area_col = dim_cols[0]
    cat_col  = dim_cols[1]
    known_cats = {"total", "male", "female", "both sexes"}

    area_vals, cat_vals = [], []
    for row in data_rows[:30]:
        a = clean(row[area_col]) if area_col < len(row) else ""
        c = clean(row[cat_col])  if cat_col  < len(row) else ""
        if a or c:
            area_vals.append(a)
            cat_vals.append(c)

    for i in range(1, len(area_vals)):
        prev_area = area_vals[i - 1]
        curr_area = area_vals[i] or prev_area
        if (curr_area == prev_area
                and cat_vals[i] != cat_vals[i - 1]
                and (cat_vals[i - 1].lower() in known_cats
                     or cat_vals[i].lower() in known_cats)):
            return True
    return False