"""
rowllect/parsers/grouped.py
---------------------------
Parser for grouped NSO tables: 2-3 data rows per area (Total / Male /
Female), where areas are identified by name only (no numeric code column).

Example (Table 5 — household heads by age of head, by province):

  Area and sex of household head | Category | Oct-14 | 15-19 | 20-29 …
  Nepal                          | Total    | 7331   | 67974 …
  Koshi                          | Total    | 724    | 8243  …
  Madhesh                        | Total    | 985    | 7245  …

long_df columns:  area_name | category | indicator | value

nso_census.py is responsible for resolving area_name to a numeric voo code.
"""

from __future__ import annotations

import pandas as pd

from .base import BaseTableParser, TableSchema
from ._detect import (
    clean, is_numeric, padded,
    read_csv_rows, read_xlsx_rows,
    detect_header_block, collapse_headers,
    detect_column_roles, extract_title,
)


def _read(raw_bytes: bytes) -> tuple[list, set]:
    if raw_bytes[:4] in (b"PK\x03\x04", b"PK\x05\x06"):
        return read_xlsx_rows(raw_bytes)
    return read_csv_rows(raw_bytes)


class GroupedParser(BaseTableParser):

    def schema(self, raw_bytes: bytes) -> TableSchema:
        rows, title_rows = _read(raw_bytes)
        title   = extract_title(rows)
        h_start, h_end, d_start = detect_header_block(rows, title_rows)
        col_names = collapse_headers(rows, h_start, h_end)
        data_rows = [r for r in rows[d_start:] if any(clean(c) for c in r)]
        dim_cols, value_cols, _, _ = detect_column_roles(col_names, data_rows)

        return TableSchema(
            title       = title,
            subject     = title,
            dim_names   = [col_names[c] if c < len(col_names) else f"dim_{c}"
                           for c in dim_cols],
            value_names = [col_names[c] if c < len(col_names) else f"col_{c}"
                           for c in value_cols],
            layout      = "grouped",
            extras      = {
                "d_start"   : d_start,
                "dim_cols"  : dim_cols,
                "value_cols": value_cols,
                "col_names" : col_names,
            },
        )

    def to_long(self, raw_bytes: bytes, schema: TableSchema) -> pd.DataFrame:
        rows, _ = _read(raw_bytes)
        e = schema.extras

        dim_cols   = e["dim_cols"]
        value_cols = e["value_cols"]
        col_names  = e["col_names"]
        data_rows  = [r for r in rows[e["d_start"]:] if any(clean(c) for c in r)]

        # First dim col = area name, second = category (Total / Male / Female)
        area_col     = dim_cols[0] if dim_cols else 0
        category_col = dim_cols[1] if len(dim_cols) > 1 else None

        width = max(value_cols) + 1
        last  = {c: "" for c in dim_cols}
        records = []

        for row in data_rows:
            p  = padded(row, width)
            ne = [clean(p[c]) for c in range(len(p)) if clean(p[c])]
            if len(ne) == 1 and not is_numeric(ne[0]):   # footer row
                continue
            for c in dim_cols:
                if clean(p[c]):
                    last[c] = clean(p[c])
            if not any(clean(p[c]) for c in value_cols):
                continue

            for c in value_cols:
                raw = clean(p[c])
                if not raw:
                    continue
                records.append({
                    "area_name": last.get(area_col, ""),
                    "category" : last.get(category_col, "") if category_col is not None else "",
                    "indicator": col_names[c] if c < len(col_names) else f"col_{c}",
                    "value"    : float(raw.replace(",", "")),
                })

        return pd.DataFrame(records)