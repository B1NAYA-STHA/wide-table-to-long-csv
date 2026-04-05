"""
rowllect/parsers/flat.py
------------------------
Parser for flat NSO tables: one data row per area, with a numeric code
column that already identifies the area level.

Example (Table 01 — households and population by province/district):

  prov_dist_code | prov_dist_name | households | total | male | female
  1              | Koshi          | 1191556    | …
  101            | Taplejung      | …

long_df columns:  area_code | area_name | indicator | value

The code column contains 1-digit province codes, 3-digit district codes, etc.
nso_census.py maps these directly to feature strings via code length:
  len 1 -> ADM1, len 3 -> ADM2, len 5 -> ADM3
No voo resolution needed for flat tables.
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


class FlatParser(BaseTableParser):

    def schema(self, raw_bytes: bytes) -> TableSchema:
        rows, title_rows = _read(raw_bytes)
        title   = extract_title(rows)
        h_start, h_end, d_start = detect_header_block(rows, title_rows)
        col_names = collapse_headers(rows, h_start, h_end)
        data_rows = [r for r in rows[d_start:] if any(clean(c) for c in r)]
        dim_cols, value_cols, _, id_cols = detect_column_roles(col_names, data_rows)

        return TableSchema(
            title       = title,
            subject     = title,
            dim_names   = [col_names[c] if c < len(col_names) else f"dim_{c}"
                           for c in dim_cols],
            value_names = [col_names[c] if c < len(col_names) else f"col_{c}"
                           for c in value_cols],
            layout      = "flat",
            extras      = {
                "h_start"   : h_start,
                "h_end"     : h_end,
                "d_start"   : d_start,
                "dim_cols"  : dim_cols,
                "value_cols": value_cols,
                "id_cols"   : id_cols,
                "col_names" : col_names,
            },
        )

    def to_long(self, raw_bytes: bytes, schema: TableSchema) -> pd.DataFrame:
        rows, _ = _read(raw_bytes)
        e = schema.extras

        dim_cols   = e["dim_cols"]
        value_cols = e["value_cols"]
        id_cols    = e["id_cols"]
        col_names  = e["col_names"]
        data_rows  = [r for r in rows[e["d_start"]:] if any(clean(c) for c in r)]

        # First id col = area code, first non-id dim col = area name
        code_col = next((c for c in dim_cols if c in id_cols), dim_cols[0])
        name_col = next((c for c in dim_cols if c not in id_cols), None)

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
                    "area_code" : last.get(code_col, ""),
                    "area_name" : last.get(name_col, "") if name_col is not None else "",
                    "indicator" : col_names[c] if c < len(col_names) else f"col_{c}",
                    "value"     : float(raw.replace(",", "")),
                })

        return pd.DataFrame(records)