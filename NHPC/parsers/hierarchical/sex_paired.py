from __future__ import annotations
import io
import pandas as pd
from parsers.base import BaseLayout
from parsers.registry import register
from builder._detect import (
    clean, detect_header_block, collapse_headers,
    detect_column_roles, detect_layout, read_xlsx_rows, read_csv_rows,
)
from ._base import HierarchicalResolve, HierarchicalEAV, detect_value_columns, walk_rows, _SEX_LABELS

_XLSX = (b"PK\x03\x04", b"PK\x05\x06")


@register
class HierSexPaired(HierarchicalResolve, HierarchicalEAV, BaseLayout):
    """
    Hierarchical table where Male / Female / Total appear as paired column
    headers grouped under sector labels.

    Structure:
        Province | District | Palika | (gap) | Sector A        | Sector B
                                              | Male | Female   | Male | Female

    Structural signal:
        - detect_layout() returns 'hierarchical'
        - Bottom header row labels are a subset of sex labels
        - detect_value_columns() confirms sub_layout == 'sex_paired'
    """

    name = "hierarchical_sex_paired"

    def detect(self, rows: list, title_rows: set) -> bool:
        h_start, h_end, d_start = detect_header_block(rows, title_rows)
        col_names = collapse_headers(rows, h_start, h_end)
        data_rows = [r for r in rows[d_start:] if any(clean(c) for c in r)]
        if not data_rows:
            return False
        try:
            dim_cols, value_cols, _, _ = detect_column_roles(col_names, data_rows)
        except ValueError:
            return False
        if detect_layout(data_rows, dim_cols, value_cols) != "hierarchical":
            return False
        # Confirm sex labels appear as column headers (not in data rows)
        # by checking the bottom header row in the collapsed names
        if h_end > h_start:
            bot_header_row = rows[h_end]
            bot_vals = {clean(bot_header_row[c]).lower() for c in value_cols if c < len(bot_header_row) and clean(bot_header_row[c])}
            return bool(bot_vals) and bot_vals.issubset(_SEX_LABELS)
        return False

    def parse(self, raw_bytes: bytes) -> pd.DataFrame:
        info   = detect_value_columns(raw_bytes)
        raw_df = pd.read_excel(io.BytesIO(raw_bytes), header=None, engine="openpyxl")
        return walk_rows(raw_df, info)