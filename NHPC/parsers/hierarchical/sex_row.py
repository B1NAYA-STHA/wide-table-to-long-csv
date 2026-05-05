from __future__ import annotations
import io
import pandas as pd
from parsers.base import BaseLayout
from parsers.registry import register
from builder._detect import clean, detect_header_block, collapse_headers, detect_column_roles, detect_layout
from ._base import HierarchicalResolve, HierarchicalEAV, detect_value_columns, walk_rows, _SEX_LABELS

_XLSX = (b"PK\x03\x04", b"PK\x05\x06")


@register
class HierSexRow(HierarchicalResolve, HierarchicalEAV, BaseLayout):
    """
    Hierarchical table where sex appears in col-3 as a data value
    (one row per sex category under each palika block).

    Structure:
        Province | District | Palika | Sex    | Sector A | Sector B
        Prov 1   |          |        | Male   |   123    |   456
                 |          |        | Female |   111    |   444

    Structural signal:
        - detect_layout() returns 'hierarchical'
        - Col-3 data values include at least one sex label
    """

    name = "hierarchical_sex_row"

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
        col3_vals = {
            clean(r[3]).lower()
            for r in data_rows[:40]
            if len(r) > 3 and clean(r[3])
        }
        return bool(col3_vals & _SEX_LABELS)

    def parse(self, raw_bytes: bytes) -> pd.DataFrame:
        info   = detect_value_columns(raw_bytes)
        raw_df = pd.read_excel(io.BytesIO(raw_bytes), header=None, engine="openpyxl")
        return walk_rows(raw_df, info)