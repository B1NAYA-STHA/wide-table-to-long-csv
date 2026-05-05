from __future__ import annotations
import re
import pandas as pd
from parsers.registry import register
from builder._detect import clean, is_numeric
from ._base import FlatBase, _read

# Known Nepal place names used to identify area-as-column-header tables
_KNOWN_AREAS = frozenset({
    "nepal",
    "koshi", "madhesh", "bagmati", "gandaki", "lumbini", "karnali", "sudurpashchim",
    "province 1", "province 2", "province 3", "province 4", "province 5", "province 6", "province 7",
    # one anchor district per province
    "taplejung", "parsa", "kathmandu", "kaski", "rupandehi", "dolpa", "darchula",
})


def _looks_like_area(text: str) -> bool:
    t = re.sub(r"\s+province$", "", text.strip().lower()).strip()
    return t in _KNOWN_AREAS


@register
class TransposedAreasLayout(FlatBase):
    """
    Flat table where areas are column headers and indicators are rows.

    Structural signal:
        - At least one column header (cols 1+) matches a known Nepal place name.
        - Col-0 of first data row is text (the indicator label).
        - Cols 1+ of first data row are mostly numeric (>= 70%).

    parse() flips the table so standard FlatBase.resolve() and to_eav() work.
    """

    name = "flat_transposed_areas"

    def detect(self, rows: list, title_rows: set) -> bool:
        data_rows = [
            r for i, r in enumerate(rows)
            if i not in title_rows and any(clean(c) for c in r)
        ]
        if len(data_rows) < 2:
            return False

        header     = data_rows[0]
        first_data = data_rows[1]

        # At least one header cell must match a known Nepal place name
        header_cells = [clean(header[c]) for c in range(1, min(len(header), 10)) if clean(header[c])]
        if not any(_looks_like_area(h) for h in header_cells):
            return False

        # Col-0 of first data row must be a text indicator label
        first_cell = clean(first_data[0]) if first_data else ""
        if not first_cell or is_numeric(first_cell):
            return False

        rest = [clean(first_data[c]) for c in range(1, len(first_data)) if clean(first_data[c])]
        if not rest:
            return False
        return sum(1 for v in rest if is_numeric(v)) / len(rest) >= 0.7

    def parse(self, raw_bytes: bytes) -> pd.DataFrame:
        rows, title_rows = _read(raw_bytes)
        data_rows = [
            r for i, r in enumerate(rows)
            if i not in title_rows and any(clean(c) for c in r)
        ]
        if not data_rows:
            return pd.DataFrame()

        header     = data_rows[0]
        area_names = [
            re.sub(r"\s+province$", "", clean(header[c]), flags=re.IGNORECASE).strip()
            for c in range(1, len(header))
        ]

        records = []
        for row in data_rows[1:]:
            indicator = clean(row[0]) if row else ""
            if not indicator or is_numeric(indicator):
                continue
            for i, area in enumerate(area_names):
                if not area:
                    continue
                col = i + 1
                val = clean(row[col]) if col < len(row) else ""
                if val and is_numeric(val):
                    records.append({
                        "area_name": area,
                        "indicator": indicator,
                        "value"    : float(val.replace(",", "")),
                    })

        return pd.DataFrame(records)