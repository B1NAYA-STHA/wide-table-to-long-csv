"""
Shared foundation for all flat-family layouts.

Flat tables have one row per observation and columns as indicators.
The three variants differ in how the "area" dimension is encoded:

    FlatLayout           — standard: area name in a dim column, resolved via attach_codes
    TransposedAreasLayout — areas are column headers, indicators are rows; flip then parse
    NationalLayout        — no area column at all; every row is stamped as Nepal (code=0)

All three share:
    _read()             — detects xlsx vs csv and returns (rows, title_rows)
    _parse_rows()       — core row-walking logic that produces long-form records
    FlatBase            — base class with resolve() and to_eav() that subclasses inherit
"""
from __future__ import annotations

import pandas as pd
from loguru import logger

from parsers.base import BaseLayout
from builder._detect import (
    clean, is_numeric, padded,
    read_csv_rows, read_xlsx_rows,
    detect_header_block, collapse_headers, detect_column_roles,
)
from builder.resolve import attach_codes, COUNTRY
from builder.build_eav import slug, build_eav_rows

_XLSX = (b"PK\x03\x04", b"PK\x05\x06")


# ── I/O helper ────────────────────────────────────────────────────────────────

def _read(raw_bytes: bytes) -> tuple[list, set]:
    return read_xlsx_rows(raw_bytes) if raw_bytes[:4] in _XLSX else read_csv_rows(raw_bytes)


# ── Core row walker ───────────────────────────────────────────────────────────

def _parse_rows(
    raw_bytes : bytes,
    name_col  : int | None = None,   # None → caller supplies fixed area_name
    fixed_area: str = "",            # used when name_col is None
) -> pd.DataFrame:
    """
    Walk a flat table and return a long-form DataFrame.

    Columns in output:
        area_name  — resolved from name_col (carry-forward) or fixed_area
        indicator  — column header text
        value      — numeric value

    name_col=None + fixed_area="Nepal" is used by NationalLayout.
    name_col is auto-detected when None is not intended — callers that want
    auto-detection pass name_col=_AUTO (see FlatBase.parse).
    """
    rows, title_rows = _read(raw_bytes)
    h_start, h_end, d_start = detect_header_block(rows, title_rows)
    col_names = collapse_headers(rows, h_start, h_end)
    data_rows = [r for r in rows[d_start:] if any(clean(c) for c in r)]
    dim_cols, value_cols, _, id_cols = detect_column_roles(col_names, data_rows)

    if name_col is _AUTO:
        name_col = next((c for c in dim_cols if c not in id_cols), None)

    width = max(value_cols) + 1
    last  = {c: "" for c in dim_cols}
    records: list[dict] = []

    for row in data_rows:
        p  = padded(row, width)
        ne = [clean(p[c]) for c in range(len(p)) if clean(p[c])]
        if len(ne) == 1 and not is_numeric(ne[0]):
            continue
        for c in dim_cols:
            if clean(p[c]):
                last[c] = clean(p[c])
        if not any(clean(p[c]) for c in value_cols):
            continue
        area = (last.get(name_col, "") if name_col is not None else fixed_area)
        for c in value_cols:
            raw = clean(p[c])
            if not raw or not is_numeric(raw):
                continue
            records.append({
                "area_name": area,
                "indicator": col_names[c] if c < len(col_names) else f"col_{c}",
                "value"    : float(raw.replace(",", "")),
            })

    return pd.DataFrame(records)


class _AUTO_T:
    """tells _parse_rows to auto-detect the name column."""
_AUTO = _AUTO_T()


# ── Shared base class ─────────────────────────────────────────────────────────

class FlatBase(BaseLayout):
    """
    Base for all flat variants.

    Subclasses must implement detect() and parse().
    resolve() and to_eav() are provided here and work for any layout whose
    parse() emits {area_name, indicator, value} rows.

    Override resolve() for NationalLayout (no lookup needed).
    Override to_eav() if you need extra dim columns in the indicator path.
    """

    name = "flat_base"

    def detect(self, rows: list, title_rows: set) -> bool:
        raise NotImplementedError

    def parse(self, raw_bytes: bytes) -> pd.DataFrame:
        raise NotImplementedError

    def resolve(self, long_df: pd.DataFrame) -> pd.DataFrame:
        df = long_df.copy()
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df.dropna(subset=["value"], inplace=True)
        return attach_codes(df, "area_name")

    def to_eav(self, clean_df: pd.DataFrame, indicator_prefix: str) -> pd.DataFrame:
        def _meta(row):
            ind = str(row["indicator"])
            return [{"label": ind, "slug": slug(ind) or "metric", "category": "Metric"}]

        return build_eav_rows(
            clean_df,
            indicator_prefix,
            dims    = ["indicator"],
            meta_fn = _meta,
        )