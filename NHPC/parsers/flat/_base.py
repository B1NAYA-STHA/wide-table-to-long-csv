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


def _read(raw_bytes: bytes, sheet_name: str | None = None) -> tuple[list, set]:
    return read_xlsx_rows(raw_bytes, sheet_name=sheet_name) if raw_bytes[:4] in _XLSX else read_csv_rows(raw_bytes)


def _parse_rows(
    raw_bytes  : bytes,
    name_col   : int | None = None,
    fixed_area : str = "",
    sheet_name : str | None = None,
) -> pd.DataFrame:
    """
    Walk a flat table and return a long-form DataFrame with columns:
    area_name, indicator, value.

    Pass name_col=_AUTO for auto-detection, or name_col=None + fixed_area
    for tables with no area column (e.g. national-only tables).
    """
    rows, title_rows = _read(raw_bytes, sheet_name=sheet_name)
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
        area = last.get(name_col, "") if name_col is not None else fixed_area
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
    """Sentinel — tells _parse_rows to auto-detect the name column."""
_AUTO = _AUTO_T()


class FlatBase(BaseLayout):
    """
    Base for all flat layout variants.

    Subclasses implement detect() and parse(). resolve() and to_eav() work
    for any layout whose parse() emits {area_name, indicator, value} rows.
    Override as needed (e.g. NationalTransposedLayout overrides both).
    """

    name = "flat_base"

    def detect(self, rows: list, title_rows: set) -> bool:
        raise NotImplementedError

    def parse(self, raw_bytes: bytes, sheet_name: str | None = None) -> pd.DataFrame:
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