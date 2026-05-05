from __future__ import annotations
import pandas as pd
from parsers.registry import register
from builder._detect import clean, is_numeric
from builder.resolve import COUNTRY
from builder.build_eav import slug, build_eav_rows
from ._base import FlatBase, _read

# Any of these words in col-0 or header cells means the table has an area dimension
_PLACE_WORDS = frozenset({
    "area", "province", "district", "palika", "municipality", "ward", "vdc",
    "zone", "region", "bagmati", "gandaki", "koshi", "lumbini",
    "madhesh", "karnali", "sudurpashchim", "nepal", "national",
})


def _has_place_word(text: str) -> bool:
    return any(w in text.lower().split() for w in _PLACE_WORDS)


@register
class NationalTransposedLayout(FlatBase):
    """
    Table with no area dimension. Col-0 is a category axis (age group,
    education level, crop type, etc.) and column headers are metric labels.
    Every record is stamped as Nepal (code=0, feature=PCL).

    Structural signal:
        - Column headers (cols 1+) contain no place-name words.
        - Col-0 data values are text and contain no place-name words.
        - First data row: cols 1+ are mostly numeric (>= 70%).

    Registered before FlatLayout and TransposedAreasLayout.
    """

    name = "flat_national"

    def detect(self, rows: list, title_rows: set) -> bool:
        data_rows = [
            r for i, r in enumerate(rows)
            if i not in title_rows and any(clean(c) for c in r)
        ]
        if len(data_rows) < 2:
            return False

        header     = data_rows[0]
        first_data = data_rows[1]

        # Header cols 1+ must have no place-name words
        header_cells = [clean(header[c]) for c in range(1, len(header)) if clean(header[c])]
        if not header_cells or any(_has_place_word(h) for h in header_cells):
            return False

        # Col-0 data must be text and contain no place-name words
        col0_vals = [
            clean(r[0]) for r in data_rows[1:]
            if r and clean(r[0]) and not is_numeric(clean(r[0]))
        ]
        if not col0_vals or any(_has_place_word(v) for v in col0_vals):
            return False

        # First data row: cols 1+ mostly numeric
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

        header         = data_rows[0]
        col_categories = [clean(header[c]) for c in range(1, len(header))]

        records = []
        for row in data_rows[1:]:
            row_label = clean(row[0]) if row else ""
            if not row_label or is_numeric(row_label):
                continue
            for i, cat in enumerate(col_categories):
                if not cat:
                    continue
                col = i + 1
                val = clean(row[col]) if col < len(row) else ""
                if val and is_numeric(val):
                    records.append({
                        "area_name": "Nepal",
                        "category" : cat,
                        "indicator": row_label,
                        "value"    : float(val.replace(",", "")),
                    })

        return pd.DataFrame(records)

    def resolve(self, long_df: pd.DataFrame) -> pd.DataFrame:
        df = long_df.copy()
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df.dropna(subset=["value"], inplace=True)
        df["code"]    = "0"
        df["feature"] = "PCL"
        df["country"] = COUNTRY
        return df

    def to_eav(self, clean_df: pd.DataFrame, indicator_prefix: str) -> pd.DataFrame:
        def _meta(row):
            cat = str(row.get("category", ""))
            ind = str(row["indicator"])
            meta = []
            if cat and cat.lower() not in ("", "nan", "none"):
                meta.append({"label": cat, "slug": slug(cat), "category": "Category"})
            meta.append({"label": ind, "slug": slug(ind) or "metric", "category": "Metric"})
            return meta

        return build_eav_rows(
            clean_df,
            indicator_prefix,
            dims    = ["category", "indicator"],
            meta_fn = _meta,
        )