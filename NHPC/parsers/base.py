"""
rowllect/parsers/base.py
------------------------
Abstract base class and schema dataclass for all NSO table parsers.

A parser does two things only:
  schema(raw_bytes)         -> TableSchema   (detect structure)
  to_long(raw_bytes, schema) -> pd.DataFrame (convert to tidy long format)

The pipeline layer (nso_census.py) handles everything after that:
voo code resolution, LocationAggregator roll-up, EAV formatting.

Layouts
-------
  flat         One row per area; a numeric code column already identifies
               the area level (province 1-digit, district 3-digit, etc.).
               long_df columns: area_code | area_name | indicator | value

  grouped      2-3 rows per area (Total / Male / Female); area identified
               by name only — must be resolved to a code via voo.
               long_df columns: area_name | category | indicator | value

  hierarchical XLSX with Province / District / Palika / Sex on their own
               dedicated rows; values appear on a separate breakdown row.
               long_df columns: province | district | palika | sex
                                | breakdown | sector | value
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import pandas as pd


@dataclass
class TableSchema:
    """Describes the structure of a parsed NSO table."""
    title      : str            # raw title string from the file (may be '')
    subject    : str            # title with 'Table N:' and 'NPHC YYYY' stripped
    dim_names  : list[str]      # dimension column names in the long DataFrame
    value_names: list[str]      # value/sector column names
    layout     : str            # 'flat' | 'grouped' | 'hierarchical'
    extras     : dict = field(default_factory=dict)  # parser-specific metadata


class BaseTableParser(ABC):

    @abstractmethod
    def schema(self, raw_bytes: bytes) -> TableSchema:
        """Read raw bytes and return the detected table schema."""

    @abstractmethod
    def to_long(self, raw_bytes: bytes, schema: TableSchema) -> pd.DataFrame:
        """Convert raw bytes to a flat long DataFrame."""

    def parse(self, raw_bytes: bytes) -> tuple[TableSchema, pd.DataFrame]:
        """Convenience: return (schema, long_df) in one call."""
        s = self.schema(raw_bytes)
        return s, self.to_long(raw_bytes, s)