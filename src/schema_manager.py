"""Schema management utilities for inferring and creating SQLite tables from CSV data."""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd


def normalize_identifier(name: str) -> str:
    """Convert a user-provided name into a safe SQLite identifier."""
    normalized = re.sub(r"\W+", "_", name.strip()).strip("_")
    if not normalized:
        normalized = "column"
    if normalized[0].isdigit():
        normalized = f"col_{normalized}"
    return normalized.lower()


def quote_identifier(name: str) -> str:
    """Quote an identifier for SQLite."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def infer_sqlite_type(series: pd.Series) -> str:
    """Map a pandas series to a reasonable SQLite type."""
    non_null = series.dropna()
    if non_null.empty:
        return "TEXT"

    if non_null.map(lambda value: isinstance(value, bool)).all():
        return "INTEGER"
    if non_null.map(lambda value: isinstance(value, int) and not isinstance(value, bool)).all():
        return "INTEGER"
    if non_null.map(lambda value: isinstance(value, (int, float)) and not isinstance(value, bool)).all():
        return "REAL"
    return "TEXT"


def build_column_mapping(columns: Iterable[str]) -> dict[str, str]:
    """Normalize column names while preserving uniqueness."""
    mapping: dict[str, str] = {}
    used: set[str] = set()

    for original in columns:
        base_name = normalize_identifier(original)
        candidate = base_name
        suffix = 1

        while candidate in used:
            suffix += 1
            candidate = f"{base_name}_{suffix}"

        mapping[original] = candidate
        used.add(candidate)

    return mapping


@dataclass(frozen=True)
class ColumnSchema:
    name: str
    sqlite_type: str
    source_name: str


@dataclass(frozen=True)
class TableSchema:
    table_name: str
    columns: tuple[ColumnSchema, ...]


class SchemaManager:
    """Infer schemas from CSV data and create matching SQLite tables."""

    def infer_schema_from_dataframe(self, df: pd.DataFrame, table_name: str) -> tuple[TableSchema, pd.DataFrame]:
        """Return a normalized schema and a DataFrame with normalized column names."""
        if df.empty and len(df.columns) == 0:
            raise ValueError("No tabular data found in the provided DataFrame")

        column_mapping = build_column_mapping(df.columns)
        normalized_df = df.rename(columns=column_mapping)
        schema = TableSchema(
            table_name=normalize_identifier(table_name),
            columns=tuple(
                ColumnSchema(
                    name=column_name,
                    sqlite_type=infer_sqlite_type(normalized_df[column_name]),
                    source_name=source_name,
                )
                for source_name, column_name in column_mapping.items()
            ),
        )
        return schema, normalized_df

    def infer_schema_from_csv(self, csv_path: str | Path, table_name: str) -> tuple[TableSchema, pd.DataFrame]:
        """Read a CSV file and infer its SQLite schema."""
        csv_path = Path(csv_path)
        df = pd.read_csv(csv_path)
        return self.infer_schema_from_dataframe(df, table_name)

    def build_create_table_sql(self, schema: TableSchema) -> str:
        """Generate a CREATE TABLE statement for a normalized schema."""
        column_defs = ", ".join(
            f"{quote_identifier(column.name)} {column.sqlite_type}"
            for column in schema.columns
        )
        return f"CREATE TABLE {quote_identifier(schema.table_name)} ({column_defs})"

    def create_table(self, connection: sqlite3.Connection, schema: TableSchema) -> None:
        """Create a SQLite table matching the provided schema."""
        connection.execute(self.build_create_table_sql(schema))

