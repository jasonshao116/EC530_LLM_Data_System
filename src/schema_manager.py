"""Schema management utilities for inferring and comparing SQLite table schemas."""

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


@dataclass(frozen=True)
class SchemaComparison:
    matches: bool
    message: str


class SchemaManager:
    """Infer, inspect, compare, and create SQLite table schemas."""

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

    def table_exists(self, connection: sqlite3.Connection, table_name: str) -> bool:
        """Return whether a table exists in the SQLite database."""
        normalized_table_name = normalize_identifier(table_name)
        existing = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            (normalized_table_name,),
        ).fetchone()
        return existing is not None

    def get_existing_schema(
        self, connection: sqlite3.Connection, table_name: str
    ) -> TableSchema | None:
        """Read an existing table schema using PRAGMA table_info()."""
        normalized_table_name = normalize_identifier(table_name)
        pragma_sql = f"PRAGMA table_info({quote_identifier(normalized_table_name)})"
        rows = connection.execute(pragma_sql).fetchall()
        if not rows:
            return None

        columns = tuple(
            ColumnSchema(
                name=row[1],
                sqlite_type=(row[2] or "TEXT").upper(),
                source_name=row[1],
            )
            for row in rows
        )
        return TableSchema(table_name=normalized_table_name, columns=columns)

    def compare_schemas(
        self, expected_schema: TableSchema, existing_schema: TableSchema | None
    ) -> SchemaComparison:
        """Compare two schemas and describe whether they are compatible."""
        if existing_schema is None:
            return SchemaComparison(matches=False, message="Table does not exist yet.")

        expected_columns = [
            (column.name, column.sqlite_type.upper()) for column in expected_schema.columns
        ]
        existing_columns = [
            (column.name, column.sqlite_type.upper()) for column in existing_schema.columns
        ]

        if expected_columns == existing_columns:
            return SchemaComparison(matches=True, message="Existing schema matches incoming CSV schema.")

        return SchemaComparison(
            matches=False,
            message=(
                "Schema conflict detected. "
                f"Incoming columns: {expected_columns}. "
                f"Existing columns: {existing_columns}."
            ),
        )
