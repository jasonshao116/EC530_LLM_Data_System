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
    is_primary_key: bool = False


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

    MANAGED_PRIMARY_KEY_NAME = "id"
    MANAGED_PRIMARY_KEY_SQL = '"id" INTEGER PRIMARY KEY AUTOINCREMENT'

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
                    is_primary_key=False,
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
        if column_defs:
            column_defs = f"{self.MANAGED_PRIMARY_KEY_SQL}, {column_defs}"
        else:
            column_defs = self.MANAGED_PRIMARY_KEY_SQL
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

    def list_tables(self, connection: sqlite3.Connection) -> list[str]:
        """Return user tables in the SQLite database."""
        rows = connection.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        ).fetchall()
        return [row[0] for row in rows]

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
                is_primary_key=bool(row[5]),
            )
            for row in rows
        )
        return TableSchema(table_name=normalized_table_name, columns=columns)

    def _comparable_columns(self, schema: TableSchema) -> list[tuple[str, str]]:
        """Return columns used for CSV-to-table schema comparison."""
        comparable_columns: list[tuple[str, str]] = []
        for column in schema.columns:
            if column.is_primary_key and column.name == self.MANAGED_PRIMARY_KEY_NAME:
                continue
            comparable_columns.append((column.name, column.sqlite_type.upper()))
        return comparable_columns

    def compare_schemas(
        self, expected_schema: TableSchema, existing_schema: TableSchema | None
    ) -> SchemaComparison:
        """Compare two schemas and describe whether they are compatible."""
        if existing_schema is None:
            return SchemaComparison(matches=False, message="Table does not exist yet.")

        expected_columns = self._comparable_columns(expected_schema)
        existing_columns = self._comparable_columns(existing_schema)

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

    def find_matching_table(
        self, connection: sqlite3.Connection, expected_schema: TableSchema
    ) -> str | None:
        """Find an existing table whose normalized schema exactly matches the incoming schema."""
        for table_name in self.list_tables(connection):
            existing_schema = self.get_existing_schema(connection, table_name)
            comparison = self.compare_schemas(expected_schema, existing_schema)
            if comparison.matches:
                return table_name
        return None

    def next_available_table_name(self, connection: sqlite3.Connection, base_name: str) -> str:
        """Return the next unused table name based on a preferred base name."""
        normalized_base_name = normalize_identifier(base_name)
        if not self.table_exists(connection, normalized_base_name):
            return normalized_base_name

        suffix = 1
        candidate = f"{normalized_base_name}_{suffix}"
        while self.table_exists(connection, candidate):
            suffix += 1
            candidate = f"{normalized_base_name}_{suffix}"
        return candidate
