"""Utilities for loading CSV data into SQLite without using DataFrame.to_sql()."""

from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path
from typing import Iterable

import pandas as pd


def _normalize_identifier(name: str) -> str:
    """Convert a CSV/header name into a safe SQLite identifier."""
    normalized = re.sub(r"\W+", "_", name.strip()).strip("_")
    if not normalized:
        normalized = "column"
    if normalized[0].isdigit():
        normalized = f"col_{normalized}"
    return normalized.lower()


def _quote_identifier(name: str) -> str:
    return f'"{name.replace("\"", "\"\"")}"'


def _infer_sqlite_type(series: pd.Series) -> str:
    """Map parsed series values to a reasonable SQLite column type."""
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


def _build_column_mapping(columns: Iterable[str]) -> dict[str, str]:
    """Ensure normalized column names stay unique."""
    mapping: dict[str, str] = {}
    used: set[str] = set()

    for original in columns:
        candidate = _normalize_identifier(original)
        suffix = 1
        while candidate in used:
            suffix += 1
            candidate = f"{_normalize_identifier(original)}_{suffix}"
        mapping[original] = candidate
        used.add(candidate)

    return mapping


def load_csv_to_sqlite(
    csv_path: str | Path,
    db_path: str | Path,
    table_name: str,
    if_exists: str = "replace",
) -> int:
    """
    Load a CSV file into a SQLite table.

    pandas is used only for CSV parsing and lightweight inspection.
    Table creation and row insertion are implemented manually.
    """
    csv_path = Path(csv_path)
    db_path = Path(db_path)

    if if_exists not in {"replace", "append", "fail"}:
        raise ValueError("if_exists must be one of: replace, append, fail")

    df = pd.read_csv(csv_path)
    if df.empty and len(df.columns) == 0:
        raise ValueError(f"No tabular data found in {csv_path}")

    column_mapping = _build_column_mapping(df.columns)
    renamed_df = df.rename(columns=column_mapping)
    normalized_table_name = _normalize_identifier(table_name)

    column_defs = ", ".join(
        f'{_quote_identifier(column)} {_infer_sqlite_type(renamed_df[column])}'
        for column in renamed_df.columns
    )
    create_table_sql = (
        f"CREATE TABLE {_quote_identifier(normalized_table_name)} ({column_defs})"
    )

    placeholders = ", ".join("?" for _ in renamed_df.columns)
    quoted_columns = ", ".join(_quote_identifier(column) for column in renamed_df.columns)
    insert_sql = (
        f"INSERT INTO {_quote_identifier(normalized_table_name)} "
        f"({quoted_columns}) VALUES ({placeholders})"
    )

    records = renamed_df.where(pd.notna(renamed_df), None).itertuples(index=False, name=None)

    with sqlite3.connect(db_path) as connection:
        cursor = connection.cursor()

        if if_exists == "fail":
            existing = cursor.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
                (normalized_table_name,),
            ).fetchone()
            if existing:
                raise ValueError(f"Table '{normalized_table_name}' already exists")
        elif if_exists == "replace":
            cursor.execute(f"DROP TABLE IF EXISTS {_quote_identifier(normalized_table_name)}")

        if if_exists in {"replace", "fail"}:
            cursor.execute(create_table_sql)
        elif if_exists == "append":
            existing = cursor.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
                (normalized_table_name,),
            ).fetchone()
            if not existing:
                cursor.execute(create_table_sql)

        cursor.executemany(insert_sql, records)
        connection.commit()

    return len(renamed_df)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load a CSV file into a SQLite table without using DataFrame.to_sql()."
    )
    parser.add_argument("csv_path", help="Path to the CSV file to import.")
    parser.add_argument("db_path", help="Path to the SQLite database file.")
    parser.add_argument("table_name", help="Destination table name.")
    parser.add_argument(
        "--if-exists",
        default="replace",
        choices=["replace", "append", "fail"],
        help="Behavior when the destination table already exists.",
    )
    args = parser.parse_args()

    inserted_rows = load_csv_to_sqlite(
        csv_path=args.csv_path,
        db_path=args.db_path,
        table_name=args.table_name,
        if_exists=args.if_exists,
    )
    print(
        f"Loaded {inserted_rows} rows from {args.csv_path} into "
        f"{args.db_path}:{_normalize_identifier(args.table_name)}"
    )


if __name__ == "__main__":
    main()
