"""Utilities for loading CSV data into SQLite without using DataFrame.to_sql()."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import pandas as pd

from schema_manager import SchemaManager, normalize_identifier, quote_identifier


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

    schema_manager = SchemaManager()
    schema, renamed_df = schema_manager.infer_schema_from_dataframe(df, table_name)
    normalized_table_name = schema.table_name

    placeholders = ", ".join("?" for _ in renamed_df.columns)
    quoted_columns = ", ".join(quote_identifier(column) for column in renamed_df.columns)
    insert_sql = (
        f"INSERT INTO {quote_identifier(normalized_table_name)} "
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
            cursor.execute(f"DROP TABLE IF EXISTS {quote_identifier(normalized_table_name)}")

        if if_exists in {"replace", "fail"}:
            schema_manager.create_table(connection, schema)
        elif if_exists == "append":
            existing = cursor.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
                (normalized_table_name,),
            ).fetchone()
            if not existing:
                schema_manager.create_table(connection, schema)

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
        f"{args.db_path}:{normalize_identifier(args.table_name)}"
    )


if __name__ == "__main__":
    main()
