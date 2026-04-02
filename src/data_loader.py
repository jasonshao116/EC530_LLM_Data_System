"""Utilities for loading CSV data into SQLite without using DataFrame.to_sql()."""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

from schema_manager import SchemaManager, normalize_identifier, quote_identifier


def log_error(message: str, log_path: str | Path = "error_log.txt") -> None:
    """Append a timestamped error message to a log file."""
    timestamp = datetime.now().isoformat(timespec="seconds")
    log_path = Path(log_path)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"[{timestamp}] {message}\n")


def prompt_schema_conflict(table_name: str, details: str) -> str:
    """Prompt the user to choose how to handle a schema conflict."""
    print(f"Schema conflict for table '{table_name}'.")
    print(details)
    print("Choose an action: overwrite, rename, or skip")

    while True:
        choice = input("Enter choice [overwrite/rename/skip]: ").strip().lower()
        if choice in {"overwrite", "rename", "skip"}:
            return choice
        print("Invalid choice. Please enter overwrite, rename, or skip.")


def load_csv_to_sqlite(
    csv_path: str | Path,
    db_path: str | Path,
    table_name: str,
    if_exists: str = "replace",
    on_schema_conflict: str = "prompt",
) -> tuple[int, str]:
    """
    Load a CSV file into a SQLite table.

    pandas is used only for CSV parsing and lightweight inspection.
    Table creation and row insertion are implemented manually.
    """
    csv_path = Path(csv_path)
    db_path = Path(db_path)

    if if_exists not in {"replace", "append", "fail"}:
        raise ValueError("if_exists must be one of: replace, append, fail")
    if on_schema_conflict not in {"prompt", "overwrite", "rename", "skip"}:
        raise ValueError("on_schema_conflict must be one of: prompt, overwrite, rename, skip")

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
        existing_schema = schema_manager.get_existing_schema(connection, normalized_table_name)
        comparison = schema_manager.compare_schemas(schema, existing_schema)
        table_exists = existing_schema is not None

        if table_exists and not comparison.matches:
            action = on_schema_conflict
            if action == "prompt":
                action = prompt_schema_conflict(normalized_table_name, comparison.message)

            if action == "skip":
                print(f"Skipped loading data into {normalized_table_name}.")
                return 0, normalized_table_name

            if action == "rename":
                suffix = 1
                renamed_table_name = f"{normalized_table_name}_{suffix}"
                while schema_manager.table_exists(connection, renamed_table_name):
                    suffix += 1
                    renamed_table_name = f"{normalized_table_name}_{suffix}"
                schema = schema.__class__(table_name=renamed_table_name, columns=schema.columns)
                normalized_table_name = renamed_table_name
                insert_sql = (
                    f"INSERT INTO {quote_identifier(normalized_table_name)} "
                    f"({quoted_columns}) VALUES ({placeholders})"
                )
                existing_schema = None
                table_exists = False
            elif action == "overwrite":
                cursor.execute(f"DROP TABLE IF EXISTS {quote_identifier(normalized_table_name)}")
                existing_schema = None
                table_exists = False

        if if_exists == "fail":
            if table_exists:
                raise ValueError(f"Table '{normalized_table_name}' already exists")
        elif if_exists == "replace":
            cursor.execute(f"DROP TABLE IF EXISTS {quote_identifier(normalized_table_name)}")
            existing_schema = None
            table_exists = False

        if if_exists in {"replace", "fail"}:
            schema_manager.create_table(connection, schema)
        elif if_exists == "append":
            if not table_exists:
                schema_manager.create_table(connection, schema)

        cursor.executemany(insert_sql, records)
        connection.commit()

    return len(renamed_df), normalized_table_name


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
    parser.add_argument(
        "--on-schema-conflict",
        default="prompt",
        choices=["prompt", "overwrite", "rename", "skip"],
        help="Behavior when an existing table has a different schema.",
    )
    args = parser.parse_args()
    try:
        inserted_rows, loaded_table_name = load_csv_to_sqlite(
            csv_path=args.csv_path,
            db_path=args.db_path,
            table_name=args.table_name,
            if_exists=args.if_exists,
            on_schema_conflict=args.on_schema_conflict,
        )
        print(
            f"Loaded {inserted_rows} rows from {args.csv_path} into "
            f"{args.db_path}:{loaded_table_name}"
        )
    except Exception as exc:
        log_error(str(exc))
        raise


if __name__ == "__main__":
    main()
