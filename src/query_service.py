"""Interactive CLI for loading CSV data and running validated SQL queries."""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from data_loader import load_csv_to_sqlite, log_error
from schema_manager import SchemaManager
from sql_validator import SQLValidator


def format_rows(columns: list[str], rows: list[tuple[object, ...]]) -> str:
    """Render query results as a simple aligned table."""
    if not columns:
        return "Query returned no columns."

    string_rows = [[str(value) if value is not None else "NULL" for value in row] for row in rows]
    widths = [len(column) for column in columns]
    for row in string_rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    header = " | ".join(column.ljust(widths[index]) for index, column in enumerate(columns))
    divider = "-+-".join("-" * width for width in widths)
    body = [" | ".join(value.ljust(widths[index]) for index, value in enumerate(row)) for row in string_rows]
    return "\n".join([header, divider, *body]) if body else "\n".join([header, divider, "(no rows)"])


def list_tables(connection: sqlite3.Connection, schema_manager: SchemaManager) -> list[str]:
    """Return database tables via sqlite_master."""
    return schema_manager.list_tables(connection)


def interactive_cli(db_path: str | Path) -> None:
    """Run a simple chatbot-like CLI for CSV loading and SELECT queries."""
    db_path = Path(db_path)
    schema_manager = SchemaManager()
    validator = SQLValidator(schema_manager=schema_manager)

    print(f"Connected to {db_path}")
    print("Commands: load, tables, query, help, exit")

    while True:
        command = input("> ").strip().lower()

        if command in {"exit", "quit"}:
            print("Goodbye.")
            return

        if command == "help":
            print("load   - import a CSV file into SQLite")
            print("tables - list known tables")
            print("query  - validate and execute a SELECT query")
            print("exit   - leave the CLI")
            continue

        if command == "tables":
            with sqlite3.connect(db_path) as connection:
                tables = list_tables(connection, schema_manager)
            print("No tables found." if not tables else "\n".join(tables))
            continue

        if command == "load":
            csv_path = input("CSV path: ").strip()
            table_name = input("Table name: ").strip()
            try:
                inserted_rows, loaded_table_name = load_csv_to_sqlite(
                    csv_path=csv_path,
                    db_path=db_path,
                    table_name=table_name,
                    match_existing_schema=True,
                )
                print(f"Loaded {inserted_rows} rows into {loaded_table_name}.")
            except Exception as exc:
                log_error(str(exc))
                print(f"Load failed: {exc}")
            continue

        if command == "query":
            sql = input("SQL> ").strip()
            try:
                with sqlite3.connect(db_path) as connection:
                    validation = validator.validate_query(connection, sql)
                    if not validation.is_valid:
                        print(f"Rejected: {validation.message}")
                        continue

                    columns, rows = validator.execute_query(connection, sql)
                print(format_rows(columns, rows))
            except Exception as exc:
                log_error(str(exc))
                print(f"Query failed: {exc}")
            continue

        if command:
            print("Unknown command. Type help, load, tables, query, or exit.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run an interactive query service with SQL validation."
    )
    parser.add_argument(
        "db_path",
        nargs="?",
        default="students.db",
        help="Path to the SQLite database file.",
    )
    args = parser.parse_args()
    interactive_cli(args.db_path)


if __name__ == "__main__":
    main()
