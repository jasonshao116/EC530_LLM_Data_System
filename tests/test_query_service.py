"""Unit tests for query service helpers and CLI flow."""

from __future__ import annotations

import io
import sqlite3
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_loader import load_csv_to_sqlite
from query_service import format_rows, interactive_cli, list_tables
from schema_manager import SchemaManager


class QueryServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.project_root = Path(__file__).resolve().parents[1]
        self.example_csv = self.project_root / "src" / "example_data.csv"
        self.db_path = self.project_root / "tests" / "query_service_test.db"
        if self.db_path.exists():
            self.db_path.unlink()

    def tearDown(self) -> None:
        if self.db_path.exists():
            self.db_path.unlink()

    def test_format_rows_renders_table_headers_and_body(self) -> None:
        output = format_rows(
            ["id", "name"],
            [(1, "Alice"), (2, "Brian")],
        )

        self.assertIn("id | name", output)
        self.assertIn("1  | Alice", output)
        self.assertIn("2  | Brian", output)

    def test_list_tables_returns_loaded_tables(self) -> None:
        load_csv_to_sqlite(self.example_csv, self.db_path, "students", if_exists="replace")

        with sqlite3.connect(self.db_path) as connection:
            tables = list_tables(connection, SchemaManager())

        self.assertEqual(tables, ["students"])

    def test_interactive_cli_tables_command_prints_tables(self) -> None:
        load_csv_to_sqlite(self.example_csv, self.db_path, "students", if_exists="replace")
        stdout = io.StringIO()

        with patch("builtins.input", side_effect=["tables", "exit"]), patch("sys.stdout", stdout):
            interactive_cli(self.db_path)

        output = stdout.getvalue()
        self.assertIn("students", output)
        self.assertIn("Goodbye.", output)


if __name__ == "__main__":
    unittest.main()
