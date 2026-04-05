"""Unit tests for the CSV data loader."""

from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_loader import load_csv_to_sqlite, log_error


class DataLoaderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.project_root = Path(__file__).resolve().parents[1]
        self.example_csv = self.project_root / "src" / "example_data.csv"
        self.conflict_csv = self.project_root / "src" / "conflict_data.csv"
        self.db_path = self.project_root / "tests" / "data_loader_test.db"
        self.log_path = self.project_root / "tests" / "data_loader_error_log.txt"

        if self.db_path.exists():
            self.db_path.unlink()
        if self.log_path.exists():
            self.log_path.unlink()

    def tearDown(self) -> None:
        if self.db_path.exists():
            self.db_path.unlink()
        if self.log_path.exists():
            self.log_path.unlink()

    def test_load_csv_creates_table_and_rows(self) -> None:
        inserted_rows, loaded_table_name = load_csv_to_sqlite(
            self.example_csv,
            self.db_path,
            "students",
            if_exists="replace",
        )

        self.assertEqual(inserted_rows, 4)
        self.assertEqual(loaded_table_name, "students")

        with sqlite3.connect(self.db_path) as connection:
            row_count = connection.execute("SELECT COUNT(*) FROM students").fetchone()[0]
        self.assertEqual(row_count, 4)

    def test_match_existing_schema_appends_to_existing_table(self) -> None:
        load_csv_to_sqlite(self.example_csv, self.db_path, "archive_students", if_exists="replace")

        inserted_rows, loaded_table_name = load_csv_to_sqlite(
            self.example_csv,
            self.db_path,
            "incoming_students",
            match_existing_schema=True,
        )

        self.assertEqual(inserted_rows, 4)
        self.assertEqual(loaded_table_name, "archive_students")

        with sqlite3.connect(self.db_path) as connection:
            row_count = connection.execute("SELECT COUNT(*) FROM archive_students").fetchone()[0]
        self.assertEqual(row_count, 8)

    def test_rename_conflict_creates_new_table_name(self) -> None:
        load_csv_to_sqlite(self.example_csv, self.db_path, "students", if_exists="replace")

        inserted_rows, loaded_table_name = load_csv_to_sqlite(
            self.conflict_csv,
            self.db_path,
            "students",
            if_exists="append",
            on_schema_conflict="rename",
        )

        self.assertEqual(inserted_rows, 2)
        self.assertEqual(loaded_table_name, "students_1")

        with sqlite3.connect(self.db_path) as connection:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                ).fetchall()
            }
        self.assertEqual(tables, {"students", "students_1"})

    def test_log_error_writes_message_to_log_file(self) -> None:
        log_error("example failure", self.log_path)

        contents = self.log_path.read_text(encoding="utf-8")
        self.assertIn("example failure", contents)


if __name__ == "__main__":
    unittest.main()
