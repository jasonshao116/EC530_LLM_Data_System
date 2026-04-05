"""Unit tests for SQL validation behavior."""

from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_loader import load_csv_to_sqlite
from sql_validator import SQLValidator


class SQLValidatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = sqlite3.connect(":memory:")
        temp_csv = Path(__file__).resolve().parents[1] / "src" / "example_data.csv"
        self.db_path = Path(__file__).resolve().parents[1] / "tests" / "validator_test.db"
        if self.db_path.exists():
            self.db_path.unlink()
        load_csv_to_sqlite(temp_csv, self.db_path, "students", if_exists="replace")
        self.file_connection = sqlite3.connect(self.db_path)
        self.validator = SQLValidator()

    def tearDown(self) -> None:
        self.connection.close()
        self.file_connection.close()
        if self.db_path.exists():
            self.db_path.unlink()

    def test_accepts_valid_select_query(self) -> None:
        result = self.validator.validate_query(
            self.file_connection,
            "SELECT name, gpa FROM students WHERE gpa > 3.5",
        )
        self.assertTrue(result.is_valid)

    def test_rejects_non_select_query(self) -> None:
        result = self.validator.validate_query(
            self.file_connection,
            "DELETE FROM students",
        )
        self.assertFalse(result.is_valid)
        self.assertIn("single SELECT", result.message)

    def test_rejects_unknown_table(self) -> None:
        result = self.validator.validate_query(
            self.file_connection,
            "SELECT * FROM teachers",
        )
        self.assertFalse(result.is_valid)
        self.assertIn("Unknown table", result.message)

    def test_rejects_unknown_column(self) -> None:
        result = self.validator.validate_query(
            self.file_connection,
            "SELECT nickname FROM students",
        )
        self.assertFalse(result.is_valid)
        self.assertIn("Unknown column", result.message)

    def test_rejects_ambiguous_column(self) -> None:
        self.file_connection.execute(
            'CREATE TABLE "students_copy" ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "student_id" INTEGER, "name" TEXT)'
        )
        result = self.validator.validate_query(
            self.file_connection,
            "SELECT name FROM students JOIN students_copy ON students.student_id = students_copy.student_id",
        )
        self.assertFalse(result.is_valid)
        self.assertIn("Ambiguous column", result.message)


if __name__ == "__main__":
    unittest.main()
