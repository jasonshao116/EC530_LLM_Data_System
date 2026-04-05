"""Unit tests for schema manager behavior."""

from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from schema_manager import SchemaManager, build_column_mapping, normalize_identifier


class SchemaManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.schema_manager = SchemaManager()
        self.connection = sqlite3.connect(":memory:")

    def tearDown(self) -> None:
        self.connection.close()

    def test_normalize_identifier_handles_spaces_and_digits(self) -> None:
        self.assertEqual(normalize_identifier(" Student ID "), "student_id")
        self.assertEqual(normalize_identifier("123 score"), "col_123_score")

    def test_build_column_mapping_preserves_unique_names(self) -> None:
        mapping = build_column_mapping(["Name", "Name!", "Major"])
        self.assertEqual(mapping["Name"], "name")
        self.assertEqual(mapping["Name!"], "name_2")
        self.assertEqual(mapping["Major"], "major")

    def test_infer_schema_from_dataframe_creates_expected_types(self) -> None:
        dataframe = pd.DataFrame(
            {
                "Student ID": [1001, 1002],
                "Name": ["Alice", "Brian"],
                "GPA": [3.8, 3.5],
                "Graduated": [True, False],
            }
        )

        schema, normalized_df = self.schema_manager.infer_schema_from_dataframe(
            dataframe,
            "Students",
        )

        self.assertEqual(schema.table_name, "students")
        self.assertEqual(
            [(column.name, column.sqlite_type) for column in schema.columns],
            [
                ("student_id", "INTEGER"),
                ("name", "TEXT"),
                ("gpa", "REAL"),
                ("graduated", "INTEGER"),
            ],
        )
        self.assertEqual(list(normalized_df.columns), ["student_id", "name", "gpa", "graduated"])

    def test_build_create_table_sql_includes_primary_key(self) -> None:
        dataframe = pd.DataFrame({"Name": ["Alice"]})
        schema, _ = self.schema_manager.infer_schema_from_dataframe(dataframe, "students")

        sql = self.schema_manager.build_create_table_sql(schema)

        self.assertIn('"id" INTEGER PRIMARY KEY AUTOINCREMENT', sql)
        self.assertIn('"name" TEXT', sql)

    def test_compare_schemas_ignores_managed_primary_key(self) -> None:
        dataframe = pd.DataFrame({"Name": ["Alice"], "GPA": [3.8]})
        expected_schema, _ = self.schema_manager.infer_schema_from_dataframe(dataframe, "students")
        self.schema_manager.create_table(self.connection, expected_schema)
        existing_schema = self.schema_manager.get_existing_schema(self.connection, "students")

        comparison = self.schema_manager.compare_schemas(expected_schema, existing_schema)

        self.assertTrue(comparison.matches)

    def test_find_matching_table_returns_table_name(self) -> None:
        dataframe = pd.DataFrame({"Name": ["Alice"], "GPA": [3.8]})
        schema, _ = self.schema_manager.infer_schema_from_dataframe(dataframe, "students")
        self.schema_manager.create_table(self.connection, schema)

        match = self.schema_manager.find_matching_table(self.connection, schema)

        self.assertEqual(match, "students")


if __name__ == "__main__":
    unittest.main()
