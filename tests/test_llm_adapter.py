"""Unit tests for the LLM adapter and validator integration."""

from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from data_loader import load_csv_to_sqlite
from llm_adapter import LLMAdapter, SQLProposal
from sql_validator import SQLValidator


class StubGenerator:
    def __init__(self, sql: str, explanation: str = "stub") -> None:
        self.sql = sql
        self.explanation = explanation

    def generate(self, prompt: str) -> SQLProposal:
        user_request = prompt.split("User request:", maxsplit=1)[1].strip()
        return SQLProposal(
            user_request=user_request,
            prompt=prompt,
            sql=self.sql,
            explanation=self.explanation,
        )


class LLMAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        temp_csv = Path(__file__).resolve().parents[1] / "src" / "example_data.csv"
        self.db_path = Path(__file__).resolve().parents[1] / "tests" / "llm_adapter_test.db"
        if self.db_path.exists():
            self.db_path.unlink()
        load_csv_to_sqlite(temp_csv, self.db_path, "students", if_exists="replace")
        self.connection = sqlite3.connect(self.db_path)
        self.validator = SQLValidator()

    def tearDown(self) -> None:
        self.connection.close()
        if self.db_path.exists():
            self.db_path.unlink()

    def test_prompt_includes_schema_and_user_request(self) -> None:
        adapter = LLMAdapter(sql_generator=StubGenerator("SELECT * FROM students"))
        prompt = adapter.build_prompt(self.connection, "show all students")

        self.assertIn("students", prompt)
        self.assertIn("student_id", prompt)
        self.assertIn("User request: show all students", prompt)

    def test_valid_generated_sql_passes_validation(self) -> None:
        adapter = LLMAdapter(
            sql_generator=StubGenerator("SELECT name, gpa FROM students ORDER BY gpa DESC")
        )
        proposal = adapter.translate_to_sql(self.connection, "show students by gpa")
        validation = self.validator.validate_query(self.connection, proposal.sql)

        self.assertTrue(validation.is_valid)

    def test_hallucinated_column_from_adapter_is_rejected(self) -> None:
        adapter = LLMAdapter(
            sql_generator=StubGenerator("SELECT nickname FROM students")
        )
        proposal = adapter.translate_to_sql(self.connection, "show nicknames")
        validation = self.validator.validate_query(self.connection, proposal.sql)

        self.assertFalse(validation.is_valid)
        self.assertIn("Unknown column", validation.message)

    def test_write_query_from_adapter_is_rejected(self) -> None:
        adapter = LLMAdapter(
            sql_generator=StubGenerator("DROP TABLE students")
        )
        proposal = adapter.translate_to_sql(self.connection, "delete the table")
        validation = self.validator.validate_query(self.connection, proposal.sql)

        self.assertFalse(validation.is_valid)
        self.assertIn("single SELECT", validation.message)


if __name__ == "__main__":
    unittest.main()
