"""LLM adapter for converting natural-language requests into SQL proposals.

The adapter never executes SQL. It only:
- gathers schema context
- builds a prompt
- asks a pluggable SQL generator for a SQL proposal
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Protocol

from schema_manager import SchemaManager


@dataclass(frozen=True)
class SQLProposal:
    user_request: str
    prompt: str
    sql: str
    explanation: str


class SQLGenerator(Protocol):
    """Interface for components that turn prompts into SQL text."""

    def generate(self, prompt: str) -> SQLProposal: ...


class RuleBasedSQLGenerator:
    """A small local stand-in for an external LLM during development."""

    def generate(self, prompt: str) -> SQLProposal:
        request = self._extract_user_request(prompt)
        request_lower = request.lower()

        if "top" in request_lower and "gpa" in request_lower:
            sql = "SELECT name, gpa FROM students ORDER BY gpa DESC LIMIT 5"
            explanation = "Returns the top 5 students ordered by GPA."
        elif "all students" in request_lower or "show students" in request_lower:
            sql = "SELECT id, student_id, name, major, gpa, graduated FROM students"
            explanation = "Returns the main student columns."
        elif "major" in request_lower and "count" in request_lower:
            sql = (
                "SELECT major, COUNT(*) AS student_count "
                "FROM students GROUP BY major ORDER BY student_count DESC"
            )
            explanation = "Counts students per major."
        else:
            sql = "SELECT * FROM students LIMIT 5"
            explanation = "Fallback query that returns a few rows from students."

        return SQLProposal(
            user_request=request,
            prompt=prompt,
            sql=sql,
            explanation=explanation,
        )

    def _extract_user_request(self, prompt: str) -> str:
        marker = "User request:"
        if marker not in prompt:
            return ""
        return prompt.split(marker, maxsplit=1)[1].strip()


class LLMAdapter:
    """Build prompt context and request SQL from a generator.

    This class does not execute SQL and should treat generated SQL as untrusted input.
    """

    def __init__(
        self,
        sql_generator: SQLGenerator | None = None,
        schema_manager: SchemaManager | None = None,
    ) -> None:
        self.sql_generator = sql_generator or RuleBasedSQLGenerator()
        self.schema_manager = schema_manager or SchemaManager()

    def collect_schema_context(self, connection: sqlite3.Connection) -> str:
        """Return a compact schema summary for prompting."""
        sections: list[str] = []
        for table_name in self.schema_manager.list_tables(connection):
            schema = self.schema_manager.get_existing_schema(connection, table_name)
            if schema is None:
                continue
            column_descriptions = ", ".join(
                f"{column.name} {column.sqlite_type}" for column in schema.columns
            )
            sections.append(f"{table_name} ({column_descriptions})")
        return " ; ".join(sections) if sections else "(no tables available)"

    def build_prompt(self, connection: sqlite3.Connection, user_request: str) -> str:
        """Create a schema-aware prompt for SQL generation."""
        schema_context = self.collect_schema_context(connection)
        return (
            "You are an AI assistant that converts natural-language requests into SQLite SELECT queries.\n"
            "Only generate a single SELECT statement that matches the schema.\n"
            "Do not write INSERT, UPDATE, DELETE, DROP, ALTER, or PRAGMA statements.\n"
            f"Database schema: {schema_context}\n"
            f"User request: {user_request}"
        )

    def translate_to_sql(
        self,
        connection: sqlite3.Connection,
        user_request: str,
    ) -> SQLProposal:
        """Generate a SQL proposal for a natural-language request."""
        prompt = self.build_prompt(connection, user_request)
        proposal = self.sql_generator.generate(prompt)
        return SQLProposal(
            user_request=user_request,
            prompt=prompt,
            sql=proposal.sql.strip(),
            explanation=proposal.explanation,
        )
