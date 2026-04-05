"""Lightweight SQL validation for read-only SQLite queries."""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass

from schema_manager import SchemaManager


SQL_KEYWORDS = {
    "and",
    "as",
    "asc",
    "avg",
    "between",
    "by",
    "count",
    "desc",
    "distinct",
    "from",
    "group",
    "having",
    "in",
    "is",
    "join",
    "left",
    "like",
    "limit",
    "max",
    "min",
    "not",
    "null",
    "offset",
    "on",
    "or",
    "order",
    "right",
    "select",
    "sum",
    "where",
}


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    message: str


class SQLValidator:
    """Validate simple read-only SELECT queries against the live database schema."""

    def __init__(self, schema_manager: SchemaManager | None = None) -> None:
        self.schema_manager = schema_manager or SchemaManager()

    def validate_query(self, connection: sqlite3.Connection, query: str) -> ValidationResult:
        """Validate a SQL query before execution.

        Rules:
        - only a single SELECT statement is allowed
        - referenced tables must exist
        - referenced columns must exist
        """
        normalized_query = query.strip()
        if not normalized_query:
            return ValidationResult(False, "Query cannot be empty.")

        if not self._is_single_select_query(normalized_query):
            return ValidationResult(False, "Only a single SELECT query is allowed.")

        sanitized_query = self._strip_string_literals(normalized_query)
        table_aliases = self._extract_table_aliases(sanitized_query)
        if not table_aliases:
            return ValidationResult(False, "Query must reference at least one table in a FROM clause.")

        available_tables = {
            table_name: self.schema_manager.get_existing_schema(connection, table_name)
            for table_name in self.schema_manager.list_tables(connection)
        }

        for table_name in table_aliases.values():
            if table_name not in available_tables or available_tables[table_name] is None:
                return ValidationResult(False, f"Unknown table referenced: {table_name}")

        table_columns = {
            table_name: {
                column.name
                for column in available_tables[table_name].columns
            }
            for table_name in table_aliases.values()
        }

        qualified_references = self._extract_qualified_column_references(sanitized_query)
        for alias, column_name in qualified_references:
            if alias not in table_aliases:
                return ValidationResult(False, f"Unknown table or alias referenced: {alias}")
            table_name = table_aliases[alias]
            if column_name not in table_columns[table_name]:
                return ValidationResult(False, f"Unknown column referenced: {alias}.{column_name}")

        unqualified_references = self._extract_unqualified_column_references(
            sanitized_query,
            table_aliases=table_aliases,
        )
        for column_name in unqualified_references:
            matching_tables = [
                table_name
                for table_name in dict.fromkeys(table_aliases.values())
                if column_name in table_columns[table_name]
            ]
            if not matching_tables:
                return ValidationResult(False, f"Unknown column referenced: {column_name}")
            if len(matching_tables) > 1:
                return ValidationResult(
                    False,
                    f"Ambiguous column referenced: {column_name}. Qualify it with a table name.",
                )

        try:
            connection.execute(f"EXPLAIN QUERY PLAN {normalized_query.rstrip(';')}")
        except sqlite3.Error as exc:
            return ValidationResult(False, f"SQLite rejected the query: {exc}")

        return ValidationResult(True, "Query is valid.")

    def execute_query(
        self, connection: sqlite3.Connection, query: str
    ) -> tuple[list[str], list[tuple[object, ...]]]:
        """Execute a validated SELECT query and return headers and rows."""
        cursor = connection.execute(query.rstrip(";"))
        columns = [description[0] for description in cursor.description or []]
        rows = cursor.fetchall()
        return columns, rows

    def _is_single_select_query(self, query: str) -> bool:
        stripped_query = query.strip()
        if not re.match(r"(?is)^select\b", stripped_query):
            return False
        if ";" in stripped_query[:-1]:
            return False
        if stripped_query.count(";") > 1:
            return False
        forbidden_pattern = re.compile(
            r"(?i)\b(insert|update|delete|drop|alter|create|attach|detach|pragma|reindex|vacuum|replace|truncate)\b"
        )
        return forbidden_pattern.search(stripped_query) is None

    def _strip_string_literals(self, query: str) -> str:
        return re.sub(r"'(?:''|[^'])*'", "''", query)

    def _extract_table_aliases(self, query: str) -> dict[str, str]:
        alias_map: dict[str, str] = {}
        pattern = re.compile(
            r"(?i)\b(from|join)\s+([a-zA-Z_][\w]*)"
            r"(?:\s+(?:as\s+)?([a-zA-Z_][\w]*))?"
            r"(?=\s+\b(join|where|group|order|limit|having|on)\b|\s*$|;)"
        )
        for _keyword, table_name, alias, _next_keyword in pattern.findall(query):
            alias_map[table_name] = table_name
            if alias:
                alias_map[alias] = table_name
        return alias_map

    def _extract_qualified_column_references(self, query: str) -> list[tuple[str, str]]:
        return [
            (alias, column_name)
            for alias, column_name in re.findall(
                r"(?i)\b([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*|\*)\b", query
            )
            if column_name != "*"
        ]

    def _extract_unqualified_column_references(
        self,
        query: str,
        table_aliases: dict[str, str],
    ) -> set[str]:
        scrubbed_query = re.sub(r"(?i)\b[a-zA-Z_][\w]*\.([a-zA-Z_][\w]*|\*)\b", " ", query)
        scrubbed_query = re.sub(r"(?i)\b(from|join)\s+[a-zA-Z_][\w]*(?:\s+(?:as\s+)?[a-zA-Z_][\w]*)?", " ", scrubbed_query)

        references: set[str] = set()
        for match in re.finditer(r"\b([a-zA-Z_][\w]*)\b", scrubbed_query):
            token = match.group(1)
            token_lower = token.lower()
            next_character_index = match.end()
            next_character = scrubbed_query[next_character_index:next_character_index + 1]

            if token_lower in SQL_KEYWORDS:
                continue
            if token in table_aliases:
                continue
            if next_character == "(":
                continue
            references.add(token)

        return references
