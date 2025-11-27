"""
SQL Parser - Hand-written recursive descent parser

Parses a minimal SQL subset:
- SELECT column1, column2 FROM source
- WHERE column = value (with AND)
- LIMIT n

Design: Keep it simple for the 90% case. No complex expressions initially.
"""

import re
from typing import List, Optional

from sqlstream.sql.ast_nodes import Condition, SelectStatement, WhereClause


class ParseError(Exception):
    """Raised when SQL parsing fails"""

    pass


class SQLParser:
    """
    Simple recursive descent parser for SQL

    Grammar (simplified):
        SELECT_STMT := SELECT columns FROM source [WHERE conditions] [LIMIT n]
        columns     := * | column_name [, column_name]*
        conditions  := condition [AND condition]*
        condition   := column_name operator value
        operator    := = | > | < | >= | <= | !=
    """

    def __init__(self, sql: str):
        self.sql = sql.strip()
        self.tokens = self._tokenize(sql)
        self.pos = 0

    def _tokenize(self, sql: str) -> List[str]:
        """
        Simple tokenization by splitting on whitespace and special characters

        This is intentionally simple. A production parser would use a proper lexer.
        """
        # Replace commas and parens with spaces around them
        sql = re.sub(r"([,()])", r" \1 ", sql)

        # Split on whitespace
        tokens = sql.split()

        return tokens

    def current(self) -> Optional[str]:
        """Get current token without advancing"""
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return None

    def peek(self, offset: int = 1) -> Optional[str]:
        """Look ahead at token"""
        pos = self.pos + offset
        if pos < len(self.tokens):
            return self.tokens[pos]
        return None

    def consume(self, expected: Optional[str] = None) -> str:
        """
        Consume and return current token, optionally checking it matches expected

        Args:
            expected: If provided, raises ParseError if current token doesn't match

        Returns:
            The consumed token

        Raises:
            ParseError: If expected token doesn't match or no more tokens
        """
        if self.pos >= len(self.tokens):
            raise ParseError(f"Unexpected end of query. Expected: {expected}")

        token = self.tokens[self.pos]

        if expected and token.upper() != expected.upper():
            raise ParseError(
                f"Expected '{expected}' but got '{token}' at position {self.pos}"
            )

        self.pos += 1
        return token

    def parse(self) -> SelectStatement:
        """Parse SQL query into AST"""
        return self._parse_select()

    def _parse_select(self) -> SelectStatement:
        """Parse SELECT statement"""
        self.consume("SELECT")

        # Parse columns
        columns = self._parse_columns()

        # Parse FROM
        self.consume("FROM")
        source = self.consume()

        # Optional WHERE clause
        where = None
        if self.current() and self.current().upper() == "WHERE":
            where = self._parse_where()

        # Optional LIMIT clause
        limit = None
        if self.current() and self.current().upper() == "LIMIT":
            limit = self._parse_limit()

        return SelectStatement(columns=columns, source=source, where=where, limit=limit)

    def _parse_columns(self) -> List[str]:
        """
        Parse column list

        Examples:
            *
            name
            name, age
            name, age, city
        """
        columns = []

        # Check for SELECT *
        if self.current() == "*":
            self.consume()
            return ["*"]

        # Parse comma-separated column names
        while True:
            column = self.consume()
            columns.append(column)

            # Check for comma (more columns)
            if self.current() == ",":
                self.consume(",")
            else:
                break

        return columns

    def _parse_where(self) -> WhereClause:
        """
        Parse WHERE clause

        Example: WHERE age > 25 AND city = 'NYC'
        """
        self.consume("WHERE")

        conditions = []

        # Parse first condition
        conditions.append(self._parse_condition())

        # Parse additional AND conditions
        while self.current() and self.current().upper() == "AND":
            self.consume("AND")
            conditions.append(self._parse_condition())

        return WhereClause(conditions=conditions)

    def _parse_condition(self) -> Condition:
        """
        Parse a single condition: column operator value

        Examples:
            age > 25
            name = 'Alice'
            city != 'NYC'
        """
        column = self.consume()
        operator = self.consume()

        # Parse value (could be number, string, or identifier)
        value_token = self.consume()
        value = self._parse_value(value_token)

        # Validate operator
        valid_operators = {"=", ">", "<", ">=", "<=", "!=", "<>"}
        if operator not in valid_operators:
            raise ParseError(f"Invalid operator: {operator}")

        # Normalize <> to !=
        if operator == "<>":
            operator = "!="

        return Condition(column=column, operator=operator, value=value)

    def _parse_value(self, token: str):
        """
        Parse a value token into appropriate Python type

        Examples:
            '123' -> 123 (int)
            '3.14' -> 3.14 (float)
            "'Alice'" -> 'Alice' (string, quotes removed)
            'Alice' -> 'Alice' (string)
        """
        # Remove quotes if present
        if (token.startswith("'") and token.endswith("'")) or (
            token.startswith('"') and token.endswith('"')
        ):
            return token[1:-1]

        # Try parsing as number
        try:
            # Try int first
            if "." not in token:
                return int(token)
            # Then float
            return float(token)
        except ValueError:
            # Return as string
            return token

    def _parse_limit(self) -> int:
        """Parse LIMIT clause"""
        self.consume("LIMIT")
        limit_str = self.consume()

        try:
            limit = int(limit_str)
            if limit < 0:
                raise ParseError(f"LIMIT must be non-negative, got {limit}")
            return limit
        except ValueError:
            raise ParseError(f"LIMIT must be an integer, got '{limit_str}'")


def parse(sql: str) -> SelectStatement:
    """
    Convenience function to parse SQL query

    Args:
        sql: SQL query string

    Returns:
        Parsed SelectStatement AST

    Raises:
        ParseError: If query is invalid

    Examples:
        >>> ast = parse("SELECT * FROM data")
        >>> ast = parse("SELECT name, age FROM users WHERE age > 25 LIMIT 10")
    """
    parser = SQLParser(sql)
    return parser.parse()
