"""
Tests for SQL parser
"""

import pytest

from sqlstream.sql.ast_nodes import Condition, SelectStatement, WhereClause
from sqlstream.sql.parser import ParseError, parse


class TestBasicParsing:
    """Test basic SQL parsing"""

    def test_select_star(self):
        """Test SELECT * FROM table"""
        ast = parse("SELECT * FROM data")

        assert isinstance(ast, SelectStatement)
        assert ast.columns == ["*"]
        assert ast.source == "data"
        assert ast.where is None
        assert ast.limit is None

    def test_select_columns(self):
        """Test SELECT col1, col2 FROM table"""
        ast = parse("SELECT name, age FROM users")

        assert ast.columns == ["name", "age"]
        assert ast.source == "users"

    def test_select_single_column(self):
        """Test SELECT col FROM table"""
        ast = parse("SELECT name FROM users")

        assert ast.columns == ["name"]
        assert ast.source == "users"


class TestWhereClause:
    """Test WHERE clause parsing"""

    def test_where_equals(self):
        """Test WHERE col = value"""
        ast = parse("SELECT * FROM data WHERE age = 25")

        assert ast.where is not None
        assert len(ast.where.conditions) == 1

        cond = ast.where.conditions[0]
        assert cond.column == "age"
        assert cond.operator == "="
        assert cond.value == 25

    def test_where_greater_than(self):
        """Test WHERE col > value"""
        ast = parse("SELECT * FROM data WHERE age > 25")

        cond = ast.where.conditions[0]
        assert cond.operator == ">"
        assert cond.value == 25

    def test_where_string_value(self):
        """Test WHERE col = 'string'"""
        ast = parse("SELECT * FROM data WHERE name = 'Alice'")

        cond = ast.where.conditions[0]
        assert cond.column == "name"
        assert cond.value == "Alice"

    def test_where_multiple_and(self):
        """Test WHERE col1 = val1 AND col2 > val2"""
        ast = parse("SELECT * FROM data WHERE age > 25 AND city = 'NYC'")

        assert len(ast.where.conditions) == 2

        cond1 = ast.where.conditions[0]
        assert cond1.column == "age"
        assert cond1.operator == ">"
        assert cond1.value == 25

        cond2 = ast.where.conditions[1]
        assert cond2.column == "city"
        assert cond2.operator == "="
        assert cond2.value == "NYC"

    def test_where_all_operators(self):
        """Test all comparison operators"""
        operators = ["=", ">", "<", ">=", "<=", "!="]

        for op in operators:
            ast = parse(f"SELECT * FROM data WHERE age {op} 25")
            assert ast.where.conditions[0].operator == op


class TestLimitClause:
    """Test LIMIT clause parsing"""

    def test_limit(self):
        """Test LIMIT n"""
        ast = parse("SELECT * FROM data LIMIT 10")

        assert ast.limit == 10

    def test_limit_zero(self):
        """Test LIMIT 0"""
        ast = parse("SELECT * FROM data LIMIT 0")

        assert ast.limit == 0

    def test_where_and_limit(self):
        """Test WHERE ... LIMIT ..."""
        ast = parse("SELECT * FROM data WHERE age > 25 LIMIT 10")

        assert ast.where is not None
        assert ast.limit == 10


class TestComplexQueries:
    """Test complete queries"""

    def test_full_query(self):
        """Test SELECT cols FROM table WHERE conditions LIMIT n"""
        ast = parse("""
            SELECT name, age
            FROM users
            WHERE age > 25 AND city = 'NYC'
            LIMIT 10
        """)

        assert ast.columns == ["name", "age"]
        assert ast.source == "users"
        assert len(ast.where.conditions) == 2
        assert ast.limit == 10

    def test_case_insensitive(self):
        """Test that keywords are case-insensitive"""
        ast = parse("select * from data where age > 25 limit 10")

        assert ast.columns == ["*"]
        assert ast.source == "data"
        assert ast.where is not None
        assert ast.limit == 10


class TestErrorHandling:
    """Test parser error handling"""

    def test_missing_from(self):
        """Test error when FROM is missing"""
        with pytest.raises(ParseError):
            parse("SELECT * WHERE age > 25")

    def test_missing_select(self):
        """Test error when SELECT is missing"""
        with pytest.raises(ParseError):
            parse("FROM data WHERE age > 25")

    def test_invalid_operator(self):
        """Test error with invalid operator"""
        with pytest.raises(ParseError):
            parse("SELECT * FROM data WHERE age << 25")

    def test_negative_limit(self):
        """Test error with negative LIMIT"""
        with pytest.raises(ParseError):
            parse("SELECT * FROM data LIMIT -10")

    def test_non_numeric_limit(self):
        """Test error with non-numeric LIMIT"""
        with pytest.raises(ParseError):
            parse("SELECT * FROM data LIMIT abc")


class TestValueParsing:
    """Test value type inference"""

    def test_integer_value(self):
        """Test integer value parsing"""
        ast = parse("SELECT * FROM data WHERE age = 25")
        assert ast.where.conditions[0].value == 25
        assert isinstance(ast.where.conditions[0].value, int)

    def test_float_value(self):
        """Test float value parsing"""
        ast = parse("SELECT * FROM data WHERE price = 19.99")
        assert ast.where.conditions[0].value == 19.99
        assert isinstance(ast.where.conditions[0].value, float)

    def test_string_value_single_quotes(self):
        """Test string with single quotes"""
        ast = parse("SELECT * FROM data WHERE name = 'Alice'")
        assert ast.where.conditions[0].value == "Alice"

    def test_string_value_double_quotes(self):
        """Test string with double quotes"""
        ast = parse('SELECT * FROM data WHERE name = "Alice"')
        assert ast.where.conditions[0].value == "Alice"

    def test_string_value_no_quotes(self):
        """Test unquoted string (treated as identifier)"""
        ast = parse("SELECT * FROM data WHERE name = Alice")
        assert ast.where.conditions[0].value == "Alice"
