"""
Tests for SQL parser
"""

import pytest

from sqlstream.sql.ast_nodes import (
    AggregateFunction,
    Condition,
    JoinClause,
    OrderByColumn,
    SelectStatement,
    WhereClause,
)
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


class TestAggregateFunctions:
    """Test aggregate function parsing"""

    def test_count_star(self):
        """Test COUNT(*)"""
        ast = parse("SELECT COUNT(*) FROM data")

        assert ast.aggregates is not None
        assert len(ast.aggregates) == 1

        agg = ast.aggregates[0]
        assert agg.function == "COUNT"
        assert agg.column == "*"
        assert agg.alias is None

    def test_count_column(self):
        """Test COUNT(column)"""
        ast = parse("SELECT COUNT(id) FROM users")

        agg = ast.aggregates[0]
        assert agg.function == "COUNT"
        assert agg.column == "id"

    def test_sum_function(self):
        """Test SUM(column)"""
        ast = parse("SELECT SUM(amount) FROM transactions")

        agg = ast.aggregates[0]
        assert agg.function == "SUM"
        assert agg.column == "amount"

    def test_avg_function(self):
        """Test AVG(column)"""
        ast = parse("SELECT AVG(price) FROM products")

        agg = ast.aggregates[0]
        assert agg.function == "AVG"
        assert agg.column == "price"

    def test_min_max_functions(self):
        """Test MIN and MAX functions"""
        ast_min = parse("SELECT MIN(age) FROM users")
        assert ast_min.aggregates[0].function == "MIN"

        ast_max = parse("SELECT MAX(age) FROM users")
        assert ast_max.aggregates[0].function == "MAX"

    def test_aggregate_with_alias(self):
        """Test COUNT(*) AS total"""
        ast = parse("SELECT COUNT(*) AS total FROM data")

        agg = ast.aggregates[0]
        assert agg.function == "COUNT"
        assert agg.alias == "total"
        assert "total" in ast.columns

    def test_multiple_aggregates(self):
        """Test multiple aggregate functions"""
        ast = parse("SELECT COUNT(*), SUM(amount), AVG(price) FROM data")

        assert len(ast.aggregates) == 3
        assert ast.aggregates[0].function == "COUNT"
        assert ast.aggregates[1].function == "SUM"
        assert ast.aggregates[2].function == "AVG"

    def test_mixed_columns_and_aggregates(self):
        """Test mixing regular columns with aggregates"""
        ast = parse("SELECT city, COUNT(*) FROM data")

        assert "city" in ast.columns
        assert len(ast.aggregates) == 1
        assert ast.aggregates[0].function == "COUNT"


class TestGroupByClause:
    """Test GROUP BY clause parsing"""

    def test_group_by_single(self):
        """Test GROUP BY single column"""
        ast = parse("SELECT city, COUNT(*) FROM data GROUP BY city")

        assert ast.group_by is not None
        assert ast.group_by == ["city"]

    def test_group_by_multiple(self):
        """Test GROUP BY multiple columns"""
        ast = parse("SELECT city, country, COUNT(*) FROM data GROUP BY city, country")

        assert ast.group_by == ["city", "country"]

    def test_group_by_with_where(self):
        """Test GROUP BY with WHERE clause"""
        ast = parse("SELECT city, COUNT(*) FROM data WHERE age > 18 GROUP BY city")

        assert ast.where is not None
        assert ast.group_by == ["city"]
        assert len(ast.where.conditions) == 1

    def test_group_by_with_order_limit(self):
        """Test GROUP BY with ORDER BY and LIMIT"""
        ast = parse(
            "SELECT city, COUNT(*) FROM data GROUP BY city ORDER BY city LIMIT 10"
        )

        assert ast.group_by == ["city"]
        assert ast.order_by is not None
        assert ast.limit == 10


class TestOrderByClause:
    """Test ORDER BY clause parsing"""

    def test_order_by_single(self):
        """Test ORDER BY single column"""
        ast = parse("SELECT * FROM data ORDER BY name")

        assert ast.order_by is not None
        assert len(ast.order_by) == 1
        assert ast.order_by[0].column == "name"
        assert ast.order_by[0].direction == "ASC"  # Default

    def test_order_by_asc(self):
        """Test ORDER BY column ASC"""
        ast = parse("SELECT * FROM data ORDER BY name ASC")

        assert ast.order_by[0].column == "name"
        assert ast.order_by[0].direction == "ASC"

    def test_order_by_desc(self):
        """Test ORDER BY column DESC"""
        ast = parse("SELECT * FROM data ORDER BY age DESC")

        assert ast.order_by[0].column == "age"
        assert ast.order_by[0].direction == "DESC"

    def test_order_by_multiple(self):
        """Test ORDER BY multiple columns"""
        ast = parse("SELECT * FROM data ORDER BY city ASC, age DESC")

        assert len(ast.order_by) == 2
        assert ast.order_by[0].column == "city"
        assert ast.order_by[0].direction == "ASC"
        assert ast.order_by[1].column == "age"
        assert ast.order_by[1].direction == "DESC"

    def test_order_by_with_where(self):
        """Test ORDER BY with WHERE clause"""
        ast = parse("SELECT * FROM data WHERE age > 18 ORDER BY name")

        assert ast.where is not None
        assert ast.order_by is not None

    def test_order_by_with_limit(self):
        """Test ORDER BY with LIMIT"""
        ast = parse("SELECT * FROM data ORDER BY age DESC LIMIT 5")

        assert ast.order_by[0].column == "age"
        assert ast.order_by[0].direction == "DESC"
        assert ast.limit == 5


class TestComplexQueriesPhase4:
    """Test complex queries with all Phase 4 features"""

    def test_full_aggregation_query(self):
        """Test complete query with all aggregation features"""
        sql = """
            SELECT city, COUNT(*) AS count, AVG(age) AS avg_age
            FROM users
            WHERE age > 18
            GROUP BY city
            ORDER BY count DESC
            LIMIT 10
        """
        ast = parse(sql)

        # Verify all components
        assert ast.source == "users"
        assert ast.where is not None
        assert ast.group_by == ["city"]
        assert ast.order_by is not None
        assert ast.order_by[0].column == "count"
        assert ast.order_by[0].direction == "DESC"
        assert ast.limit == 10
        assert len(ast.aggregates) == 2

    def test_simple_order_by(self):
        """Test simple ORDER BY without aggregation"""
        ast = parse("SELECT name, age FROM users ORDER BY age")

        assert ast.columns == ["name", "age"]
        assert ast.order_by[0].column == "age"
        assert ast.aggregates is None
        assert ast.group_by is None


class TestJoinParsing:
    """Test JOIN clause parsing (Phase 5)"""

    def test_inner_join_explicit(self):
        """Test INNER JOIN with explicit keyword"""
        ast = parse("SELECT * FROM customers INNER JOIN orders ON customers.id = orders.customer_id")

        assert ast.source == "customers"
        assert ast.join is not None
        assert isinstance(ast.join, JoinClause)
        assert ast.join.join_type == "INNER"
        assert ast.join.right_source == "orders"
        assert ast.join.on_left == "id"
        assert ast.join.on_right == "customer_id"

    def test_inner_join_implicit(self):
        """Test JOIN defaults to INNER JOIN"""
        ast = parse("SELECT * FROM customers JOIN orders ON id = customer_id")

        assert ast.join is not None
        assert ast.join.join_type == "INNER"
        assert ast.join.right_source == "orders"
        assert ast.join.on_left == "id"
        assert ast.join.on_right == "customer_id"

    def test_left_join(self):
        """Test LEFT JOIN"""
        ast = parse("SELECT * FROM customers LEFT JOIN orders ON customers.id = orders.customer_id")

        assert ast.join is not None
        assert ast.join.join_type == "LEFT"
        assert ast.join.right_source == "orders"
        assert ast.join.on_left == "id"
        assert ast.join.on_right == "customer_id"

    def test_right_join(self):
        """Test RIGHT JOIN"""
        ast = parse("SELECT * FROM customers RIGHT JOIN orders ON customers.id = orders.customer_id")

        assert ast.join is not None
        assert ast.join.join_type == "RIGHT"
        assert ast.join.right_source == "orders"

    def test_join_without_table_qualification(self):
        """Test JOIN with unqualified column names"""
        ast = parse("SELECT * FROM users JOIN posts ON user_id = id")

        assert ast.join.on_left == "user_id"
        assert ast.join.on_right == "id"

    def test_join_with_where(self):
        """Test JOIN combined with WHERE clause"""
        ast = parse("SELECT * FROM customers JOIN orders ON id = customer_id WHERE total > 100")

        assert ast.join is not None
        assert ast.where is not None
        assert len(ast.where.conditions) == 1
        assert ast.where.conditions[0].column == "total"

    def test_join_with_columns(self):
        """Test JOIN with specific columns"""
        ast = parse("SELECT name, amount FROM customers JOIN orders ON id = customer_id")

        assert ast.columns == ["name", "amount"]
        assert ast.join is not None

    def test_join_with_order_by(self):
        """Test JOIN with ORDER BY"""
        ast = parse("SELECT * FROM customers JOIN orders ON id = customer_id ORDER BY amount DESC")

        assert ast.join is not None
        assert ast.order_by is not None
        assert ast.order_by[0].column == "amount"
        assert ast.order_by[0].direction == "DESC"

    def test_join_with_limit(self):
        """Test JOIN with LIMIT"""
        ast = parse("SELECT * FROM customers JOIN orders ON id = customer_id LIMIT 10")

        assert ast.join is not None
        assert ast.limit == 10

    def test_join_full_query(self):
        """Test JOIN with all clauses"""
        sql = """
            SELECT name, total
            FROM customers
            INNER JOIN orders ON customers.id = orders.customer_id
            WHERE total > 100
            ORDER BY total DESC
            LIMIT 5
        """
        ast = parse(sql)

        assert ast.columns == ["name", "total"]
        assert ast.source == "customers"
        assert ast.join is not None
        assert ast.join.join_type == "INNER"
        assert ast.join.right_source == "orders"
        assert ast.where is not None
        assert ast.order_by is not None
        assert ast.limit == 5
