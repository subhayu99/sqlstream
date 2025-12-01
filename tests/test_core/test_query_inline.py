"""
Tests for inline file path support (Phase 7.6)

These tests verify that Query() works with no source parameter,
extracting sources directly from SQL queries.
"""

import pytest

from sqlstream.core.query import Query


class TestQueryInline:
    """Test inline file path support in SQL queries using Query() with no source"""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create a sample CSV file"""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,Chicago\n")
        return csv_file

    @pytest.fixture
    def second_csv(self, tmp_path):
        """Create a second CSV file for JOINs"""
        csv_file = tmp_path / "orders.csv"
        csv_file.write_text("name,product\nAlice,Laptop\nBob,Phone\nAlice,Tablet\n")
        return csv_file

    def test_inline_simple_query(self, sample_csv):
        """Test simple query with inline file path"""
        q = Query()
        results = list(q.sql(f"SELECT * FROM '{sample_csv}'"))

        assert len(results) == 3
        assert results[0]["name"] == "Alice"
        assert results[1]["name"] == "Bob"

    def test_inline_with_where(self, sample_csv):
        """Test inline file path with WHERE clause"""
        q = Query()
        results = list(q.sql(f"SELECT * FROM '{sample_csv}' WHERE age > 25"))

        assert len(results) == 2
        assert all(r["age"] > 25 for r in results)

    def test_inline_unquoted_path(self, sample_csv):
        """Test inline file path without quotes (when path has no spaces)"""
        q = Query()
        results = list(q.sql(f"SELECT * FROM {sample_csv}"))

        assert len(results) == 3
        assert results[0]["name"] == "Alice"

    def test_inline_select_columns(self, sample_csv):
        """Test inline file path with column selection"""
        q = Query()
        results = list(q.sql(f"SELECT name, age FROM '{sample_csv}'"))

        assert len(results) == 3
        assert "name" in results[0]
        assert "age" in results[0]
        assert "city" not in results[0]

    def test_inline_with_limit(self, sample_csv):
        """Test inline file path with LIMIT"""
        q = Query()
        results = list(q.sql(f"SELECT * FROM '{sample_csv}' LIMIT 2"))

        assert len(results) == 2

    def test_inline_with_order_by(self, sample_csv):
        """Test inline file path with ORDER BY"""
        q = Query()
        results = list(q.sql(f"SELECT * FROM '{sample_csv}' ORDER BY age DESC"))

        assert len(results) == 3
        assert results[0]["age"] == 35  # Charlie
        assert results[1]["age"] == 30  # Alice
        assert results[2]["age"] == 25  # Bob

    def test_inline_join(self, sample_csv, second_csv):
        """Test inline file paths with JOIN"""
        q = Query()
        # Note: JOIN with inline paths needs both files quoted
        # Use unqualified column names in JOIN condition
        results = list(
            q.sql(
                f"SELECT name, age, product FROM '{sample_csv}' "
                f"JOIN '{second_csv}' ON name = name"
            )
        )

        # Should match Alice (2 products) and Bob (1 product) = 3 rows
        assert len(results) == 3
        alice_orders = [r for r in results if r["name"] == "Alice"]
        assert len(alice_orders) == 2

    def test_inline_aggregates(self, sample_csv):
        """Test inline file path with aggregate functions"""
        q = Query()
        results = list(
            q.sql(f"SELECT city, COUNT(*) FROM '{sample_csv}' GROUP BY city")
        )

        assert len(results) == 3  # 3 cities
        assert all("count_*" in r or "COUNT(*)" in str(r) for r in results)

    def test_inline_file_not_found(self):
        """Test error handling for non-existent file"""
        q = Query()
        with pytest.raises(FileNotFoundError):
            list(q.sql("SELECT * FROM 'nonexistent.csv'"))

    def test_inline_with_pandas_backend(self, sample_csv):
        """Test inline file path with pandas backend"""
        try:
            import pandas  # noqa: F401

            q = Query()
            results = list(
                q.sql(f"SELECT * FROM '{sample_csv}' WHERE age > 25", backend="pandas")
            )

            assert len(results) == 2
            assert all(r["age"] > 25 for r in results)
        except ImportError:
            pytest.skip("Pandas not installed")

    def test_inline_with_python_backend(self, sample_csv):
        """Test inline file path with pure Python backend"""
        q = Query()
        results = list(
            q.sql(f"SELECT * FROM '{sample_csv}' WHERE age > 25", backend="python")
        )

        assert len(results) == 2
        assert all(r["age"] > 25 for r in results)

    def test_inline_double_quotes(self, sample_csv):
        """Test inline file path with double quotes"""
        q = Query()
        results = list(q.sql(f'SELECT * FROM "{sample_csv}"'))

        assert len(results) == 3
        assert results[0]["name"] == "Alice"
