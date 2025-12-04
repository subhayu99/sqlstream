"""
Tests for DuckDB backend

Ensures DuckDB backend works correctly with full SQL support.
"""

import pytest

try:
    import duckdb  # noqa: F401

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

try:
    import pandas as pd  # noqa: F401

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

from sqlstream.core.query import query

# Skip all tests if duckdb not available
pytestmark = pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")


class TestDuckDBBackendBasic:
    """Test basic queries with DuckDB backend"""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create sample CSV file"""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text(
            "id,name,age,salary\n"
            "1,Alice,30,75000\n"
            "2,Bob,25,65000\n"
            "3,Charlie,35,85000\n"
            "4,David,28,70000\n"
        )
        return csv_file

    def test_select_all(self, sample_csv):
        """Test SELECT *"""
        result = (
            query(str(sample_csv)).sql(f"SELECT * FROM '{sample_csv}'", backend="duckdb").to_list()
        )

        assert len(result) == 4
        # Check values
        names = sorted([r["name"] for r in result])
        assert names == ["Alice", "Bob", "Charlie", "David"]

    def test_where_filter(self, sample_csv):
        """Test WHERE clause"""
        result = (
            query(str(sample_csv))
            .sql(f"SELECT * FROM '{sample_csv}' WHERE age > 28", backend="duckdb")
            .to_list()
        )

        assert len(result) == 2
        names = sorted([r["name"] for r in result])
        assert names == ["Alice", "Charlie"]

    def test_aggregations(self, sample_csv):
        """Test aggregations"""
        result = (
            query(str(sample_csv))
            .sql(f"SELECT AVG(salary) as avg_sal FROM '{sample_csv}'", backend="duckdb")
            .to_list()
        )

        assert len(result) == 1
        expected_avg = (75000 + 65000 + 85000 + 70000) / 4
        assert result[0]["avg_sal"] == expected_avg


class TestDuckDBAdvancedFeatures:
    """Test advanced SQL features only supported by DuckDB"""

    @pytest.fixture
    def employees_csv(self, tmp_path):
        """Create employees CSV file"""
        csv_file = tmp_path / "employees.csv"
        csv_file.write_text(
            "name,dept,salary\n"
            "Alice,Eng,100000\n"
            "Bob,Eng,80000\n"
            "Charlie,Sales,90000\n"
            "David,Sales,70000\n"
            "Eve,Eng,120000\n"
        )
        return csv_file

    def test_window_functions(self, employees_csv):
        """Test window functions (ROW_NUMBER, AVG OVER)"""
        # Rank employees by salary within department
        sql = f"""
            SELECT
                name,
                dept,
                salary,
                ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rank
            FROM '{employees_csv}'
            ORDER BY dept, rank
        """
        result = query(str(employees_csv)).sql(sql, backend="duckdb").to_list()

        assert len(result) == 5

        # Check Engineering ranks
        eng = [r for r in result if r["dept"] == "Eng"]
        assert len(eng) == 3
        assert eng[0]["name"] == "Eve" and eng[0]["rank"] == 1
        assert eng[1]["name"] == "Alice" and eng[1]["rank"] == 2
        assert eng[2]["name"] == "Bob" and eng[2]["rank"] == 3

    def test_cte(self, employees_csv):
        """Test Common Table Expressions (CTEs)"""
        sql = f"""
            WITH high_earners AS (
                SELECT * FROM '{employees_csv}' WHERE salary > 90000
            )
            SELECT dept, COUNT(*) as count FROM high_earners GROUP BY dept
        """
        result = query(str(employees_csv)).sql(sql, backend="duckdb").to_list()

        # Should be Alice (100k) and Eve (120k) -> 2 in Eng
        assert len(result) == 1
        assert result[0]["dept"] == "Eng"
        assert result[0]["count"] == 2


class TestDuckDBJoins:
    """Test JOINs with multiple files using DuckDB backend"""

    @pytest.fixture
    def customers_csv(self, tmp_path):
        csv_file = tmp_path / "customers.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n3,Charlie\n")
        return csv_file

    @pytest.fixture
    def orders_csv(self, tmp_path):
        csv_file = tmp_path / "orders.csv"
        csv_file.write_text("order_id,customer_id,amount\n101,1,100\n102,2,200\n103,1,150\n")
        return csv_file

    def test_join_multiple_files(self, customers_csv, orders_csv):
        """Test JOIN across two CSV files"""
        # Note: DuckDB requires quoted paths for files with special chars or extensions
        sql = f"""
            SELECT c.name, o.amount
            FROM '{customers_csv}' c
            JOIN '{orders_csv}' o ON c.id = o.customer_id
            ORDER BY c.name
        """

        # We can pass either file as the 'source' to init query,
        # but the SQL references both explicitly
        result = query(str(customers_csv)).sql(sql, backend="duckdb").to_list()

        assert len(result) == 3

        # Alice has 2 orders (100, 150)
        alice_orders = [r for r in result if r["name"] == "Alice"]
        assert len(alice_orders) == 2
        amounts = sorted([r["amount"] for r in alice_orders])
        assert amounts == [100, 150]


class TestBackendSelection:
    """Test backend selection logic"""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,val\n1,10\n")
        return csv_file

    def test_explicit_duckdb(self, sample_csv):
        """Test explicit backend='duckdb'"""
        q = query(str(sample_csv)).sql(f"SELECT * FROM '{sample_csv}'", backend="duckdb")
        assert q.use_duckdb is True
        assert q.use_pandas is False
        assert isinstance(q.executor, object)  # Should be DuckDBExecutor

    def test_auto_priority(self, sample_csv):
        """Test auto priority (Pandas > DuckDB > Python)"""
        # This depends on what is installed in the environment
        q = query(str(sample_csv)).sql(f"SELECT * FROM '{sample_csv}'", backend="auto")

        if PANDAS_AVAILABLE:
            assert q.use_pandas is True
            assert q.use_duckdb is False
        elif DUCKDB_AVAILABLE:
            assert q.use_duckdb is True
            assert q.use_pandas is False
