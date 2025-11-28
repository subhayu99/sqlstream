"""
Tests for Pandas backend

Compares pandas backend results with Python backend to ensure correctness.
"""

import tempfile
from pathlib import Path

import pytest

try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

from sqlstream.core.query import query

# Skip all tests if pandas not available
pytestmark = pytest.mark.skipif(not PANDAS_AVAILABLE, reason="Pandas not installed")


class TestPandasBackendBasic:
    """Test basic queries with pandas backend"""

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
        """Test SELECT * with both backends"""
        # Python backend
        python_result = query(str(sample_csv)).sql(
            "SELECT * FROM data", backend="python"
        ).to_list()

        # Pandas backend
        pandas_result = query(str(sample_csv)).sql(
            f"SELECT * FROM {sample_csv}", backend="pandas"
        ).to_list()

        assert len(python_result) == len(pandas_result) == 4
        # Results should match (pandas may have different types, so compare values)
        for py_row, pd_row in zip(python_result, pandas_result):
            assert py_row["name"] == pd_row["name"]
            assert py_row["age"] == pd_row["age"]

    def test_select_columns(self, sample_csv):
        """Test SELECT specific columns"""
        python_result = query(str(sample_csv)).sql(
            "SELECT name, age FROM data", backend="python"
        ).to_list()

        pandas_result = query(str(sample_csv)).sql(
            f"SELECT name, age FROM {sample_csv}", backend="pandas"
        ).to_list()

        assert len(python_result) == len(pandas_result) == 4
        assert list(python_result[0].keys()) == list(pandas_result[0].keys())

    def test_where_filter(self, sample_csv):
        """Test WHERE clause"""
        python_result = query(str(sample_csv)).sql(
            "SELECT * FROM data WHERE age > 28", backend="python"
        ).to_list()

        pandas_result = query(str(sample_csv)).sql(
            f"SELECT * FROM {sample_csv} WHERE age > 28", backend="pandas"
        ).to_list()

        assert len(python_result) == len(pandas_result) == 2
        # Should be Alice and Charlie
        names = sorted([r["name"] for r in pandas_result])
        assert names == ["Alice", "Charlie"]

    def test_limit(self, sample_csv):
        """Test LIMIT clause"""
        python_result = query(str(sample_csv)).sql(
            "SELECT * FROM data LIMIT 2", backend="python"
        ).to_list()

        pandas_result = query(str(sample_csv)).sql(
            f"SELECT * FROM {sample_csv} LIMIT 2", backend="pandas"
        ).to_list()

        assert len(python_result) == len(pandas_result) == 2


class TestPandasBackendGroupBy:
    """Test GROUP BY with pandas backend"""

    @pytest.fixture
    def sales_csv(self, tmp_path):
        """Create sales CSV file"""
        csv_file = tmp_path / "sales.csv"
        csv_file.write_text(
            "region,product,sales\n"
            "East,A,100\n"
            "East,B,150\n"
            "West,A,200\n"
            "West,B,120\n"
            "East,A,80\n"
        )
        return csv_file

    def test_groupby_sum(self, sales_csv):
        """Test GROUP BY with SUM"""
        python_result = query(str(sales_csv)).sql(
            "SELECT region, SUM(sales) FROM data GROUP BY region", backend="python"
        ).to_list()

        pandas_result = query(str(sales_csv)).sql(
            f"SELECT region, SUM(sales) FROM {sales_csv} GROUP BY region",
            backend="pandas",
        ).to_list()

        assert len(python_result) == len(pandas_result) == 2

        # Check sums match
        py_sums = {r["region"]: r["sum_sales"] for r in python_result}
        pd_sums = {r["region"]: r["sum_sales"] for r in pandas_result}

        assert py_sums["East"] == pd_sums["East"] == 330
        assert py_sums["West"] == pd_sums["West"] == 320

    def test_groupby_count(self, sales_csv):
        """Test GROUP BY with COUNT"""
        python_result = query(str(sales_csv)).sql(
            "SELECT region, COUNT(*) FROM data GROUP BY region", backend="python"
        ).to_list()

        pandas_result = query(str(sales_csv)).sql(
            f"SELECT region, COUNT(*) FROM {sales_csv} GROUP BY region",
            backend="pandas",
        ).to_list()

        assert len(python_result) == len(pandas_result) == 2


class TestPandasBackendOrderBy:
    """Test ORDER BY with pandas backend"""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create sample CSV file"""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text(
            "name,age\n" "Charlie,35\n" "Alice,30\n" "Bob,25\n" "David,28\n"
        )
        return csv_file

    def test_order_by_asc(self, sample_csv):
        """Test ORDER BY ASC"""
        python_result = query(str(sample_csv)).sql(
            "SELECT * FROM data ORDER BY age ASC", backend="python"
        ).to_list()

        pandas_result = query(str(sample_csv)).sql(
            f"SELECT * FROM {sample_csv} ORDER BY age ASC", backend="pandas"
        ).to_list()

        assert len(python_result) == len(pandas_result) == 4

        # Check order
        py_ages = [r["age"] for r in python_result]
        pd_ages = [r["age"] for r in pandas_result]

        assert py_ages == pd_ages == [25, 28, 30, 35]

    def test_order_by_desc(self, sample_csv):
        """Test ORDER BY DESC"""
        pandas_result = query(str(sample_csv)).sql(
            f"SELECT * FROM {sample_csv} ORDER BY age DESC", backend="pandas"
        ).to_list()

        ages = [r["age"] for r in pandas_result]
        assert ages == [35, 30, 28, 25]


class TestPandasBackendJoin:
    """Test JOIN with pandas backend"""

    @pytest.fixture
    def customers_csv(self, tmp_path):
        """Create customers CSV file"""
        csv_file = tmp_path / "customers.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n3,Charlie\n")
        return csv_file

    @pytest.fixture
    def orders_csv(self, tmp_path):
        """Create orders CSV file"""
        csv_file = tmp_path / "orders.csv"
        csv_file.write_text(
            "order_id,customer_id,amount\n" "101,1,100\n" "102,2,200\n" "103,1,150\n"
        )
        return csv_file

    def test_inner_join(self, customers_csv, orders_csv):
        """Test INNER JOIN"""
        pandas_result = query(str(customers_csv)).sql(
            f"SELECT * FROM {customers_csv} "
            f"INNER JOIN {orders_csv} ON id = customer_id",
            backend="pandas",
        ).to_list()

        assert len(pandas_result) == 3

        # Check Alice has 2 orders
        alice_orders = [r for r in pandas_result if r["name"] == "Alice"]
        assert len(alice_orders) == 2

    def test_left_join(self, customers_csv, orders_csv):
        """Test LEFT JOIN"""
        pandas_result = query(str(customers_csv)).sql(
            f"SELECT * FROM {customers_csv} "
            f"LEFT JOIN {orders_csv} ON id = customer_id",
            backend="pandas",
        ).to_list()

        # Should have 4 rows (Alice twice, Bob once, Charlie once with NULL)
        assert len(pandas_result) == 4

        # Charlie should have NaN/None for amount
        charlie = next(r for r in pandas_result if r["name"] == "Charlie")
        assert pd.isna(charlie.get("amount")) or charlie.get("amount") is None


class TestPandasBackendAuto:
    """Test automatic backend selection"""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create sample CSV file"""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n")
        return csv_file

    def test_auto_uses_pandas(self, sample_csv):
        """Test that auto backend selects pandas when available"""
        result = query(str(sample_csv)).sql(
            f"SELECT * FROM {sample_csv}", backend="auto"
        )

        # Check that pandas backend was selected
        assert result.use_pandas is True
        assert result.backend == "auto"

    def test_can_force_python(self, sample_csv):
        """Test that python backend can be forced"""
        result = query(str(sample_csv)).sql(
            "SELECT * FROM data", backend="python"
        )

        assert result.use_pandas is False
        assert result.backend == "python"


class TestPandasBackendExplain:
    """Test explain() with pandas backend"""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create sample CSV file"""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name,age\n1,Alice,30\n2,Bob,25\n")
        return csv_file

    def test_explain_format(self, sample_csv):
        """Test explain output format"""
        result = query(str(sample_csv)).sql(
            f"SELECT * FROM {sample_csv} WHERE age > 25 LIMIT 1", backend="pandas"
        )

        plan = result.explain()

        # Check plan contains key operations
        assert "Pandas Execution Plan" in plan
        assert "Load DataFrame" in plan
        assert "Filter" in plan
        assert "Limit" in plan
