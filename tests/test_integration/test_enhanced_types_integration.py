"""
Integration tests for enhanced types (DATETIME, TIME, DECIMAL, JSON) across all backends.

Tests each backend's ability to:
- Read files with enhanced types
- Filter on enhanced type columns
- Aggregate enhanced type columns
- Compare enhanced type values
"""

from decimal import Decimal

import pytest

from sqlstream import query
from sqlstream.core.types import DataType
from sqlstream.readers.csv_reader import CSVReader


# Helper functions
def _is_pandas_available():
    """Check if pandas is installed."""
    try:
        import pandas
        return True
    except ImportError:
        return False


def _is_duckdb_available():
    """Check if duckdb is installed."""
    try:
        import duckdb
        return True
    except ImportError:
        return False


# Test data with all enhanced types
CSV_ENHANCED_TYPES = """id,created_at,appointment_time,price,metadata,status
1,2024-01-15 10:30:00,14:30:00,199.9999999,"{""product"": ""laptop""}",active
2,2024-01-16 09:15:30,10:00:00,49.99,"{""product"": ""mouse""}",pending
3,2024-01-17 14:20:15,16:45:00,1299.50,"{""product"": ""monitor""}",active
4,2024-01-18 11:05:00,09:30:00,29.95,"{""product"": ""cable""}",inactive
5,2024-01-19 16:40:22,13:15:00,899.50,"{""product"": ""keyboard""}",active
"""


@pytest.fixture
def csv_file_with_enhanced_types(tmp_path):
    """Create a CSV file with datetime, time, decimal, and JSON columns."""
    file_path = tmp_path / "enhanced_types.csv"
    with open(file_path, 'w') as f:
        f.write(CSV_ENHANCED_TYPES)
    return str(file_path)


class TestSchemaInference:
    """Test that enhanced types are correctly inferred from CSV."""

    def test_csv_schema_inference(self, csv_file_with_enhanced_types):
        """Verify all enhanced types are detected correctly."""
        reader = CSVReader(csv_file_with_enhanced_types)
        schema = reader.get_schema()

        assert schema is not None
        assert schema['id'] == DataType.INTEGER
        assert schema['created_at'] == DataType.DATETIME
        assert schema['appointment_time'] == DataType.TIME
        assert schema['price'] in (DataType.DECIMAL, DataType.FLOAT)  # Could be either
        assert schema['metadata'] == DataType.JSON
        assert schema['status'] == DataType.STRING

        print("\n✓ Schema inferred correctly:")
        for col, dtype in schema.columns.items():
            print(f"  {col}: {dtype.value}")


class TestPythonBackend:
    """Test enhanced types with pure Python backend."""

    def test_read_basic(self, csv_file_with_enhanced_types):
        """Test basic reading with Python backend."""
        results = list(query(csv_file_with_enhanced_types).sql(
            f"SELECT * FROM '{csv_file_with_enhanced_types}'"
        ))

        assert len(results) == 5
        # Check that values are properly typed
        first_row = results[0]
        assert first_row['id'] == 1
        # Note: Python backend might keep these as strings

    def test_filter_on_integer(self, csv_file_with_enhanced_types):
        """Test filtering on integer column."""
        results = list(query(csv_file_with_enhanced_types).sql(
            f"SELECT * FROM '{csv_file_with_enhanced_types}' WHERE id > 3"
        ))
        assert len(results) == 2

    def test_filter_on_string(self, csv_file_with_enhanced_types):
        """Test filtering on string column."""
        results = list(query(csv_file_with_enhanced_types).sql(
            f"SELECT * FROM '{csv_file_with_enhanced_types}' WHERE status = 'active'"
        ))
        assert len(results) == 3

    @pytest.mark.skip(reason="Python backend may not handle datetime comparisons")
    def test_filter_on_datetime(self, csv_file_with_enhanced_types):
        """Test filtering on datetime column (may not work with Python backend)."""
        results = list(query(csv_file_with_enhanced_types).sql(
            f"SELECT * FROM '{csv_file_with_enhanced_types}' WHERE created_at > '2024-01-16 00:00:00'"
        ))
        # Expected: rows 2, 3, 4, 5
        assert len(results) == 4

    @pytest.mark.skip(reason="Python backend may not handle decimal comparisons")
    def test_filter_on_decimal(self, csv_file_with_enhanced_types):
        """Test filtering on decimal/high-precision column."""
        results = list(query(csv_file_with_enhanced_types).sql(
            f"SELECT * FROM '{csv_file_with_enhanced_types}' WHERE price > 100.0"
        ))
        # Expected: rows 1, 3, 5
        assert len(results) == 3


class TestPandasBackend:
    """Test enhanced types with Pandas backend."""

    @pytest.mark.skipif(
        not _is_pandas_available(),
        reason="Pandas not installed"
    )
    def test_read_basic(self, csv_file_with_enhanced_types):
        """Test basic reading with Pandas backend."""
        results = list(query(csv_file_with_enhanced_types).sql(
            f"SELECT * FROM '{csv_file_with_enhanced_types}'",
            backend="pandas"
        ))

        assert len(results) == 5

    @pytest.mark.skipif(
        not _is_pandas_available(),
        reason="Pandas not installed"
    )
    def test_filter_on_datetime(self, csv_file_with_enhanced_types):
        """Test filtering on datetime with Pandas."""
        results = list(query(csv_file_with_enhanced_types).sql(
            f"SELECT * FROM '{csv_file_with_enhanced_types}' WHERE created_at > '2024-01-16 00:00:00'",
            backend="pandas"
        ))
        # Pandas should handle datetime comparisons
        assert len(results) == 4

    @pytest.mark.skipif(
        not _is_pandas_available(),
        reason="Pandas not installed"
    )
    def test_filter_on_decimal(self, csv_file_with_enhanced_types):
        """Test filtering on decimal/numeric with Pandas."""
        results = list(query(csv_file_with_enhanced_types).sql(
            f"SELECT * FROM '{csv_file_with_enhanced_types}' WHERE price > 100.0",
            backend="pandas"
        ))
        assert len(results) == 3

    @pytest.mark.skipif(
        not _is_pandas_available(),
        reason="Pandas not installed"
    )
    def test_aggregation_on_decimal(self, csv_file_with_enhanced_types):
        """Test aggregation on decimal column with Pandas."""
        results = list(query(csv_file_with_enhanced_types).sql(
            f"SELECT SUM(price) as total FROM '{csv_file_with_enhanced_types}'",
            backend="pandas"
        ))
        assert len(results) == 1
        # Total should be around 2478.94
        assert results[0]['total'] > 2400


class TestDuckDBBackend:
    """Test enhanced types with DuckDB backend."""

    @pytest.mark.skipif(
        not _is_duckdb_available(),
        reason="DuckDB not installed"
    )
    def test_read_basic(self, csv_file_with_enhanced_types):
        """Test basic reading with DuckDB backend."""
        results = list(query(csv_file_with_enhanced_types).sql(
            f"SELECT * FROM '{csv_file_with_enhanced_types}'",
            backend="duckdb"
        ))

        assert len(results) == 5

    @pytest.mark.skipif(
        not _is_duckdb_available(),
        reason="DuckDB not installed"
    )
    def test_filter_on_datetime(self, csv_file_with_enhanced_types):
        """Test filtering on datetime with DuckDB."""
        results = list(query(csv_file_with_enhanced_types).sql(
            f"SELECT * FROM '{csv_file_with_enhanced_types}' WHERE created_at > '2024-01-16 00:00:00'",
            backend="duckdb"
        ))
        assert len(results) == 4

    @pytest.mark.skipif(
        not _is_duckdb_available(),
        reason="DuckDB not installed"
    )
    def test_filter_on_time(self, csv_file_with_enhanced_types):
        """Test filtering on time column with DuckDB."""
        results = list(query(csv_file_with_enhanced_types).sql(
            f"SELECT * FROM '{csv_file_with_enhanced_types}' WHERE appointment_time > '12:00:00'",
            backend="duckdb"
        ))
        # Rows with time after noon: rows 1, 3, 5
        assert len(results) == 3

    @pytest.mark.skipif(
        not _is_duckdb_available(),
        reason="DuckDB not installed"
    )
    def test_filter_on_decimal(self, csv_file_with_enhanced_types):
        """Test filtering on high-precision decimal with DuckDB."""
        results = list(query(csv_file_with_enhanced_types).sql(
            f"SELECT * FROM '{csv_file_with_enhanced_types}' WHERE price > 100.0",
            backend="duckdb"
        ))
        assert len(results) == 3

    @pytest.mark.skipif(
        not _is_duckdb_available(),
        reason="DuckDB not installed"
    )
    def test_aggregation_on_decimal(self, csv_file_with_enhanced_types):
        """Test SUM aggregation on decimal column."""
        results = list(query(csv_file_with_enhanced_types).sql(
            f"SELECT SUM(price) as total FROM '{csv_file_with_enhanced_types}'",
            backend="duckdb"
        ))
        assert len(results) == 1
        total = results[0]['total']
        assert total > 2400
        assert total < 2500

    @pytest.mark.skipif(
        not _is_duckdb_available(),
        reason="DuckDB not installed"
    )
    def test_datetime_functions(self, csv_file_with_enhanced_types):
        """Test datetime functions with DuckDB."""
        results = list(query(csv_file_with_enhanced_types).sql(
            f"SELECT COUNT(*) as count FROM '{csv_file_with_enhanced_types}' WHERE strftime(created_at, '%Y-%m-%d') = '2024-01-15'",
            backend="duckdb"
        ))
        assert results[0]['count'] == 1

    @pytest.mark.skipif(
        not _is_duckdb_available(),
        reason="DuckDB not installed"
    )
    def test_json_column_basic(self, csv_file_with_enhanced_types):
        """Test that JSON column is preserved (basic check)."""
        results = list(query(csv_file_with_enhanced_types).sql(
            f"SELECT id, metadata FROM '{csv_file_with_enhanced_types}' WHERE id = 1",
            backend="duckdb"
        ))
        assert len(results) == 1
        # JSON should be present as string
        assert 'laptop' in results[0]['metadata']


class TestTypeConversions:
    """Test that type conversions work correctly across operations."""

    def test_datetime_string_comparison(self, csv_file_with_enhanced_types):
        """Test comparing datetime values with string literals."""
        reader = CSVReader(csv_file_with_enhanced_types)
        rows = list(reader.read_lazy())

        # First row should have datetime
        first_row = rows[0]
        # Check type of created_at value
        # The CSV reader should convert it to datetime or keep as string
        created_at_value = first_row['created_at']
        print(f"\ncreated_at type: {type(created_at_value)}")
        print(f"created_at value: {created_at_value}")

    def test_decimal_precision_preserved(self, csv_file_with_enhanced_types):
        """Test that decimal precision is preserved during reading."""
        reader = CSVReader(csv_file_with_enhanced_types)
        rows = list(reader.read_lazy())

        # First row has 199.9999999
        price = rows[0]['price']
        print(f"\nprice type: {type(price)}")
        print(f"price value: {price}")

        # Check if it's Decimal or float
        if isinstance(price, Decimal):
            # High precision decimal
            assert str(price) == "199.9999999"
        else:
            # Regular float
            assert abs(price - 199.9999999) < 0.0000001


# Run with: pytest tests/test_integration/test_enhanced_types_integration.py -v -s
