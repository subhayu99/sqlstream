"""
Tests for CSV reader
"""

import tempfile
from pathlib import Path

import pytest

from sqlstream.readers.csv_reader import CSVReader
from sqlstream.sql.ast_nodes import Condition


@pytest.fixture
def sample_csv_file(tmp_path):
    """Create a sample CSV file for testing"""
    csv_content = """name,age,city,salary
Alice,30,NYC,75000
Bob,25,LA,65000
Charlie,35,SF,85000
Diana,28,NYC,70000
Eve,32,LA,80000"""

    csv_file = tmp_path / "test_data.csv"
    csv_file.write_text(csv_content)
    return csv_file


@pytest.fixture
def mixed_types_csv(tmp_path):
    """CSV with mixed types for testing type inference"""
    csv_content = """name,age,price,active
Alice,30,19.99,true
Bob,25,25.50,false
Charlie,35,30.00,true"""

    csv_file = tmp_path / "mixed.csv"
    csv_file.write_text(csv_content)
    return csv_file


@pytest.fixture
def empty_csv(tmp_path):
    """Empty CSV file"""
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("name,age,city\n")
    return csv_file


class TestBasicReading:
    """Test basic CSV reading"""

    def test_read_csv(self, sample_csv_file):
        """Test reading CSV file"""
        reader = CSVReader(str(sample_csv_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 5
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 30  # Should be int
        assert rows[0]["city"] == "NYC"

    def test_type_inference(self, mixed_types_csv):
        """Test that types are inferred correctly"""
        reader = CSVReader(str(mixed_types_csv))
        rows = list(reader.read_lazy())

        row = rows[0]

        # String
        assert isinstance(row["name"], str)
        assert row["name"] == "Alice"

        # Integer
        assert isinstance(row["age"], int)
        assert row["age"] == 30

        # Float
        assert isinstance(row["price"], float)
        assert row["price"] == 19.99

        # String (boolean not inferred yet)
        assert isinstance(row["active"], str)

    def test_empty_csv(self, empty_csv):
        """Test reading empty CSV"""
        reader = CSVReader(str(empty_csv))
        rows = list(reader.read_lazy())

        assert len(rows) == 0

    def test_file_not_found(self):
        """Test error when file doesn't exist"""
        with pytest.raises(FileNotFoundError):
            CSVReader("nonexistent.csv")


class TestLazyIteration:
    """Test lazy iteration (generator behavior)"""

    def test_lazy_evaluation(self, sample_csv_file):
        """Test that data is not loaded all at once"""
        reader = CSVReader(str(sample_csv_file))
        iterator = reader.read_lazy()

        # Iterator should be a generator
        assert hasattr(iterator, "__iter__")
        assert hasattr(iterator, "__next__")

        # Get first row
        first_row = next(iterator)
        assert first_row["name"] == "Alice"

        # Get second row
        second_row = next(iterator)
        assert second_row["name"] == "Bob"

    def test_iterate_with_for_loop(self, sample_csv_file):
        """Test using reader in for loop"""
        reader = CSVReader(str(sample_csv_file))
        names = []

        for row in reader:
            names.append(row["name"])

        assert names == ["Alice", "Bob", "Charlie", "Diana", "Eve"]


class TestPredicatePushdown:
    """Test predicate pushdown optimization"""

    def test_filter_equals(self, sample_csv_file):
        """Test filter with equals"""
        reader = CSVReader(str(sample_csv_file))
        reader.set_filter([Condition("city", "=", "NYC")])

        rows = list(reader.read_lazy())

        assert len(rows) == 2
        assert all(row["city"] == "NYC" for row in rows)

    def test_filter_greater_than(self, sample_csv_file):
        """Test filter with greater than"""
        reader = CSVReader(str(sample_csv_file))
        reader.set_filter([Condition("age", ">", 30)])

        rows = list(reader.read_lazy())

        assert len(rows) == 2
        assert all(row["age"] > 30 for row in rows)

    def test_filter_multiple_conditions(self, sample_csv_file):
        """Test filter with multiple AND conditions"""
        reader = CSVReader(str(sample_csv_file))
        reader.set_filter([Condition("age", ">", 25), Condition("city", "=", "NYC")])

        rows = list(reader.read_lazy())

        # Only Alice (30, NYC) and Diana (28, NYC) match
        assert len(rows) == 2
        assert all(row["age"] > 25 and row["city"] == "NYC" for row in rows)

    def test_filter_no_matches(self, sample_csv_file):
        """Test filter that matches nothing"""
        reader = CSVReader(str(sample_csv_file))
        reader.set_filter([Condition("age", ">", 100)])

        rows = list(reader.read_lazy())

        assert len(rows) == 0

    def test_supports_pushdown(self, sample_csv_file):
        """Test that CSV reader supports pushdown"""
        reader = CSVReader(str(sample_csv_file))
        assert reader.supports_pushdown() is True


class TestColumnPruning:
    """Test column pruning optimization"""

    def test_select_specific_columns(self, sample_csv_file):
        """Test selecting only specific columns"""
        reader = CSVReader(str(sample_csv_file))
        reader.set_columns(["name", "age"])

        rows = list(reader.read_lazy())

        assert len(rows) == 5
        # Should only have name and age
        assert set(rows[0].keys()) == {"name", "age"}
        assert "city" not in rows[0]
        assert "salary" not in rows[0]

    def test_select_single_column(self, sample_csv_file):
        """Test selecting single column"""
        reader = CSVReader(str(sample_csv_file))
        reader.set_columns(["name"])

        rows = list(reader.read_lazy())

        assert len(rows) == 5
        assert set(rows[0].keys()) == {"name"}

    def test_supports_column_selection(self, sample_csv_file):
        """Test that CSV reader supports column selection"""
        reader = CSVReader(str(sample_csv_file))
        assert reader.supports_column_selection() is True


class TestCombinedOptimizations:
    """Test combining predicate pushdown and column pruning"""

    def test_filter_and_project(self, sample_csv_file):
        """Test applying both filter and column selection"""
        reader = CSVReader(str(sample_csv_file))
        reader.set_filter([Condition("age", ">", 30)])
        reader.set_columns(["name", "age"])

        rows = list(reader.read_lazy())

        # Only rows with age > 30
        assert len(rows) == 2

        # Only name and age columns
        assert set(rows[0].keys()) == {"name", "age"}

        # Verify values
        assert all(row["age"] > 30 for row in rows)


class TestSchemaInference:
    """Test schema inference"""

    def test_get_schema(self, mixed_types_csv):
        """Test schema inference from first row"""
        reader = CSVReader(str(mixed_types_csv))
        schema = reader.get_schema()

        assert "name" in schema
        assert "age" in schema
        assert "price" in schema

        assert schema["name"] == "str"
        assert schema["age"] == "int"
        assert schema["price"] == "float"

    def test_schema_empty_file(self, empty_csv):
        """Test schema on empty file"""
        reader = CSVReader(str(empty_csv))
        schema = reader.get_schema()

        assert len(schema) == 0


class TestMalformedData:
    """Test handling of malformed data"""

    def test_null_values(self, tmp_path):
        """Test handling of NULL/empty values"""
        csv_content = """name,age,city
Alice,30,NYC
Bob,,LA
Charlie,35,"""

        csv_file = tmp_path / "nulls.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(str(csv_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 3
        assert rows[1]["age"] is None
        assert rows[2]["city"] is None
