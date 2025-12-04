"""Tests for schema inference in readers."""

import csv

import pytest

from sqlstream.core.types import DataType
from sqlstream.readers.csv_reader import CSVReader


@pytest.fixture
def sample_csv(tmp_path):
    """Create a sample CSV file with mixed types."""
    csv_file = tmp_path / "schema_test.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "age", "salary", "active", "hire_date"])
        writer.writerow(["Alice", "30", "95000.50", "true", "2020-01-15"])
        writer.writerow(["Bob", "25", "75000", "false", "2019-06-01"])
        writer.writerow(["Charlie", "35", "105000.00", "true", "2018-03-20"])
    return str(csv_file)


@pytest.fixture
def numeric_csv(tmp_path):
    """Create a CSV with mixed numeric types."""
    csv_file = tmp_path / "numeric.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "value", "price"])
        writer.writerow(["1", "100", "10.5"])
        writer.writerow(["2", "200", "20.0"])
        writer.writerow(["3", "300", "30.5"])
    return str(csv_file)


@pytest.fixture
def null_csv(tmp_path):
    """Create a CSV with NULL values."""
    csv_file = tmp_path / "nulls.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "age", "city"])
        writer.writerow(["Alice", "", "NYC"])
        writer.writerow(["Bob", "25", ""])
        writer.writerow(["Charlie", "35", "LA"])
    return str(csv_file)


@pytest.fixture
def empty_csv(tmp_path):
    """Create an empty CSV file with only headers."""
    csv_file = tmp_path / "empty.csv"
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "age"])
    return str(csv_file)


class TestCSVReaderSchemaInference:
    """Test schema inference in CSVReader."""

    def test_schema_inference_basic_types(self, sample_csv):
        """Test inferring basic types from CSV."""
        reader = CSVReader(sample_csv)
        schema = reader.get_schema()

        assert schema is not None
        assert len(schema) == 5
        assert schema["name"] == DataType.STRING
        assert schema["age"] == DataType.INTEGER
        assert schema["salary"] == DataType.FLOAT
        assert schema["active"] == DataType.BOOLEAN
        assert schema["hire_date"] == DataType.DATE

    def test_schema_inference_mixed_numeric(self, numeric_csv):
        """Test that mixed int/float promotes to FLOAT."""
        reader = CSVReader(numeric_csv)
        schema = reader.get_schema()

        assert schema is not None
        assert schema["id"] == DataType.INTEGER
        assert schema["value"] == DataType.INTEGER
        assert schema["price"] == DataType.FLOAT  # Mixed int/float -> FLOAT

    def test_schema_inference_with_nulls(self, null_csv):
        """Test schema inference with NULL values."""
        reader = CSVReader(null_csv)
        schema = reader.get_schema()

        assert schema is not None
        assert schema["name"] == DataType.STRING
        assert schema["age"] == DataType.INTEGER  # Inferred from non-null values
        assert schema["city"] == DataType.STRING

    def test_schema_inference_empty_file(self, empty_csv):
        """Test schema inference on empty file."""
        reader = CSVReader(empty_csv)
        schema = reader.get_schema()

        # Empty file should return None
        assert schema is None

    def test_schema_inference_sample_size(self, sample_csv):
        """Test that sample_size parameter works."""
        reader = CSVReader(sample_csv)

        # Sample only 1 row
        schema1 = reader.get_schema(sample_size=1)
        assert schema1 is not None
        assert len(schema1) == 5

        # Sample all rows
        schema_all = reader.get_schema(sample_size=100)
        assert schema_all is not None
        assert len(schema_all) == 5

        # Types should be the same
        assert schema1["name"] == schema_all["name"]
        assert schema1["age"] == schema_all["age"]

    def test_schema_get_column_names(self, sample_csv):
        """Test getting column names from schema."""
        reader = CSVReader(sample_csv)
        schema = reader.get_schema()

        assert schema is not None
        column_names = schema.get_column_names()
        assert column_names == ["name", "age", "salary", "active", "hire_date"]

    def test_schema_validate_column(self, sample_csv):
        """Test validating column existence."""
        reader = CSVReader(sample_csv)
        schema = reader.get_schema()

        assert schema is not None

        # Should not raise
        schema.validate_column("name")
        schema.validate_column("age")

        # Should raise
        with pytest.raises(ValueError, match="Column 'invalid' not found"):
            schema.validate_column("invalid")

    def test_schema_get_column_type(self, sample_csv):
        """Test getting column type."""
        reader = CSVReader(sample_csv)
        schema = reader.get_schema()

        assert schema is not None
        assert schema.get_column_type("name") == DataType.STRING
        assert schema.get_column_type("age") == DataType.INTEGER
        assert schema.get_column_type("invalid") is None


class TestSchemaIntegration:
    """Test schema usage in query execution."""

    def test_schema_in_query_context(self, sample_csv):
        """Test that schema can be used for query validation."""
        reader = CSVReader(sample_csv)
        schema = reader.get_schema()

        assert schema is not None

        # Simulate validating a SELECT query
        select_columns = ["name", "age"]
        for col in select_columns:
            schema.validate_column(col)  # Should not raise

        # Simulate invalid column
        with pytest.raises(ValueError):
            schema.validate_column("invalid_column")

    def test_schema_type_checking(self, sample_csv):
        """Test that schema can be used for type checking."""
        reader = CSVReader(sample_csv)
        schema = reader.get_schema()

        assert schema is not None

        # Check if age column is numeric
        assert schema["age"].is_numeric()

        # Check if salary column is numeric
        assert schema["salary"].is_numeric()

        # Check if name column is not numeric
        assert not schema["name"].is_numeric()

    def test_schema_type_compatibility(self, sample_csv):
        """Test checking type compatibility for comparisons."""
        reader = CSVReader(sample_csv)
        schema = reader.get_schema()

        assert schema is not None

        # Numeric types are comparable
        assert schema["age"].is_comparable(schema["salary"])

        # String and numeric are not comparable
        assert not schema["name"].is_comparable(schema["age"])

        # Same types are comparable
        assert schema["name"].is_comparable(schema["name"])
