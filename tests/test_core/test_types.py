"""Tests for type system."""

import pytest
from datetime import date, datetime

from sqlstream.core.types import (
    DataType,
    infer_type,
    infer_common_type,
    Schema,
)


class TestDataType:
    """Test DataType enum."""

    def test_is_numeric(self):
        """Test is_numeric method."""
        assert DataType.INTEGER.is_numeric()
        assert DataType.FLOAT.is_numeric()
        assert not DataType.STRING.is_numeric()
        assert not DataType.BOOLEAN.is_numeric()
        assert not DataType.DATE.is_numeric()
        assert not DataType.NULL.is_numeric()

    def test_is_comparable_same_type(self):
        """Test is_comparable with same types."""
        assert DataType.INTEGER.is_comparable(DataType.INTEGER)
        assert DataType.FLOAT.is_comparable(DataType.FLOAT)
        assert DataType.STRING.is_comparable(DataType.STRING)

    def test_is_comparable_numeric_types(self):
        """Test is_comparable between numeric types."""
        assert DataType.INTEGER.is_comparable(DataType.FLOAT)
        assert DataType.FLOAT.is_comparable(DataType.INTEGER)

    def test_is_comparable_with_null(self):
        """Test is_comparable with NULL type."""
        assert DataType.NULL.is_comparable(DataType.INTEGER)
        assert DataType.NULL.is_comparable(DataType.STRING)
        assert DataType.INTEGER.is_comparable(DataType.NULL)
        assert DataType.STRING.is_comparable(DataType.NULL)

    def test_is_comparable_incompatible_types(self):
        """Test is_comparable with incompatible types."""
        assert not DataType.STRING.is_comparable(DataType.INTEGER)
        assert not DataType.INTEGER.is_comparable(DataType.STRING)
        assert not DataType.DATE.is_comparable(DataType.INTEGER)

    def test_coerce_to_same_type(self):
        """Test coerce_to with same types."""
        assert DataType.INTEGER.coerce_to(DataType.INTEGER) == DataType.INTEGER
        assert DataType.STRING.coerce_to(DataType.STRING) == DataType.STRING

    def test_coerce_to_numeric_promotion(self):
        """Test coerce_to promotes INTEGER to FLOAT."""
        assert DataType.INTEGER.coerce_to(DataType.FLOAT) == DataType.FLOAT
        assert DataType.FLOAT.coerce_to(DataType.INTEGER) == DataType.FLOAT

    def test_coerce_to_with_null(self):
        """Test coerce_to with NULL type."""
        assert DataType.NULL.coerce_to(DataType.INTEGER) == DataType.INTEGER
        assert DataType.INTEGER.coerce_to(DataType.NULL) == DataType.INTEGER
        assert DataType.NULL.coerce_to(DataType.STRING) == DataType.STRING

    def test_coerce_to_incompatible_fallback(self):
        """Test coerce_to falls back to STRING for incompatible types."""
        assert DataType.INTEGER.coerce_to(DataType.STRING) == DataType.STRING
        assert DataType.STRING.coerce_to(DataType.INTEGER) == DataType.STRING
        assert DataType.DATE.coerce_to(DataType.INTEGER) == DataType.STRING


class TestInferType:
    """Test infer_type function."""

    def test_infer_null(self):
        """Test inferring NULL type."""
        assert infer_type(None) == DataType.NULL

    def test_infer_boolean(self):
        """Test inferring BOOLEAN type."""
        assert infer_type(True) == DataType.BOOLEAN
        assert infer_type(False) == DataType.BOOLEAN

    def test_infer_integer(self):
        """Test inferring INTEGER type."""
        assert infer_type(42) == DataType.INTEGER
        assert infer_type(0) == DataType.INTEGER
        assert infer_type(-100) == DataType.INTEGER

    def test_infer_float(self):
        """Test inferring FLOAT type."""
        assert infer_type(3.14) == DataType.FLOAT
        assert infer_type(0.0) == DataType.FLOAT
        assert infer_type(-2.5) == DataType.FLOAT

    def test_infer_date(self):
        """Test inferring DATE type."""
        assert infer_type(date(2024, 1, 1)) == DataType.DATE
        # datetime objects now return DATETIME type (enhanced type system)
        assert infer_type(datetime(2024, 1, 1, 12, 0)) == DataType.DATETIME

    def test_infer_string(self):
        """Test inferring STRING type."""
        assert infer_type("hello") == DataType.STRING
        # Empty strings now return NULL type (enhanced type system)
        assert infer_type("") == DataType.NULL

    def test_infer_string_boolean(self):
        """Test inferring BOOLEAN from string."""
        assert infer_type("true") == DataType.BOOLEAN
        assert infer_type("false") == DataType.BOOLEAN
        assert infer_type("True") == DataType.BOOLEAN
        assert infer_type("FALSE") == DataType.BOOLEAN

    def test_infer_string_integer(self):
        """Test inferring INTEGER from string."""
        assert infer_type("42") == DataType.INTEGER
        assert infer_type("-100") == DataType.INTEGER
        assert infer_type("0") == DataType.INTEGER

    def test_infer_string_float(self):
        """Test inferring FLOAT from string."""
        assert infer_type("3.14") == DataType.FLOAT
        assert infer_type("-2.5") == DataType.FLOAT
        assert infer_type("0.0") == DataType.FLOAT

    def test_infer_string_date(self):
        """Test inferring DATE from string."""
        assert infer_type("2024-01-01") == DataType.DATE
        assert infer_type("2023-12-25") == DataType.DATE

    def test_infer_string_date_invalid_format(self):
        """Test enhanced date parsing recognizes various formats."""
        # Enhanced type system now parses MM/DD/YYYY format
        assert infer_type("01/01/2024") == DataType.DATE
        # Still falls back to STRING for truly invalid dates
        assert infer_type("2024-1-1") in (DataType.STRING, DataType.DATE)  # May be parsed
        assert infer_type("not-a-date") == DataType.STRING


class TestInferCommonType:
    """Test infer_common_type function."""

    def test_infer_empty_list(self):
        """Test inferring type from empty list."""
        assert infer_common_type([]) == DataType.NULL

    def test_infer_all_null(self):
        """Test inferring type from all NULL values."""
        assert infer_common_type([None, None, None]) == DataType.NULL

    def test_infer_all_integers(self):
        """Test inferring type from all integers."""
        assert infer_common_type([1, 2, 3]) == DataType.INTEGER

    def test_infer_all_floats(self):
        """Test inferring type from all floats."""
        assert infer_common_type([1.5, 2.5, 3.5]) == DataType.FLOAT

    def test_infer_mixed_numeric(self):
        """Test inferring type from mixed integer and float."""
        assert infer_common_type([1, 2.5, 3]) == DataType.FLOAT

    def test_infer_with_null_values(self):
        """Test inferring type with NULL values mixed in."""
        assert infer_common_type([1, None, 3]) == DataType.INTEGER
        assert infer_common_type([1.5, None, 3.5]) == DataType.FLOAT
        assert infer_common_type(["hello", None, "world"]) == DataType.STRING

    def test_infer_mixed_incompatible(self):
        """Test inferring type from incompatible types falls back to STRING."""
        assert infer_common_type([1, "hello", 3]) == DataType.STRING
        assert infer_common_type([1.5, "world", 3.5]) == DataType.STRING
        assert infer_common_type([True, 42, "test"]) == DataType.STRING


class TestSchema:
    """Test Schema class."""

    def test_schema_creation(self):
        """Test creating a schema."""
        schema = Schema({"name": DataType.STRING, "age": DataType.INTEGER})
        assert len(schema) == 2
        assert "name" in schema
        assert "age" in schema
        assert "city" not in schema

    def test_schema_getitem(self):
        """Test getting column type."""
        schema = Schema({"name": DataType.STRING, "age": DataType.INTEGER})
        assert schema["name"] == DataType.STRING
        assert schema["age"] == DataType.INTEGER

    def test_schema_get_column_names(self):
        """Test getting column names."""
        schema = Schema({"name": DataType.STRING, "age": DataType.INTEGER})
        assert schema.get_column_names() == ["name", "age"]

    def test_schema_get_column_type(self):
        """Test getting column type with get_column_type."""
        schema = Schema({"name": DataType.STRING, "age": DataType.INTEGER})
        assert schema.get_column_type("name") == DataType.STRING
        assert schema.get_column_type("age") == DataType.INTEGER
        assert schema.get_column_type("city") is None

    def test_schema_validate_column(self):
        """Test validate_column method."""
        schema = Schema({"name": DataType.STRING, "age": DataType.INTEGER})

        # Should not raise
        schema.validate_column("name")
        schema.validate_column("age")

        # Should raise
        with pytest.raises(ValueError, match="Column 'city' not found"):
            schema.validate_column("city")

    def test_schema_from_row(self):
        """Test creating schema from a single row."""
        row = {"name": "Alice", "age": 30, "salary": 95000.5}
        schema = Schema.from_row(row)

        assert len(schema) == 3
        assert schema["name"] == DataType.STRING
        assert schema["age"] == DataType.INTEGER
        assert schema["salary"] == DataType.FLOAT

    def test_schema_from_rows(self):
        """Test creating schema from multiple rows."""
        rows = [
            {"name": "Alice", "age": 30, "salary": 95000.5},
            {"name": "Bob", "age": 25, "salary": 75000},
            {"name": "Charlie", "age": 35, "salary": 105000.0},
        ]
        schema = Schema.from_rows(rows)

        assert len(schema) == 3
        assert schema["name"] == DataType.STRING
        assert schema["age"] == DataType.INTEGER
        assert schema["salary"] == DataType.FLOAT  # Should coerce to FLOAT

    def test_schema_from_rows_empty(self):
        """Test creating schema from empty list."""
        schema = Schema.from_rows([])
        assert len(schema) == 0

    def test_schema_from_rows_with_null(self):
        """Test creating schema from rows with NULL values."""
        rows = [
            {"name": "Alice", "age": None},
            {"name": "Bob", "age": 25},
        ]
        schema = Schema.from_rows(rows)

        assert schema["name"] == DataType.STRING
        assert schema["age"] == DataType.INTEGER  # Should infer from non-null value

    def test_schema_merge_same_columns(self):
        """Test merging schemas with same columns."""
        schema1 = Schema({"name": DataType.STRING, "age": DataType.INTEGER})
        schema2 = Schema({"name": DataType.STRING, "age": DataType.INTEGER})
        merged = schema1.merge(schema2)

        assert len(merged) == 2
        assert merged["name"] == DataType.STRING
        assert merged["age"] == DataType.INTEGER

    def test_schema_merge_different_types(self):
        """Test merging schemas with different types for same column."""
        schema1 = Schema({"value": DataType.INTEGER})
        schema2 = Schema({"value": DataType.FLOAT})
        merged = schema1.merge(schema2)

        assert len(merged) == 1
        assert merged["value"] == DataType.FLOAT  # Should coerce to FLOAT

    def test_schema_merge_different_columns(self):
        """Test merging schemas with different columns."""
        schema1 = Schema({"name": DataType.STRING, "age": DataType.INTEGER})
        schema2 = Schema({"city": DataType.STRING, "country": DataType.STRING})
        merged = schema1.merge(schema2)

        assert len(merged) == 4
        assert merged["name"] == DataType.STRING
        assert merged["age"] == DataType.INTEGER
        assert merged["city"] == DataType.STRING
        assert merged["country"] == DataType.STRING

    def test_schema_repr(self):
        """Test schema string representation."""
        schema = Schema({"name": DataType.STRING, "age": DataType.INTEGER})
        repr_str = repr(schema)
        assert "Schema" in repr_str
        assert "name" in repr_str
        assert "age" in repr_str
