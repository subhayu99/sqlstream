"""Type system for SQLStream.

This module provides type definitions, inference, and validation for query execution.
"""

from enum import Enum
from typing import Any, Optional, Dict, List
from datetime import date, datetime


class DataType(Enum):
    """SQL data types supported by SQLStream."""

    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    STRING = "STRING"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    NULL = "NULL"

    def __str__(self) -> str:
        return self.value

    def is_numeric(self) -> bool:
        """Check if type is numeric (INTEGER or FLOAT)."""
        return self in (DataType.INTEGER, DataType.FLOAT)

    def is_comparable(self, other: "DataType") -> bool:
        """Check if this type can be compared with another type."""
        # NULL can be compared with anything
        if self == DataType.NULL or other == DataType.NULL:
            return True

        # Same types are always comparable
        if self == other:
            return True

        # Numeric types are comparable with each other
        if self.is_numeric() and other.is_numeric():
            return True

        # Strings and dates are not comparable with numbers
        return False

    def coerce_to(self, other: "DataType") -> "DataType":
        """Determine the result type when coercing this type to another.

        Used for type promotion in expressions like INTEGER + FLOAT.
        """
        # NULL coerces to any type
        if self == DataType.NULL:
            return other
        if other == DataType.NULL:
            return self

        # Same type
        if self == other:
            return self

        # Numeric coercion: INT + FLOAT = FLOAT
        if self == DataType.INTEGER and other == DataType.FLOAT:
            return DataType.FLOAT
        if self == DataType.FLOAT and other == DataType.INTEGER:
            return DataType.FLOAT

        # Otherwise, no coercion possible - return STRING as fallback
        return DataType.STRING


def infer_type(value: Any) -> DataType:
    """Infer the data type from a Python value.

    Args:
        value: Python value to infer type from

    Returns:
        Inferred DataType

    Examples:
        >>> infer_type(42)
        DataType.INTEGER
        >>> infer_type(3.14)
        DataType.FLOAT
        >>> infer_type("hello")
        DataType.STRING
        >>> infer_type(None)
        DataType.NULL
    """
    if value is None:
        return DataType.NULL

    if isinstance(value, bool):
        return DataType.BOOLEAN

    if isinstance(value, int):
        return DataType.INTEGER

    if isinstance(value, float):
        return DataType.FLOAT

    if isinstance(value, (date, datetime)):
        return DataType.DATE

    if isinstance(value, str):
        # Try to infer more specific types from string values
        if value.lower() in ("true", "false"):
            return DataType.BOOLEAN

        # Try integer
        try:
            int(value)
            return DataType.INTEGER
        except ValueError:
            pass

        # Try float
        try:
            float(value)
            return DataType.FLOAT
        except ValueError:
            pass

        # Try date (ISO format YYYY-MM-DD)
        try:
            if len(value) == 10 and value[4] == "-" and value[7] == "-":
                datetime.strptime(value, "%Y-%m-%d")
                return DataType.DATE
        except ValueError:
            pass

        # Default to string
        return DataType.STRING

    # Fallback to string for unknown types
    return DataType.STRING


def infer_common_type(values: List[Any]) -> DataType:
    """Infer a common type from a list of values.

    This is useful for schema inference when reading data files.

    Args:
        values: List of values to infer type from

    Returns:
        Common DataType that can represent all values

    Examples:
        >>> infer_common_type([1, 2, 3])
        DataType.INTEGER
        >>> infer_common_type([1, 2.5, 3])
        DataType.FLOAT
        >>> infer_common_type([1, "hello", 3])
        DataType.STRING
    """
    if not values:
        return DataType.NULL

    # Filter out None values for type inference
    non_null_values = [v for v in values if v is not None]

    if not non_null_values:
        return DataType.NULL

    # Infer type of first value
    common_type = infer_type(non_null_values[0])

    # Check remaining values and coerce as needed
    for value in non_null_values[1:]:
        value_type = infer_type(value)
        common_type = common_type.coerce_to(value_type)

    return common_type


class Schema:
    """Schema definition for a table or query result.

    Holds column names and their corresponding data types.
    """

    def __init__(self, columns: Dict[str, DataType]):
        """Initialize schema.

        Args:
            columns: Dictionary mapping column names to data types
        """
        self.columns = columns

    def __getitem__(self, column: str) -> DataType:
        """Get type of a column."""
        return self.columns[column]

    def __contains__(self, column: str) -> bool:
        """Check if column exists in schema."""
        return column in self.columns

    def __len__(self) -> int:
        """Get number of columns."""
        return len(self.columns)

    def __repr__(self) -> str:
        cols = ", ".join(f"{name}: {dtype}" for name, dtype in self.columns.items())
        return f"Schema({cols})"

    def get_column_names(self) -> List[str]:
        """Get list of column names."""
        return list(self.columns.keys())

    def get_column_type(self, column: str) -> Optional[DataType]:
        """Get type of a column, or None if column doesn't exist."""
        return self.columns.get(column)

    def validate_column(self, column: str) -> None:
        """Validate that a column exists in the schema.

        Args:
            column: Column name to validate

        Raises:
            ValueError: If column doesn't exist
        """
        if column not in self.columns:
            available = ", ".join(self.columns.keys())
            raise ValueError(
                f"Column '{column}' not found in schema. Available columns: {available}"
            )

    @staticmethod
    def from_row(row: Dict[str, Any]) -> "Schema":
        """Infer schema from a single row.

        Args:
            row: Dictionary representing a row

        Returns:
            Inferred Schema
        """
        columns = {name: infer_type(value) for name, value in row.items()}
        return Schema(columns)

    @staticmethod
    def from_rows(rows: List[Dict[str, Any]]) -> "Schema":
        """Infer schema from multiple rows.

        This provides more accurate type inference by looking at multiple values.

        Args:
            rows: List of dictionaries representing rows

        Returns:
            Inferred Schema
        """
        if not rows:
            return Schema({})

        # Get all column names from first row
        column_names = list(rows[0].keys())

        # Collect values for each column
        columns = {}
        for col_name in column_names:
            col_values = [row.get(col_name) for row in rows if col_name in row]
            columns[col_name] = infer_common_type(col_values)

        return Schema(columns)

    def merge(self, other: "Schema") -> "Schema":
        """Merge two schemas, coercing types where needed.

        This is useful for operations like UNION or JOIN where schemas need to be compatible.

        Args:
            other: Another schema to merge with

        Returns:
            Merged schema with coerced types
        """
        merged = {}

        # Get all column names from both schemas
        all_columns = set(self.columns.keys()) | set(other.columns.keys())

        for col_name in all_columns:
            if col_name in self.columns and col_name in other.columns:
                # Column exists in both - coerce types
                merged[col_name] = self.columns[col_name].coerce_to(
                    other.columns[col_name]
                )
            elif col_name in self.columns:
                # Column only in self
                merged[col_name] = self.columns[col_name]
            else:
                # Column only in other
                merged[col_name] = other.columns[col_name]

        return Schema(merged)
