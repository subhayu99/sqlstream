"""Type system for SQLStream.

This module provides type definitions, inference, and validation for query execution.
"""

import json
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any


class DataType(Enum):
    """SQL data types supported by SQLStream."""

    # Numeric types
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    DECIMAL = "DECIMAL"

    # String types
    STRING = "STRING"
    JSON = "JSON"

    # Boolean
    BOOLEAN = "BOOLEAN"

    # Temporal types
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"

    # Special
    NULL = "NULL"

    def __str__(self) -> str:
        return self.value

    def is_numeric(self) -> bool:
        """Check if type is numeric (INTEGER, FLOAT, or DECIMAL)."""
        return self in (DataType.INTEGER, DataType.FLOAT, DataType.DECIMAL)

    def is_temporal(self) -> bool:
        """Check if type is temporal (DATE, TIME, or DATETIME)."""
        return self in (DataType.DATE, DataType.TIME, DataType.DATETIME)

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

        # Temporal types are comparable with each other
        if self.is_temporal() and other.is_temporal():
            return True

        # Strings and JSON are not comparable with numbers or temporals
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

        # Numeric coercion hierarchy: INTEGER < FLOAT < DECIMAL
        numeric_hierarchy = {
            DataType.INTEGER: 1,
            DataType.FLOAT: 2,
            DataType.DECIMAL: 3,
        }
        if self in numeric_hierarchy and other in numeric_hierarchy:
            # Return higher precedence type
            return self if numeric_hierarchy[self] > numeric_hierarchy[other] else other

        # Temporal coercion: DATE/TIME -> DATETIME
        if self == DataType.DATETIME or other == DataType.DATETIME:
            if self.is_temporal() and other.is_temporal():
                return DataType.DATETIME

        # JSON coercion
        if self == DataType.JSON and other == DataType.JSON:
            return DataType.JSON
        if self == DataType.JSON or other == DataType.JSON:
            return DataType.STRING  # Fallback to string

        # Otherwise, no coercion possible - return STRING as fallback
        return DataType.STRING


def parse_datetime(value: str) -> datetime | None:
    """Try to parse datetime from string using multiple formats.

    Args:
        value: String to parse

    Returns:
        datetime object if successful, None otherwise
    """
    if not isinstance(value, str):
        return None

    value = value.strip()

    # Try ISO 8601 with timezone (handle Z and +HH:MM)
    if "T" in value:
        # Remove timezone suffix for basic parsing
        base_value = value.replace("Z", "").split("+")[0].split("-")[0:3]
        base_value = "-".join(base_value) if len(base_value) == 3 else value.replace("Z", "")

    formats = [
        "%Y-%m-%dT%H:%M:%S",  # ISO 8601: 2024-01-15T10:30:00
        "%Y-%m-%dT%H:%M:%S.%f",  # ISO with microseconds
        "%Y-%m-%d %H:%M:%S",  # SQL format: 2024-01-15 10:30:00
        "%Y-%m-%d %H:%M:%S.%f",  # SQL with microseconds
        "%d/%m/%Y %H:%M:%S",  # EU format: 15/01/2024 10:30:00
        "%m/%d/%Y %H:%M:%S",  # US format: 01/15/2024 10:30:00
        "%Y%m%d%H%M%S",  # Compact: 20240115103000
        "%Y-%m-%d %H:%M",  # Without seconds
        "%d/%m/%Y %H:%M",  # EU without seconds
        "%m/%d/%Y %H:%M",  # US without seconds
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    return None


def parse_date(value: str) -> date | None:
    """Try to parse date from string using multiple formats.

    Args:
        value: String to parse

    Returns:
        date object if successful, None otherwise
    """
    if not isinstance(value, str):
        return None

    value = value.strip()

    formats = [
        "%Y-%m-%d",  # ISO: 2024-01-15
        "%d/%m/%Y",  # EU: 15/01/2024
        "%m/%d/%Y",  # US: 01/15/2024
        "%Y%m%d",  # Compact: 20240115
        "%d-%m-%Y",  # EU with dashes: 15-01-2024
        "%m-%d-%Y",  # US with dashes: 01-15-2024
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue

    return None


def parse_time(value: str) -> time | None:
    """Try to parse time from string using multiple formats.

    Args:
        value: String to parse

    Returns:
        time object if successful, None otherwise
    """
    if not isinstance(value, str):
        return None

    value = value.strip()

    formats = [
        "%H:%M:%S",  # 24-hour: 14:30:00
        "%H:%M:%S.%f",  # With microseconds: 14:30:00.123456
        "%H:%M",  # Without seconds: 14:30
        "%I:%M:%S %p",  # 12-hour with seconds: 02:30:00 PM
        "%I:%M %p",  # 12-hour: 02:30 PM
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).time()
        except ValueError:
            continue

    return None


def is_json_string(value: str) -> bool:
    """Check if a string contains valid JSON (object or array).

    Args:
        value: String to check

    Returns:
        True if valid JSON object/array, False otherwise
    """
    if not isinstance(value, str):
        return False

    value = value.strip()

    # Must start with { or [
    if not (value.startswith("{") or value.startswith("[")):
        return False

    # Try to parse as JSON
    try:
        parsed = json.loads(value)
        # Must be dict or list (not just a string, number, etc.)
        return isinstance(parsed, (dict, list))
    except (json.JSONDecodeError, ValueError):
        return False


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
        >>> infer_type(Decimal("19.99"))
        DataType.DECIMAL
    """
    # 1. NULL check
    if value is None:
        return DataType.NULL

    # 2. Python type checks (non-string)
    if isinstance(value, bool):
        return DataType.BOOLEAN

    if isinstance(value, int):
        return DataType.INTEGER

    if isinstance(value, float):
        return DataType.FLOAT

    if isinstance(value, Decimal):
        return DataType.DECIMAL

    if isinstance(value, datetime):
        return DataType.DATETIME

    if isinstance(value, date):
        return DataType.DATE

    if isinstance(value, time):
        return DataType.TIME

    # 3. String value inference
    if isinstance(value, str):
        value_stripped = value.strip()

        # Empty string → NULL
        if not value_stripped:
            return DataType.NULL

        # Boolean literals
        if value_stripped.lower() in ("true", "false"):
            return DataType.BOOLEAN

        # JSON (must check early - before numeric)
        if is_json_string(value_stripped):
            return DataType.JSON

        # Integer
        try:
            int(value_stripped)
            return DataType.INTEGER
        except ValueError:
            pass

        # Float or Decimal
        try:
            # Check if it has decimal point
            if "." in value_stripped:
                # Check precision - if more than 6 decimal places, use DECIMAL
                decimal_part = value_stripped.split(".")[1]
                # Remove trailing zeros for check
                significant_decimals = decimal_part.rstrip("0")
                if len(significant_decimals) > 6:
                    Decimal(value_stripped)  # Validate
                    return DataType.DECIMAL
            float(value_stripped)
            return DataType.FLOAT
        except (ValueError, InvalidOperation):
            pass

        # DateTime (check before Date to catch timestamps)
        dt = parse_datetime(value_stripped)
        if dt is not None:
            return DataType.DATETIME

        # Date
        d = parse_date(value_stripped)
        if d is not None:
            return DataType.DATE

        # Time
        t = parse_time(value_stripped)
        if t is not None:
            return DataType.TIME

        # Default to STRING
        return DataType.STRING

    # 4. Fallback for unknown types
    return DataType.STRING


def infer_type_from_string(value: str) -> Any:
    """Parse a string value and return the typed Python value.

    This is used by readers to convert string data into proper Python types.

    Args:
        value: String value to parse

    Returns:
        Typed Python value (int, float, Decimal, datetime, bool, str, etc.)

    Examples:
        >>> infer_type_from_string("42")
        42
        >>> infer_type_from_string("3.14159265358979")
        Decimal("3.14159265358979")
        >>> infer_type_from_string("2024-01-15 10:30:00")
        datetime(2024, 1, 15, 10, 30, 0)
    """
    if not isinstance(value, str):
        return value

    value_stripped = value.strip()

    # Empty → None
    if not value_stripped:
        return None

    # Boolean
    if value_stripped.lower() == "true":
        return True
    if value_stripped.lower() == "false":
        return False

    # JSON - keep as string for now (DuckDB will handle it)
    if is_json_string(value_stripped):
        return value_stripped

    # Integer
    try:
        return int(value_stripped)
    except ValueError:
        pass

    # Float or Decimal
    try:
        if "." in value_stripped:
            decimal_part = value_stripped.split(".")[1]
            significant_decimals = decimal_part.rstrip("0")
            if len(significant_decimals) > 6:
                return Decimal(value_stripped)
        return float(value_stripped)
    except (ValueError, InvalidOperation):
        pass

    # DateTime
    dt = parse_datetime(value_stripped)
    if dt is not None:
        return dt

    # Date
    d = parse_date(value_stripped)
    if d is not None:
        return d

    # Time
    t = parse_time(value_stripped)
    if t is not None:
        return t

    # Return as string
    return value


def infer_common_type(values: list[Any]) -> DataType:
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

    def __init__(self, columns: dict[str, DataType]):
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

    def get_column_names(self) -> list[str]:
        """Get list of column names."""
        return list(self.columns.keys())

    def get_column_type(self, column: str) -> DataType | None:
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
    def from_row(row: dict[str, Any]) -> "Schema":
        """Infer schema from a single row.

        Args:
            row: Dictionary representing a row

        Returns:
            Inferred Schema
        """
        columns = {name: infer_type(value) for name, value in row.items()}
        return Schema(columns)

    @staticmethod
    def from_rows(rows: list[dict[str, Any]]) -> "Schema":
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
                merged[col_name] = self.columns[col_name].coerce_to(other.columns[col_name])
            elif col_name in self.columns:
                # Column only in self
                merged[col_name] = self.columns[col_name]
            else:
                # Column only in other
                merged[col_name] = other.columns[col_name]

        return Schema(merged)

    def to_dict(self) -> dict[str, Any]:
        """Convert schema to dictionary."""
        return {name: dtype.value for name, dtype in self.columns.items()}
