"""
Parquet Reader with intelligent row group pruning

This reader uses Parquet metadata (row group statistics) to skip
entire row groups without reading them - a massive performance win!

The magic happens in row group selection using min/max statistics.
"""

from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import pyarrow.parquet as pq

from sqlstream.core.types import DataType, Schema
from sqlstream.readers.base import BaseReader
from sqlstream.sql.ast_nodes import Condition


class ParquetReader(BaseReader):
    """
    Intelligent Parquet reader with statistics-based optimization

    Features:
    - Lazy iteration (doesn't load entire file)
    - Row group statistics-based pruning (HUGE performance win)
    - Column selection (only read needed columns)
    - Predicate pushdown with statistics

    The key insight: Parquet stores min/max for each column in each row group.
    We can skip entire row groups if their statistics don't match our filters!

    Example:
        Row Group 1: age [18-30], city ['LA', 'NYC']
        Row Group 2: age [31-45], city ['NYC', 'SF']
        Row Group 3: age [46-90], city ['LA', 'SF']

        Query: WHERE age > 60
        → Skip RG1 (max=30), Skip RG2 (max=45), Read RG3 only!
    """

    def __init__(self, path: str):
        """
        Initialize Parquet reader

        Args:
            path: Path to Parquet file (local or s3://)
        """
        self.path_str = path
        self.is_s3 = path.startswith("s3://")

        filesystem = None
        path_to_open = path

        if self.is_s3:
            try:
                import s3fs
                filesystem = s3fs.S3FileSystem(anon=False)
                # s3fs expects path without protocol when filesystem is provided
                path_to_open = path.replace("s3://", "")
            except ImportError:
                raise ImportError("s3fs is required for S3 support. Install `sqlstream[s3]`")
        else:
            self.path = Path(path)
            path_to_open = str(self.path)
            if not self.path.exists():
                raise FileNotFoundError(f"Parquet file not found: {path}")

        self.parquet_file = pq.ParquetFile(path_to_open, filesystem=filesystem)

        # Optimization state (set by planner)
        self.filter_conditions: List[Condition] = []
        self.required_columns: List[str] = []
        self.limit: Optional[int] = None
        self.partition_filters: List[Condition] = []

        # Parse partition information from path
        self.partition_columns: set = set()
        self.partition_values: Dict[str, Any] = {}
        self._parse_partition_info()

        # Check if file should be skipped based on partition filters
        self.partition_pruned = False

        # Statistics tracking
        self.total_row_groups = self.parquet_file.num_row_groups
        self.row_groups_scanned = 0

    def supports_pushdown(self) -> bool:
        """Parquet reader supports predicate pushdown"""
        return True

    def supports_column_selection(self) -> bool:
        """Parquet reader supports column pruning"""
        return True

    def supports_limit(self) -> bool:
        """Parquet reader supports limit pushdown"""
        return True

    def set_filter(self, conditions: List[Condition]) -> None:
        """Set filter conditions for pushdown"""
        self.filter_conditions = conditions

    def set_columns(self, columns: List[str]) -> None:
        """Set required columns for pruning"""
        self.required_columns = columns

    def set_limit(self, limit: int) -> None:
        """Set maximum rows to read for early termination"""
        self.limit = limit

    def supports_partition_pruning(self) -> bool:
        """Parquet reader supports partition pruning for Hive-style partitioning"""
        return True

    def get_partition_columns(self) -> set:
        """Get partition column names detected from file path"""
        return self.partition_columns

    def set_partition_filters(self, conditions: List[Condition]) -> None:
        """
        Set partition filters and check if this file should be skipped

        Args:
            conditions: List of WHERE conditions on partition columns
        """
        self.partition_filters = conditions

        # Check if this file's partitions match the filters
        # If not, mark it as pruned so we skip reading it
        if not self._partition_matches_filters():
            self.partition_pruned = True

    def read_lazy(self) -> Iterator[Dict[str, Any]]:
        """
        Lazy iterator over Parquet rows with intelligent row group pruning

        This is where the magic happens:
        1. Check partition pruning (skip entire file if needed!)
        2. Select row groups using statistics (skip irrelevant ones!)
        3. Read only selected row groups
        4. Read only required columns
        5. Yield rows as dictionaries
        6. Early termination if limit is reached
        """
        # Step 0: Partition pruning - skip entire file if partition doesn't match
        if self.partition_pruned:
            # File has been pruned based on partition filters
            # Don't read any data!
            return

        # Step 1: Intelligent row group selection
        selected_row_groups = self._select_row_groups_with_statistics()

        # Track how many we're actually reading
        self.row_groups_scanned = len(selected_row_groups)

        # Step 2: Read only selected row groups
        rows_yielded = 0
        for rg_idx in selected_row_groups:
            # Read this row group (with column selection)
            for row in self._read_row_group(rg_idx):
                # Add partition columns to the row
                # These are "virtual" columns from the directory structure
                for col, value in self.partition_values.items():
                    row[col] = value

                yield row
                rows_yielded += 1

                # Early termination if limit reached
                if self.limit is not None and rows_yielded >= self.limit:
                    return

    def _select_row_groups_with_statistics(self) -> List[int]:
        """
        Use row group statistics to select which ones to read

        This is THE key optimization for Parquet!

        Returns:
            List of row group indices to read
        """
        if not self.filter_conditions:
            # No filters, read all row groups
            return list(range(self.total_row_groups))

        selected = []
        metadata = self.parquet_file.metadata

        for rg_idx in range(self.total_row_groups):
            rg_metadata = metadata.row_group(rg_idx)

            # Check if this row group's statistics match our filters
            if self._row_group_matches_filters(rg_metadata):
                selected.append(rg_idx)

        return selected

    def _row_group_matches_filters(self, rg_metadata) -> bool:
        """
        Check if row group statistics overlap with filter conditions

        Uses min/max statistics to determine if a row group could
        possibly contain matching rows.

        Args:
            rg_metadata: Row group metadata from Parquet file

        Returns:
            True if row group might contain matching rows
            False if we can definitively skip it

        Example:
            Filter: age > 60
            Row Group: age [18-55]
            Result: False (max < 60, so no rows can match)
        """
        for condition in self.filter_conditions:
            column_name = condition.column

            # Find column index
            try:
                # Get schema to find column index
                schema = self.parquet_file.schema_arrow
                column_idx = schema.get_field_index(column_name)
            except Exception:
                # Column not found or no index, can't use statistics
                continue

            # Get column metadata
            try:
                col_metadata = rg_metadata.column(column_idx)

                # Check if statistics are available
                if not col_metadata.is_stats_set:
                    continue

                stats = col_metadata.statistics

                # Get min/max values
                min_val = stats.min
                max_val = stats.max

                # Check if filter can eliminate this row group
                if not self._statistics_match_condition(
                    min_val, max_val, condition
                ):
                    return False  # Skip this row group!

            except Exception:
                # No statistics or error, conservatively keep row group
                continue

        return True  # Row group might contain matches

    def _statistics_match_condition(
        self, min_val: Any, max_val: Any, condition: Condition
    ) -> bool:
        """
        Check if min/max statistics overlap with a condition

        Args:
            min_val: Minimum value in row group
            max_val: Maximum value in row group
            condition: Filter condition to check

        Returns:
            True if row group might contain matches
            False if we can skip it

        Logic:
            age > 60: Skip if max_val <= 60
            age < 30: Skip if min_val >= 30
            age = 25: Skip if 25 < min_val or 25 > max_val
            age >= 50: Skip if max_val < 50
            age <= 40: Skip if min_val > 40
        """
        op = condition.operator
        value = condition.value

        try:
            if op == ">":
                # Skip if max_val <= value (all rows too small)
                return max_val > value

            elif op == ">=":
                # Skip if max_val < value
                return max_val >= value

            elif op == "<":
                # Skip if min_val >= value (all rows too large)
                return min_val < value

            elif op == "<=":
                # Skip if min_val > value
                return min_val <= value

            elif op == "=":
                # Skip if value outside [min_val, max_val]
                return min_val <= value <= max_val

            elif op == "!=":
                # Can only skip if min_val == max_val == value
                # (entire row group is the excluded value)
                if min_val == max_val == value:
                    return False
                return True

            else:
                # Unknown operator, conservatively keep row group
                return True

        except (TypeError, ValueError):
            # Comparison failed (type mismatch), keep row group
            return True

    def _read_row_group(self, rg_idx: int) -> Iterator[Dict[str, Any]]:
        """
        Read a specific row group

        Args:
            rg_idx: Row group index to read

        Yields:
            Rows as dictionaries
        """
        # Determine which columns to read
        # If we have filters, we need to read those columns even if not in required_columns
        columns_to_read = set()

        if self.required_columns:
            columns_to_read.update(self.required_columns)

        # Add columns needed for filtering
        if self.filter_conditions:
            for condition in self.filter_conditions:
                columns_to_read.add(condition.column)

        # Convert to list, or None to read all columns
        columns = list(columns_to_read) if columns_to_read else None

        # Read row group with column selection
        table = self.parquet_file.read_row_group(rg_idx, columns=columns)

        # Convert to row-oriented format and yield
        # PyArrow returns columnar data, we need rows
        num_rows = table.num_rows

        for i in range(num_rows):
            row = {}
            for col_name in table.column_names:
                col_data = table.column(col_name)
                # Get value at index i
                value = col_data[i].as_py()  # Convert to Python type
                row[col_name] = value

            # Apply filter conditions if set
            # Note: Row group statistics only help us skip entire groups,
            # but we still need to filter individual rows within selected groups
            if self.filter_conditions:
                if not self._matches_filter(row):
                    continue

            # Apply column selection to output
            # (we may have read extra columns for filtering)
            if self.required_columns:
                row = {k: v for k, v in row.items() if k in self.required_columns}

            yield row

    def _matches_filter(self, row: Dict[str, Any]) -> bool:
        """
        Check if row matches all filter conditions

        Args:
            row: Row to check

        Returns:
            True if row matches all conditions (AND logic)
        """
        for condition in self.filter_conditions:
            if not self._evaluate_condition(row, condition):
                return False
        return True

    def _evaluate_condition(self, row: Dict[str, Any], condition: Condition) -> bool:
        """
        Evaluate a single condition against a row

        Args:
            row: Row to check
            condition: Condition to evaluate

        Returns:
            True if condition is satisfied
        """
        # Get column value
        if condition.column not in row:
            return False

        value = row[condition.column]

        # Handle NULL values
        if value is None:
            return False

        # Evaluate operator
        op = condition.operator
        expected = condition.value

        try:
            if op == "=":
                return value == expected
            elif op == ">":
                return value > expected
            elif op == "<":
                return value < expected
            elif op == ">=":
                return value >= expected
            elif op == "<=":
                return value <= expected
            elif op == "!=":
                return value != expected
            else:
                # Unknown operator, conservatively keep row
                return True

        except TypeError:
            # Type mismatch (e.g., comparing string to int)
            # This is fine - row just doesn't match
            return False

    def get_schema(self) -> Schema:
        """
        Get schema from Parquet metadata

        Returns:
            Dictionary mapping column names to types
        """
        schema: Dict[str, DataType] = {}
        arrow_schema = self.parquet_file.schema_arrow

        for i in range(len(arrow_schema)):
            field = arrow_schema.field(i)
            # Map Arrow types to simple type names
            schema[field.name] = self._arrow_type_to_dtype(field.type)

        return Schema(schema)

    def _arrow_type_to_string(self, arrow_type) -> str:
        """
        Convert PyArrow type to simple string

        Args:
            arrow_type: PyArrow data type

        Returns:
            Simple type name (int, float, string, etc.)
        """
        type_str = str(arrow_type)

        if "int" in type_str.lower():
            return "int"
        elif "float" in type_str.lower() or "double" in type_str.lower():
            return "float"
        elif "decimal" in type_str.lower():
            return "decimal"
        elif "string" in type_str.lower() or "utf8" in type_str.lower():
            return "string"
        elif "bool" in type_str.lower():
            return "bool"
        elif "date" in type_str.lower():
            return "date"
        elif "timestamp" in type_str.lower():
            return "datetime"
        elif "time" in type_str.lower():
            return "time"
        else:
            return type_str

    def _arrow_type_to_dtype(self, arrow_type) -> DataType:
        """
        Convert PyArrow type to SQLStream data type

        Args:
            arrow_type: PyArrow data type

        Returns:
            SQLStream data type
        """
        simple_type = self._arrow_type_to_string(arrow_type)
        if simple_type == "int":
            return DataType.INTEGER
        elif simple_type == "float":
            return DataType.FLOAT
        elif simple_type == "decimal":
            return DataType.DECIMAL
        elif simple_type == "string":
            return DataType.STRING
        elif simple_type == "bool":
            return DataType.BOOLEAN
        elif simple_type == "date":
            return DataType.DATE
        elif simple_type == "datetime":
            return DataType.DATETIME
        elif simple_type == "time":
            return DataType.TIME
        else:
            return DataType.STRING  # Default to string instead of NULL for unknown types

    def _parse_partition_info(self) -> None:
        """
        Parse partition information from Hive-style partitioned path

        Detects partition columns and values from path structure:
        - s3://bucket/data/year=2024/month=01/data.parquet
        - /path/to/data/country=USA/state=CA/data.parquet

        Populates:
        - self.partition_columns: {'year', 'month'} or {'country', 'state'}
        - self.partition_values: {'year': 2024, 'month': 1} or {'country': 'USA', 'state': 'CA'}
        """
        import re

        # Parse the path string for partition key=value patterns
        # Match pattern: name=value in directory structure
        partition_pattern = re.compile(r'([^/=]+)=([^/]+)')

        matches = partition_pattern.findall(self.path_str)

        for key, value in matches:
            self.partition_columns.add(key)

            # Try to infer type of partition value
            # Common patterns: year=2024 (int), month=01 (int), country=USA (str)
            typed_value = self._infer_partition_value_type(value)
            self.partition_values[key] = typed_value

    def _infer_partition_value_type(self, value: str) -> Any:
        """
        Infer the type of a partition value string

        Args:
            value: String value from partition path (e.g., "2024", "01", "USA")

        Returns:
            Typed value (int, float, or str)
        """
        from sqlstream.core.types import infer_type_from_string
        return infer_type_from_string(value)

    def _partition_matches_filters(self) -> bool:
        """
        Check if this file's partition values match the partition filters

        Returns:
            True if partition matches (file should be read)
            False if partition doesn't match (file should be skipped)

        Example:
            File path: s3://bucket/data/year=2024/month=01/data.parquet
            Partition values: {'year': 2024, 'month': 1}

            Filter: year > 2023 AND month = 1
            Result: True (matches)

            Filter: year = 2025
            Result: False (doesn't match, skip file!)
        """
        if not self.partition_filters:
            # No filters, all partitions match
            return True

        # Check each partition filter condition
        for condition in self.partition_filters:
            column = condition.column
            operator = condition.operator
            expected = condition.value

            # Get partition value for this column
            if column not in self.partition_values:
                # Filter references a partition column that doesn't exist in path
                # Conservatively assume match (don't skip)
                continue

            actual = self.partition_values[column]

            # Evaluate condition
            if not self._evaluate_partition_condition(actual, operator, expected):
                # Condition failed, skip this file!
                return False

        # All conditions passed
        return True

    def _evaluate_partition_condition(
        self, actual: Any, operator: str, expected: Any
    ) -> bool:
        """
        Evaluate a partition filter condition

        Args:
            actual: Actual partition value from path
            operator: Comparison operator (=, >, <, etc.)
            expected: Expected value from WHERE clause

        Returns:
            True if condition is satisfied, False otherwise
        """
        try:
            if operator == "=":
                return actual == expected
            elif operator == ">":
                return actual > expected
            elif operator == "<":
                return actual < expected
            elif operator == ">=":
                return actual >= expected
            elif operator == "<=":
                return actual <= expected
            elif operator == "!=":
                return actual != expected
            else:
                # Unknown operator, conservatively match
                return True

        except (TypeError, ValueError):
            # Type mismatch, conservatively match
            return True

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about row group pruning

        Returns:
            Dictionary with pruning statistics
        """
        return {
            "total_row_groups": self.total_row_groups,
            "row_groups_scanned": self.row_groups_scanned,
            "row_groups_skipped": self.total_row_groups - self.row_groups_scanned,
            "pruning_ratio": (
                (self.total_row_groups - self.row_groups_scanned)
                / self.total_row_groups
                if self.total_row_groups > 0
                else 0
            ),
            "partition_pruned": self.partition_pruned,
            "partition_columns": list(self.partition_columns),
            "partition_values": self.partition_values,
        }

    def to_dataframe(self):
        """
        Convert to pandas DataFrame efficiently
        """
        import pandas as pd

        # Use pandas read_parquet for performance
        if self.is_s3:
            return pd.read_parquet(
                self.path_str,
                storage_options={"anon": False}
            )
        else:
            return pd.read_parquet(self.path)
