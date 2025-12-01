"""
CSV Reader with lazy evaluation and type inference

Uses Python's built-in csv module for simplicity and zero dependencies.
"""

import csv
import warnings
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from sqlstream.readers.base import BaseReader
from sqlstream.sql.ast_nodes import Condition
from sqlstream.core.types import Schema


class CSVReader(BaseReader):
    """
    Lazy CSV reader with basic type inference

    Features:
    - Lazy iteration (doesn't load entire file into memory)
    - Automatic type inference (int, float, string)
    - Predicate pushdown support
    - Column pruning support
    """

    def __init__(self, path: str, encoding: str = "utf-8", delimiter: str = ","):
        """
        Initialize CSV reader

        Args:
            path: Path to CSV file (local or s3://)
            encoding: File encoding (default: utf-8)
            delimiter: CSV delimiter (default: comma)
        """
        self.path_str = path
        self.is_s3 = path.startswith("s3://")
        if not self.is_s3:
            self.path = Path(path)
        else:
            self.path = None  # type: ignore

        self.encoding = encoding
        self.delimiter = delimiter

        # For optimization (set by query optimizer)
        self.filter_conditions: List[Condition] = []
        self.required_columns: List[str] = []
        self.limit: Optional[int] = None

        if not self.is_s3 and not self.path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")

    def supports_pushdown(self) -> bool:
        """CSV reader supports predicate pushdown"""
        return True

    def supports_column_selection(self) -> bool:
        """CSV reader supports column pruning"""
        return True

    def supports_limit(self) -> bool:
        """CSV reader supports limit pushdown"""
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

    def _get_file_handle(self):
        """Get file handle for reading (local or S3)."""
        if self.is_s3:
            try:
                import s3fs
                fs = s3fs.S3FileSystem(anon=False)
                return fs.open(self.path_str, mode="r", encoding=self.encoding)
            except ImportError:
                raise ImportError("s3fs is required for S3 support. Install with: pip install sqlstream[s3]")
        else:
            return open(self.path, encoding=self.encoding, newline="")

    def read_lazy(self) -> Iterator[Dict[str, Any]]:
        """
        Lazy iterator over CSV rows

        Yields rows as dictionaries with type inference applied.
        If filters are set, applies them during iteration.
        If columns are set, only yields those columns.
        If limit is set, stops after yielding that many rows.
        """
        with self._get_file_handle() as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            rows_yielded = 0

            for row_num, raw_row in enumerate(reader, start=2):  # Start at 2 (after header)
                try:
                    # Apply type inference
                    row = self._infer_types(raw_row)

                    # Apply filters if set (predicate pushdown)
                    if self.filter_conditions:
                        if not self._matches_filter(row):
                            continue

                    # Apply column selection if set (column pruning)
                    if self.required_columns:
                        row = {k: v for k, v in row.items() if k in self.required_columns}

                    yield row
                    rows_yielded += 1

                    # Early termination if limit reached (limit pushdown)
                    if self.limit is not None and rows_yielded >= self.limit:
                        break

                except Exception as e:
                    # Handle malformed rows gracefully
                    warnings.warn(
                        f"Skipping malformed row {row_num} in {self.path}: {e}",
                        UserWarning,
                    )
                    continue

    def _infer_types(self, row: Dict[str, str]) -> Dict[str, Any]:
        """
        Infer types for all values in a row

        Tries to convert strings to int, then float, otherwise keeps as string.

        Args:
            row: Dictionary with string values

        Returns:
            Dictionary with inferred types
        """
        typed_row = {}

        for key, value in row.items():
            # Handle empty strings as None
            if value == "" or value is None:
                typed_row[key] = None
                continue

            typed_row[key] = self._infer_value_type(value)

        return typed_row

    def _infer_value_type(self, value: str) -> Any:
        """
        Infer type of a single value

        Args:
            value: String value from CSV

        Returns:
            Value converted to int, float, or kept as string
        """
        value = value.strip()

        # Try integer
        try:
            return int(value)
        except ValueError:
            pass

        # Try float
        try:
            return float(value)
        except ValueError:
            pass

        # Keep as string
        return value

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
                # Unknown operator, skip this condition
                warnings.warn(f"Unknown operator: {op}", UserWarning)
                return True

        except TypeError:
            # Type mismatch (e.g., comparing string to int)
            # This is fine - row just doesn't match
            return False

    def get_schema(self, sample_size: int = 100) -> Optional[Schema]:
        """
        Infer schema by sampling rows from the CSV file

        Args:
            sample_size: Number of rows to sample for type inference (default: 100)

        Returns:
            Schema object with inferred types, or None if file is empty
        """
        sample_rows = []

        with self._get_file_handle() as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)

            # Read sample rows to infer types
            try:
                for i, raw_row in enumerate(reader):
                    if i >= sample_size:
                        break
                    typed_row = self._infer_types(raw_row)
                    sample_rows.append(typed_row)

            except StopIteration:
                # Empty file or fewer rows than sample_size
                pass

        if not sample_rows:
            return None

        return Schema.from_rows(sample_rows)

    def to_dataframe(self):
        """
        Convert to pandas DataFrame efficiently
        """
        import pandas as pd

        # Use pandas read_csv for performance
        if self.is_s3:
            # For S3, use s3fs via storage_options or direct s3 path if supported
            # Since we already handle s3fs import in _get_file_handle, we can rely on pandas s3 support
            # which uses s3fs under the hood
            return pd.read_csv(
                self.path_str,
                encoding=self.encoding,
                delimiter=self.delimiter,
                storage_options={"anon": False}
            )
        else:
            return pd.read_csv(
                self.path,
                encoding=self.encoding,
                delimiter=self.delimiter
            )
