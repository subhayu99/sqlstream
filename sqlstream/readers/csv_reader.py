"""
CSV Reader with lazy evaluation and type inference

Uses Python's built-in csv module for simplicity and zero dependencies.
"""

import csv
import warnings
from pathlib import Path
from typing import Any, Dict, Iterator, List

from sqlstream.readers.base import BaseReader
from sqlstream.sql.ast_nodes import Condition


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
            path: Path to CSV file
            encoding: File encoding (default: utf-8)
            delimiter: CSV delimiter (default: comma)
        """
        self.path = Path(path)
        self.encoding = encoding
        self.delimiter = delimiter

        # For optimization (set by query optimizer)
        self.filter_conditions: List[Condition] = []
        self.required_columns: List[str] = []

        if not self.path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")

    def supports_pushdown(self) -> bool:
        """CSV reader supports predicate pushdown"""
        return True

    def supports_column_selection(self) -> bool:
        """CSV reader supports column pruning"""
        return True

    def set_filter(self, conditions: List[Condition]) -> None:
        """Set filter conditions for pushdown"""
        self.filter_conditions = conditions

    def set_columns(self, columns: List[str]) -> None:
        """Set required columns for pruning"""
        self.required_columns = columns

    def read_lazy(self) -> Iterator[Dict[str, Any]]:
        """
        Lazy iterator over CSV rows

        Yields rows as dictionaries with type inference applied.
        If filters are set, applies them during iteration.
        If columns are set, only yields those columns.
        """
        with open(self.path, encoding=self.encoding, newline="") as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)

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

    def get_schema(self) -> Dict[str, str]:
        """
        Infer schema by reading first row

        Returns:
            Dictionary mapping column names to inferred types
        """
        schema = {}

        with open(self.path, encoding=self.encoding, newline="") as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)

            # Read first row to infer types
            try:
                first_row = next(reader)
                typed_row = self._infer_types(first_row)

                for key, value in typed_row.items():
                    if value is None:
                        schema[key] = "unknown"
                    else:
                        schema[key] = type(value).__name__

            except StopIteration:
                # Empty file
                pass

        return schema
