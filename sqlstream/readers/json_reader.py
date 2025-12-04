"""
JSON Reader for reading standard JSON files
"""

import json
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from sqlstream.core.types import Schema
from sqlstream.readers.base import BaseReader
from sqlstream.sql.ast_nodes import Condition


class JSONReader(BaseReader):
    """
    Reader for standard JSON files.

    Supports:
    - Array of objects: [{"a": 1}, {"a": 2}]
    - Object with records key: {"data": [{"a": 1}, ...], "meta": ...}
    - Automatic type inference
    - Predicate pushdown (filtering in Python)
    - Column pruning
    """

    def __init__(self, path: str, records_key: Optional[str] = None, encoding: str = "utf-8"):
        """
        Initialize JSON reader

        Args:
            path: Path to JSON file
            records_key: Key containing the list of records (e.g., "data", "records").
                        If None, attempts to auto-detect or expects root to be a list.
            encoding: File encoding (default: utf-8)
        """
        self.path_str = path
        self.is_s3 = path.startswith("s3://")
        if not self.is_s3:
            self.path = Path(path)
        else:
            self.path = None

        self.records_key = records_key
        self.encoding = encoding

        # Optimization flags
        self.filter_conditions: List[Condition] = []
        self.required_columns: List[str] = []
        self.limit: Optional[int] = None

        if not self.is_s3 and not self.path.exists():
            raise FileNotFoundError(f"JSON file not found: {path}")

    def supports_pushdown(self) -> bool:
        return True

    def supports_column_selection(self) -> bool:
        return True

    def supports_limit(self) -> bool:
        return True

    def set_filter(self, conditions: List[Condition]) -> None:
        self.filter_conditions = conditions

    def set_columns(self, columns: List[str]) -> None:
        self.required_columns = columns

    def set_limit(self, limit: int) -> None:
        self.limit = limit

    def _get_file_handle(self):
        """Get file handle for reading (local or S3)."""
        if self.is_s3:
            try:
                import s3fs

                fs = s3fs.S3FileSystem(anon=False)
                return fs.open(self.path_str, mode="r", encoding=self.encoding)
            except ImportError as e:
                raise ImportError(
                    "s3fs is required for S3 support. Install with: pip install sqlstream[s3]"
                ) from e
        else:
            return open(self.path, encoding=self.encoding)

    def read_lazy(self) -> Iterator[Dict[str, Any]]:
        """
        Read JSON file and yield records.

        Note: Standard JSON parsing loads the whole file into memory.
        For large files, use JSONL format.
        """
        with self._get_file_handle() as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON file {self.path_str}: {e}") from e

        # Locate records
        records = self._locate_records(data)

        rows_yielded = 0
        for row in records:
            if not isinstance(row, dict):
                continue

            # Apply filters
            if self.filter_conditions:
                if not self._matches_filter(row):
                    continue

            # Apply column selection
            if self.required_columns:
                row = {k: row.get(k) for k in self.required_columns}

            yield row
            rows_yielded += 1

            if self.limit is not None and rows_yielded >= self.limit:
                break

    def _locate_records(self, data: Any) -> List[Dict[str, Any]]:
        """
        Find the list of records in the JSON data.

        Supports JSONPath-like syntax:
        - "key" - simple key access
        - "key.nested" - nested object access
        - "key[0]" - array index access
        - "key[]" - flatten/merge arrays from all elements
        - "key[].nested" - nested access after flattening
        """
        # If no records_key specified, use auto-detection
        if not self.records_key:
            return self._auto_detect_records(data)

        # Navigate using the path
        result = self._navigate_path(data, self.records_key)

        # Ensure result is a list of dicts
        if isinstance(result, list):
            return result
        elif isinstance(result, dict):
            return [result]
        else:
            raise ValueError(f"Path '{self.records_key}' did not resolve to a list or object")

    def _auto_detect_records(self, data: Any) -> List[Dict[str, Any]]:
        """Auto-detect records when no path is specified"""
        # If root is a list, that's our data
        if isinstance(data, list):
            return data

        # If root is a dict, look for common keys
        if isinstance(data, dict):
            common_keys = ["data", "records", "items", "rows", "results"]
            for key in common_keys:
                if key in data and isinstance(data[key], list):
                    return data[key]

            # Look for any list value
            for _, value in data.items():
                if isinstance(value, list) and len(value) > 0:
                    return value

            # If single object, treat as one-row table
            return [data]

        raise ValueError("JSON content must be a list or an object containing a list")

    def _navigate_path(self, data: Any, path: str) -> Any:
        """
        Navigate through JSON using a path string.

        Supports:
        - "key" - simple key
        - "key.nested.deep" - dot notation
        - "key[0]" - array indexing
        - "key[]" - flatten arrays
        - "key[].nested" - combinations

        Examples:
        - "users" → data["users"]
        - "result.orders" → data["result"]["orders"]
        - "users[0]" → data["users"][0]
        - "users[].transactions" → flatten [user["transactions"] for user in data["users"]]
        """
        import re

        # Handle flattening first if [] is in the path
        if "[]" in path:
            return self._flatten_path(data, path)

        current = data

        # Split path into segments (handling dots and brackets)
        # Pattern matches: "key", "key[0]", "key[1]" etc.
        segments = re.findall(r"([^.\[]+)(\[\d+\])?", path)

        for key, bracket in segments:
            if not key:
                continue

            # Access key in current dict
            if isinstance(current, dict):
                if key not in current:
                    raise ValueError(f"Key '{key}' not found in JSON")
                current = current[key]
            else:
                raise ValueError(f"Cannot access key '{key}' on non-dict type")

            # Handle bracket notation (numeric index only)
            if bracket:
                # Extract index [0], [1], etc.
                index = int(bracket[1:-1])  # Remove [ and ]
                if not isinstance(current, list):
                    raise ValueError(f"Cannot index non-list at '{key}'")
                if index < 0 or index >= len(current):
                    raise ValueError(f"Index {index} out of range for '{key}'")
                current = current[index]

        return current

    def _flatten_path(self, data: Any, path: str) -> List[Any]:
        """
        Handle flattening for paths with [].

        Examples:
        - "users[]" → flatten data["users"]
        - "users[].transactions" → flatten [user["transactions"] for user in data["users"]]
        """
        # Split on [] to find the flattening point
        parts = path.split("[]")

        if len(parts) != 2:
            raise ValueError("Only one '[]' operator is supported per path")

        before_flatten = parts[0]
        after_flatten = parts[1].lstrip(".")  # Remove leading dot if present

        # Navigate to the array to flatten (without [] in the path)
        if before_flatten:
            # Use simple navigation without []
            array = self._navigate_simple(data, before_flatten)
        else:
            array = data

        if not isinstance(array, list):
            raise ValueError(f"Cannot flatten non-list at '{before_flatten}'")

        # Flatten and optionally extract nested keys
        result = []
        for item in array:
            if after_flatten:
                # Navigate to nested key in each item
                try:
                    nested = self._navigate_simple(item, after_flatten)
                    if isinstance(nested, list):
                        result.extend(nested)
                    else:
                        result.append(nested)
                except (ValueError, KeyError):
                    # Skip items that don't have the nested key
                    continue
            else:
                # Just flatten the array
                if isinstance(item, dict):
                    result.append(item)

        return result

    def _navigate_simple(self, data: Any, path: str) -> Any:
        """Navigate a simple path without [] operators"""
        import re

        current = data
        segments = re.findall(r"([^.\[]+)(\[\d+\])?", path)

        for key, bracket in segments:
            if not key:
                continue

            if isinstance(current, dict):
                if key not in current:
                    raise ValueError(f"Key '{key}' not found in JSON")
                current = current[key]
            else:
                raise ValueError(f"Cannot access key '{key}' on non-dict type")

            if bracket:
                index = int(bracket[1:-1])
                if not isinstance(current, list):
                    raise ValueError(f"Cannot index non-list at '{key}'")
                if index < 0 or index >= len(current):
                    raise ValueError(f"Index {index} out of range for '{key}'")
                current = current[index]

        return current

    def _matches_filter(self, row: Dict[str, Any]) -> bool:
        """Check if row matches filter conditions"""
        for condition in self.filter_conditions:
            if not self._evaluate_condition(row, condition):
                return False
        return True

    def _evaluate_condition(self, row: Dict[str, Any], condition: Condition) -> bool:
        """Evaluate single condition"""
        if condition.column not in row:
            return False

        value = row[condition.column]
        if value is None:
            return False

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
                return True
        except TypeError:
            return False

    def get_schema(self) -> Optional[Schema]:
        """Infer schema from data"""
        # We have to load the file to get schema
        try:
            # Get first few rows
            rows = []
            iterator = self.read_lazy()
            for _ in range(100):
                try:
                    rows.append(next(iterator))
                except StopIteration:
                    break

            if not rows:
                return None

            return Schema.from_rows(rows)
        except Exception:
            return None
