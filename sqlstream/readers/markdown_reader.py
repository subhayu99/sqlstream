"""
Markdown Reader - Parse and query tables from Markdown files

Parses GitHub Flavored Markdown tables and makes them queryable.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from typing import Any

from sqlstream.core.types import DataType, Schema
from sqlstream.readers.base import BaseReader
from sqlstream.sql.ast_nodes import Condition


class MarkdownReader(BaseReader):
    """
    Read tables from Markdown files

    Parses Markdown tables (GFM format) and allows querying them.
    Supports files with multiple tables.

    Example Markdown table:
        | Name    | Age | City          |
        |:--------|----:|--------------:|
        | Alice   | 30  | New York      |
        | Bob     | 25  | San Francisco |

    Example:
        reader = MarkdownReader("data.md")
        for row in reader.read_lazy():
            print(row)
    """

    def __init__(
        self,
        source: str,
        table: int = 0,
    ):
        """
        Initialize Markdown reader

        Args:
            source: Path to Markdown file
            table: Which table to read if multiple tables exist (0-indexed)
        """
        self.source = source
        self.table = table

        # Parse tables from Markdown
        self._parse_markdown()

        # Filter conditions and columns
        self.filter_conditions: list[Condition] = []
        self.required_columns: list[str] = []

    def _parse_markdown(self) -> None:
        """Parse all tables from Markdown file"""
        # Read file content
        with open(self.source, encoding="utf-8") as f:
            content = f.read()

        # Find all tables
        self.tables = self._extract_tables(content)

        if not self.tables:
            raise ValueError(f"No tables found in Markdown file: {self.source}")

        if self.table >= len(self.tables):
            raise ValueError(
                f"Table index {self.table} out of range. "
                f"Markdown contains {len(self.tables)} table(s)."
            )

        # Select the table to work with
        self.data = self.tables[self.table]
        self.columns = self.data["columns"]
        self.rows = self.data["rows"]

    def _extract_tables(self, content: str) -> list[dict[str, Any]]:
        """
        Extract all tables from Markdown content

        Returns:
            List of table dicts with 'columns' and 'rows' keys
        """
        tables = []
        lines = content.split("\n")

        i = 0
        while i < len(lines):
            # Check if this line looks like a table header
            line = lines[i].strip()

            if line.startswith("|") and "|" in line[1:]:
                # Potential table start
                # Check if next line is separator
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if self._is_separator_line(next_line):
                        # Found a table!
                        table = self._parse_table(lines, i)
                        if table:
                            tables.append(table)
                        # Skip past this table
                        i += table.get("line_count", 2)
                        continue

            i += 1

        return tables

    def _is_separator_line(self, line: str) -> bool:
        """Check if line is a table separator (e.g., |:---|---:|)"""
        if not line.startswith("|"):
            return False

        # Remove outer pipes and split
        parts = line.strip("|").split("|")

        # Check if all parts match separator pattern
        # Separators can be: ---, :---, ---:, :---:
        separator_pattern = re.compile(r"^:?-+:?$")

        return all(separator_pattern.match(p.strip()) for p in parts if p.strip())

    def _parse_table(self, lines: list[str], start_idx: int) -> dict[str, Any] | None:
        """Parse a single table starting at the given index"""
        # Parse header
        header_line = lines[start_idx].strip()
        columns = self._parse_row(header_line, infer_types=False)

        if not columns:
            return None

        # Skip separator line
        rows = []
        i = start_idx + 2  # Skip header and separator

        # Parse data rows
        while i < len(lines):
            line = lines[i].strip()

            # Stop if we hit an empty line or non-table content
            if not line or not line.startswith("|"):
                break

            # Skip if it's another separator (shouldn't happen in valid markdown)
            if self._is_separator_line(line):
                i += 1
                continue

            # Parse row
            row_values = self._parse_row(line)
            if row_values:
                # Ensure row has same number of columns as header
                # Pad with None if needed
                while len(row_values) < len(columns):
                    row_values.append(None)

                # Create row dict
                row_dict = {columns[j]: row_values[j] for j in range(len(columns))}
                rows.append(row_dict)

            i += 1

        return {"columns": columns, "rows": rows, "line_count": i - start_idx}

    def _parse_row(self, line: str, infer_types: bool = True) -> list[Any]:
        """Parse a single table row"""
        # Remove leading/trailing pipes and whitespace
        line = line.strip("|").strip()

        # Split by pipes, handling escaped pipes
        parts = []
        current = ""
        escaped = False

        for char in line:
            if char == "\\" and not escaped:
                escaped = True
                continue
            elif char == "|" and not escaped:
                parts.append(current.strip())
                current = ""
            else:
                if escaped:
                    current += "\\"
                    escaped = False
                current += char

        # Add last part
        if current or line.endswith("|"):
            parts.append(current.strip())

        # Clean up values
        cleaned = []
        for part in parts:
            # Convert empty strings and common null representations to None
            if not part or part.lower() in ("null", "none", "n/a", "-"):
                cleaned.append(None)
            else:
                # Try to infer types if requested
                if infer_types:
                    cleaned.append(self._infer_type(part))
                else:
                    cleaned.append(part)

        return cleaned

    def _infer_type(self, value: str) -> Any:
        """Infer and convert value to appropriate type"""
        from sqlstream.core.types import infer_type_from_string

        return infer_type_from_string(value)

    def read_lazy(self) -> Iterator[dict[str, Any]]:
        """Read data lazily from the selected table"""
        for row in self.rows:
            # Apply filters if any
            if self.filter_conditions:
                if not self._matches_filters(row):
                    continue

            # Apply column selection if any
            if self.required_columns:
                filtered_row = {k: v for k, v in row.items() if k in self.required_columns}
                yield filtered_row
            else:
                yield row

    def _matches_filters(self, row: dict[str, Any]) -> bool:
        """Check if row matches all filter conditions"""
        for condition in self.filter_conditions:
            col = condition.column
            op = condition.operator
            value = condition.value

            if col not in row:
                return False

            row_value = row[col]

            # Handle None values
            if row_value is None:
                return False

            # Apply operator
            if op == "=":
                if row_value != value:
                    return False
            elif op == ">":
                if row_value <= value:
                    return False
            elif op == "<":
                if row_value >= value:
                    return False
            elif op == ">=":
                if row_value < value:
                    return False
            elif op == "<=":
                if row_value > value:
                    return False
            elif op == "!=":
                if row_value == value:
                    return False

        return True

    def get_schema(self) -> Schema:
        """Get schema by inferring types from first few rows"""
        schema = {}

        # Sample first few rows to infer types
        sample_size = min(10, len(self.rows))

        from sqlstream.core.types import infer_type

        for col in self.columns:
            # Collect non-None values
            values = [row[col] for row in self.rows[:sample_size] if row.get(col) is not None]

            if not values:
                schema[col] = DataType.STRING
                continue

            # Infer type from values
            # Use the first non-null value to infer type, or check consistency
            # For simplicity, we'll use the first value's type, but we could be more robust
            first_val = values[0]
            schema[col] = infer_type(first_val)

        return Schema(schema)

    def supports_pushdown(self) -> bool:
        """Markdown reader supports filter pushdown"""
        return True

    def supports_column_selection(self) -> bool:
        """Markdown reader supports column selection"""
        return True

    def set_filter(self, conditions: list[Condition]) -> None:
        """Set filter conditions"""
        self.filter_conditions = conditions

    def set_columns(self, columns: list[str]) -> None:
        """Set required columns"""
        self.required_columns = columns

    def list_tables(self) -> list[str]:
        """
        List all tables found in the Markdown file

        Returns:
            List of table descriptions
        """
        descriptions = []
        for i, table in enumerate(self.tables):
            cols = table["columns"][:3]
            col_str = ", ".join(cols)
            if len(table["columns"]) > 3:
                col_str += ", ..."
            row_count = len(table["rows"])
            descriptions.append(f"Table {i}: {col_str} ({row_count} rows)")
        return descriptions

    def to_dataframe(self):
        """
        Convert to pandas DataFrame
        """
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError("Pandas is required for to_dataframe()") from e

        return pd.DataFrame(self.rows)
