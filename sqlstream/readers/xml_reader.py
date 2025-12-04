"""
XML Reader - Parse and query structured data from XML files

Parses XML files and extracts tabular data from repeating elements.
Supports XPath-like selection for specific elements.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any, Iterator

from sqlstream.core.types import DataType, Schema
from sqlstream.readers.base import BaseReader
from sqlstream.sql.ast_nodes import Condition


class XMLReader(BaseReader):
    """
    Read tabular data from XML files

    Extracts tabular data from XML by finding repeating elements.
    Each repeating element becomes a row, and child elements/attributes become columns.

    Example XML:
        <data>
            <record>
                <name>Alice</name>
                <age>30</age>
                <city>New York</city>
            </record>
            <record>
                <name>Bob</name>
                <age>25</age>
                <city>San Francisco</city>
            </record>
        </data>

    Example:
        # Query all <record> elements
        reader = XMLReader("data.xml", element="record")

        # Query with XPath-like syntax
        reader = XMLReader("data.xml", element="data/record")
    """

    def __init__(self, source: str, element: str | None = None, **kwargs):
        """
        Initialize XML reader

        Args:
            source: Path to XML file
            element: Element tag name or path to extract (e.g., "record" or "data/record")
                    If not provided, will try to find the first repeating element
            **kwargs: Additional arguments (reserved for future use)
        """
        self.source = source
        self.element = element
        self.kwargs = kwargs

        # Parse XML and extract records
        self._parse_xml()

        # Filter conditions and columns
        self.filter_conditions: list[Condition] = []
        self.required_columns: list[str] = []

    def _parse_xml(self) -> None:
        """Parse XML file and extract tabular data"""
        try:
            tree = ET.parse(self.source)
            root = tree.getroot()

            # Find the elements to extract
            if self.element:
                # User specified element path
                elements = root.findall(f".//{self.element}")
                if not elements:
                    # Try exact path without //
                    elements = root.findall(self.element)
                if not elements:
                    raise ValueError(
                        f"No elements found matching '{self.element}' in XML: {self.source}"
                    )
            else:
                # Auto-detect: find first repeating element
                elements = self._find_repeating_elements(root)
                if not elements:
                    raise ValueError(
                        f"No repeating elements found in XML: {self.source}. "
                        "Specify element parameter explicitly."
                    )

            # Extract data from elements
            self.rows = []
            self.columns = set()

            for elem in elements:
                row = self._element_to_dict(elem)
                self.rows.append(row)
                self.columns.update(row.keys())

            # Convert columns to sorted list for consistent ordering
            self.columns = sorted(self.columns)

            if not self.rows:
                raise ValueError(f"No data rows extracted from XML: {self.source}")

        except ET.ParseError as e:
            raise OSError(f"Failed to parse XML file {self.source}: {e}") from e
        except FileNotFoundError as e:
            raise OSError(f"XML file not found: {self.source}") from e

    def _find_repeating_elements(self, root: ET.Element) -> list[ET.Element]:
        """
        Find the first type of repeating element in XML

        Returns:
            List of elements that repeat (more than one with same tag)
        """
        # Count occurrences of each tag at each level
        tag_counts = {}

        # Check direct children first
        for child in root:
            tag = child.tag
            if tag not in tag_counts:
                tag_counts[tag] = []
            tag_counts[tag].append(child)

        # Return the first tag that has multiple occurrences
        for _, elements in tag_counts.items():
            if len(elements) > 1:
                return elements

        # If no repeating elements at root level, search deeper
        for child in root:
            result = self._find_repeating_elements(child)
            if result:
                return result

        return []

    def _element_to_dict(self, elem: ET.Element) -> dict[str, Any]:
        """
        Convert an XML element to a dictionary

        Extracts:
        - Attributes as columns (prefixed with '@')
        - Child element text as columns
        - Nested elements as dot-notation columns (e.g., 'address.city')

        Args:
            elem: XML element to convert

        Returns:
            Dictionary representation of the element
        """
        row = {}

        # Add attributes (prefixed with @)
        for attr_name, attr_value in elem.attrib.items():
            row[f"@{attr_name}"] = self._infer_type(attr_value)

        # Add child elements
        for child in elem:
            tag = child.tag

            # If child has children, it's a nested structure
            if len(child) > 0:
                # Create nested dict with dot notation
                nested = self._element_to_dict(child)
                for key, value in nested.items():
                    row[f"{tag}.{key}"] = value
            else:
                # Simple text element
                text = child.text
                if text and text.strip():
                    row[tag] = self._infer_type(text.strip())
                else:
                    row[tag] = None

        # If element has text and no children, add it with special key
        if elem.text and elem.text.strip() and len(elem) == 0:
            row["_text"] = self._infer_type(elem.text.strip())

        return row

    def _infer_type(self, value: str) -> Any:
        """Infer and convert value to appropriate type"""
        from sqlstream.core.types import infer_type_from_string

        return infer_type_from_string(value)

    def read_lazy(self) -> Iterator[dict[str, Any]]:
        """Read data lazily from parsed XML"""
        for row in self.rows:
            # Apply filters if any
            if self.filter_conditions:
                if not self._matches_filters(row):
                    continue

            # Apply column selection if any
            if self.required_columns:
                # Ensure all columns exist in row (fill with None if missing)
                filtered_row = {k: row.get(k) for k in self.required_columns}
                yield filtered_row
            else:
                # Ensure all columns exist in row (fill with None if missing)
                complete_row = {col: row.get(col) for col in self.columns}
                yield complete_row

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
        """Get schema by inferring types from all rows"""
        schema = {}

        from sqlstream.core.types import infer_common_type

        for col in self.columns:
            # Collect all non-None values for this column
            values = [row[col] for row in self.rows if col in row and row[col] is not None]

            if not values:
                schema[col] = DataType.STRING
                continue

            # Infer common type from all values
            schema[col] = infer_common_type(values)

        return Schema(schema)

    def supports_pushdown(self) -> bool:
        """XML reader supports filter pushdown"""
        return True

    def supports_column_selection(self) -> bool:
        """XML reader supports column selection"""
        return True

    def set_filter(self, conditions: list[Condition]) -> None:
        """Set filter conditions"""
        self.filter_conditions = conditions

    def set_columns(self, columns: list[str]) -> None:
        """Set required columns"""
        self.required_columns = columns

    def to_dataframe(self):
        """
        Convert to pandas DataFrame
        """
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError("Pandas is required for to_dataframe()") from e

        # Ensure all rows have all columns
        complete_rows = [{col: row.get(col) for col in self.columns} for row in self.rows]

        return pd.DataFrame(complete_rows)
