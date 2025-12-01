"""
HTML Reader - Extract and query tables from HTML files and URLs

Uses pandas read_html to extract tables from HTML documents.
Supports multiple tables per file with table selection.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from sqlstream.readers.base import BaseReader
from sqlstream.sql.ast_nodes import Condition
from sqlstream.core.types import Schema, DataType

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None


class HTMLReader(BaseReader):
    """
    Read tables from HTML files or URLs

    Extracts all tables from HTML and allows querying them.
    If multiple tables exist, you can select which one to query.

    Example:
        # Query first table in HTML
        reader = HTMLReader("data.html")

        # Query specific table (0-indexed)
        reader = HTMLReader("data.html", table_index=1)

        # Query table by matching text
        reader = HTMLReader("data.html", match="Sales Data")
    """

    def __init__(
        self,
        source: str,
        table: int = 0,
        match: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize HTML reader

        Args:
            source: Path to HTML file or URL
            table: Which table to read (0-indexed, default: 0)
            match: Text to match in table (tries to find table containing this text)
            **kwargs: Additional arguments passed to pandas read_html
        """
        if not PANDAS_AVAILABLE:
            raise ImportError(
                "HTML reader requires pandas library. "
                "Install `sqlstream[pandas]`"
            )

        self.source = source
        self.table = table
        self.match = match
        self.kwargs = kwargs

        # Load tables from HTML
        self._load_tables()

        # Filter conditions and columns
        self.filter_conditions: List[Condition] = []
        self.required_columns: List[str] = []

    def _load_tables(self) -> None:
        """Load all tables from HTML source"""
        try:
            # read_html returns a list of DataFrames
            match_pattern = self.match if self.match else ".+"
            self.tables = pd.read_html(
                self.source,
                match=match_pattern,
                **self.kwargs
            )

            if not self.tables:
                raise ValueError(f"No tables found in HTML: {self.source}")

            # Select the table to work with
            if self.table >= len(self.tables):
                raise ValueError(
                    f"Table index {self.table} out of range. "
                    f"HTML contains {len(self.tables)} table(s)."
                )

            self.df = self.tables[self.table]

            # Clean column names (convert to strings, handle duplicates)
            self.df.columns = [str(col) for col in self.df.columns]

        except ValueError:
            # Re-raise ValueError for validation errors
            raise
        except Exception as e:
            # Only wrap actual I/O errors
            raise IOError(f"Failed to read HTML tables from {self.source}: {e}")

    def read_lazy(self) -> Iterator[Dict[str, Any]]:
        """Read data lazily from the selected table"""
        df = self.df

        # Apply filters if any
        if self.filter_conditions:
            df = self._apply_filters(df)

        # Apply column selection if any
        if self.required_columns:
            available_cols = [c for c in self.required_columns if c in df.columns]
            if available_cols:
                df = df[available_cols]

        # Yield rows as dictionaries
        yield from df.to_dict('records')

    def _apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply filter conditions to DataFrame"""
        mask = pd.Series([True] * len(df), index=df.index)

        for condition in self.filter_conditions:
            col = condition.column
            op = condition.operator
            value = condition.value

            if col not in df.columns:
                continue

            # Build condition mask
            if op == "=":
                mask &= df[col] == value
            elif op == ">":
                mask &= df[col] > value
            elif op == "<":
                mask &= df[col] < value
            elif op == ">=":
                mask &= df[col] >= value
            elif op == "<=":
                mask &= df[col] <= value
            elif op == "!=":
                mask &= df[col] != value

        return df[mask]

    def get_schema(self) -> Schema:
        """Get schema from the selected table"""

        schema = {}
        for col in self.df.columns:
            dtype = str(self.df[col].dtype)
            # Map pandas dtypes to SQL-like types
            if dtype.startswith('int'):
                schema[col] = DataType.INTEGER
            elif dtype.startswith('float'):
                schema[col] = DataType.FLOAT
            elif dtype == 'bool':
                schema[col] = DataType.BOOLEAN
            else:
                schema[col] = DataType.STRING
        return Schema(schema)

    def supports_pushdown(self) -> bool:
        """HTML reader supports filter pushdown"""
        return True

    def supports_column_selection(self) -> bool:
        """HTML reader supports column selection"""
        return True

    def set_filter(self, conditions: List[Condition]) -> None:
        """Set filter conditions"""
        self.filter_conditions = conditions

    def set_columns(self, columns: List[str]) -> None:
        """Set required columns"""
        self.required_columns = columns

    def list_tables(self) -> List[str]:
        """
        List all tables found in the HTML

        Returns:
            List of table descriptions (first few column names)
        """
        descriptions = []
        for i, table in enumerate(self.tables):
            cols = list(table.columns)[:3]  # First 3 columns
            col_str = ", ".join(str(c) for c in cols)
            if len(table.columns) > 3:
                col_str += ", ..."
            descriptions.append(f"Table {i}: {col_str} ({len(table)} rows)")
        return descriptions

    def to_dataframe(self):
        """
        Convert to pandas DataFrame efficiently
        """
        # HTMLReader already holds data as a DataFrame
        return self.df
