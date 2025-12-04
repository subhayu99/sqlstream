"""
Pandas-based Query Executor - high-performance alternative to Volcano model

Translates SQL AST directly to pandas DataFrame operations for 10-100x speedup.
Falls back gracefully if pandas is not available.
"""

from __future__ import annotations

from typing import Any, Dict, Iterator, List, Optional

try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None

from sqlstream.sql.ast_nodes import Condition, SelectStatement


class PandasExecutor:
    """
    Pandas-based executor for high-performance query execution

    Translates SQL AST to pandas operations:
    - SELECT → df[columns]
    - WHERE → df[condition]
    - GROUP BY → df.groupby().agg()
    - ORDER BY → df.sort_values()
    - JOIN → df.merge()
    - LIMIT → df.head()

    10-100x faster than pure Python Volcano model for most queries.
    """

    def __init__(self):
        """Initialize pandas executor"""
        if not PANDAS_AVAILABLE:
            raise ImportError(
                "Pandas backend requires pandas library. "
                "Install `sqlstream[pandas]`"
            )

    def execute(
        self, ast: SelectStatement, source: str, right_source: Optional[str] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Execute query using pandas

        Args:
            ast: Parsed SELECT statement
            source: Path to data file (left table)
            right_source: Optional path to right table for JOINs

        Yields:
            Result rows as dictionaries
        """
        # Step 1: Load data into DataFrame
        df = self._load_dataframe(source)

        # Step 2: Apply JOIN if present
        if ast.join:
            if not right_source:
                right_source = ast.join.right_source
            df = self._apply_join(df, ast, right_source)

        # Step 3: Apply WHERE filter
        if ast.where:
            df = self._apply_filter(df, ast.where.conditions)

        # Step 4: Apply GROUP BY + aggregation
        if ast.group_by or ast.aggregates:
            df = self._apply_groupby(df, ast)

        # Step 5: Apply ORDER BY
        if ast.order_by:
            df = self._apply_orderby(df, ast)

        # Step 6: Apply column selection (PROJECT)
        if not ast.group_by:  # GroupBy already handled columns
            df = self._apply_projection(df, ast.columns)

        # Step 7: Apply LIMIT
        if ast.limit is not None:
            df = df.head(ast.limit)

        # Step 8: Convert to dictionaries and yield
        yield from df.to_dict("records")

    def _load_dataframe(self, source: str, format: Optional[str] = None) -> pd.DataFrame:
        """
        Load data file into DataFrame

        Supports CSV, Parquet, JSON, JSONL, XML, HTML, and Markdown formats, including HTTP URLs.

        Args:
            source: Path or URL to data file (may include fragment like 'file.html#html:1')
            format: Optional explicit format (csv, parquet, json, jsonl, xml, html, markdown)
        """
        # Parse URL fragment if present (e.g., "data.html#html:1")
        from sqlstream.core.fragment_parser import parse_source_fragment
        source_path, format_hint, table_hint = parse_source_fragment(source)

        # Use format hint from fragment if not explicitly provided
        if not format and format_hint:
            format = format_hint

        # Handle HTTP URLs by using HTTPReader (for caching)
        if source_path.startswith(("http://", "https://")):
            from sqlstream.readers.http_reader import HTTPReader

            # Use HTTPReader to download/cache, then get local path
            # Pass format if specified
            if format:
                reader = HTTPReader(source_path, format=format)
            else:
                reader = HTTPReader(source_path)
            source_path = str(reader.local_path)

            # If format not specified, detect from reader
            if not format:
                delegate_type = type(reader.delegate_reader).__name__
                if 'HTML' in delegate_type:
                    format = 'html'
                elif 'Markdown' in delegate_type:
                    format = 'markdown'
                elif 'Parquet' in delegate_type:
                    format = 'parquet'
                elif 'JSON' in delegate_type and 'JSONL' not in delegate_type:
                    format = 'json'
                elif 'JSONL' in delegate_type:
                    format = 'jsonl'
                elif 'XML' in delegate_type:
                    format = 'xml'
                else:
                    format = 'csv'

        # If format explicitly specified, use it
        if format:
            if format == "parquet":
                return pd.read_parquet(source_path)
            elif format == "html":
                # read_html returns a list, take table at table_hint index (default 0)
                tables = pd.read_html(source_path)
                if not tables:
                    raise ValueError(f"No tables found in HTML: {source_path}")
                table_index = table_hint if table_hint is not None else 0
                if table_index >= len(tables):
                    raise ValueError(
                        f"Table index {table_index} out of range. "
                        f"HTML contains {len(tables)} table(s)."
                    )
                return tables[table_index]
            elif format == "markdown":
                # Use our markdown reader with table selection
                from sqlstream.readers.markdown_reader import MarkdownReader
                table_index = table_hint if table_hint is not None else 0
                reader = MarkdownReader(source_path, table=table_index)
                # Convert to DataFrame
                return pd.DataFrame(reader.rows)
            elif format == "json":
                # Use our JSON reader with records key support
                from sqlstream.readers.json_reader import JSONReader
                key = str(table_hint) if table_hint is not None else None
                reader = JSONReader(source_path, records_key=key)
                return reader.to_dataframe()
            elif format == "jsonl":
                # Use our JSONL reader
                from sqlstream.readers.jsonl_reader import JSONLReader
                reader = JSONLReader(source_path)
                return reader.to_dataframe()
            elif format == "xml":
                # Use our XML reader with element selection
                from sqlstream.readers.xml_reader import XMLReader
                element = str(table_hint) if table_hint is not None else None
                reader = XMLReader(source_path, element=element)
                return reader.to_dataframe()
            else:  # csv
                from sqlstream.readers.csv_reader import CSVReader
                return CSVReader(source_path).to_dataframe()

        # Auto-detect from extension
        source_lower = source_path.lower()
        if source_lower.endswith(".parquet"):
            return pd.read_parquet(source_path)
        elif source_lower.endswith((".html", ".htm")):
            tables = pd.read_html(source_path)
            if not tables:
                raise ValueError(f"No tables found in HTML: {source_path}")
            table_index = table_hint if table_hint is not None else 0
            if table_index >= len(tables):
                raise ValueError(
                    f"Table index {table_index} out of range. "
                    f"HTML contains {len(tables)} table(s)."
                )
            return tables[table_index]
        elif source_lower.endswith((".md", ".markdown")):
            from sqlstream.readers.markdown_reader import MarkdownReader
            table_index = table_hint if table_hint is not None else 0
            reader = MarkdownReader(source_path, table=table_index)
            return pd.DataFrame(reader.rows)
        elif source_lower.endswith(".json"):
            from sqlstream.readers.json_reader import JSONReader
            key = str(table_hint) if table_hint is not None else None
            reader = JSONReader(source_path, records_key=key)
            return reader.to_dataframe()
        elif source_lower.endswith(".jsonl"):
            from sqlstream.readers.jsonl_reader import JSONLReader
            reader = JSONLReader(source_path)
            return reader.to_dataframe()
        elif source_lower.endswith(".xml"):
            from sqlstream.readers.xml_reader import XMLReader
            element = str(table_hint) if table_hint is not None else None
            reader = XMLReader(source_path, element=element)
            return reader.to_dataframe()
        elif source_lower.endswith(".csv"):
            from sqlstream.readers.csv_reader import CSVReader
            return CSVReader(source_path).to_dataframe()
        else:
            # Try CSV as default
            try:
                from sqlstream.readers.csv_reader import CSVReader
                return CSVReader(source_path).to_dataframe()
            except Exception:
                raise ValueError(f"Unsupported file format: {source_path}")

    def _apply_join(
        self, df: pd.DataFrame, ast: SelectStatement, right_source: str
    ) -> pd.DataFrame:
        """Apply JOIN operation"""
        # Load right table
        right_df = self._load_dataframe(right_source)

        # Map SQL join types to pandas
        join_type_map = {
            "INNER": "inner",
            "LEFT": "left",
            "RIGHT": "right",
        }

        how = join_type_map.get(ast.join.join_type, "inner")

        # Perform merge
        result = df.merge(
            right_df,
            left_on=ast.join.on_left,
            right_on=ast.join.on_right,
            how=how,
            suffixes=("", "_right"),
        )

        return result

    def _apply_filter(
        self, df: pd.DataFrame, conditions: List[Condition]
    ) -> pd.DataFrame:
        """Apply WHERE conditions"""
        mask = pd.Series([True] * len(df), index=df.index)

        for condition in conditions:
            col = condition.column
            op = condition.operator
            value = condition.value

            if col not in df.columns:
                # Column doesn't exist, skip this filter
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

    def _apply_groupby(
        self, df: pd.DataFrame, ast: SelectStatement
    ) -> pd.DataFrame:
        """Apply GROUP BY with aggregations"""
        # Build aggregation dictionary and track rename mapping
        agg_dict = {}
        rename_map = {}

        for agg in ast.aggregates:
            func = agg.function.lower()
            col = agg.column

            # Determine the target alias
            alias = agg.alias if agg.alias else f"{func}_{col}"

            # Map SQL aggregate functions to pandas
            if func == "count":
                if col == "*":
                    # COUNT(*) - count any column
                    agg_col = df.columns[0]
                    agg_dict[agg_col] = "count"
                    rename_map[agg_col] = alias
                else:
                    agg_dict[col] = "count"
                    rename_map[col] = alias
            elif func == "sum":
                agg_dict[col] = "sum"
                rename_map[col] = alias
            elif func == "avg":
                agg_dict[col] = "mean"  # pandas uses 'mean' for average
                rename_map[col] = alias
            elif func == "min":
                agg_dict[col] = "min"
                rename_map[col] = alias
            elif func == "max":
                agg_dict[col] = "max"
                rename_map[col] = alias

        # Perform groupby or global aggregation
        if ast.group_by:
            grouped = df.groupby(ast.group_by, as_index=False).agg(agg_dict)
        else:
            # Global aggregation
            # Create a single-row DataFrame with aggregated values
            result = {}
            for col, func in agg_dict.items():
                if func == "count":
                    val = df[col].count()
                elif func == "sum":
                    val = df[col].sum()
                elif func == "mean":
                    val = df[col].mean()
                elif func == "min":
                    val = df[col].min()
                elif func == "max":
                    val = df[col].max()
                else:
                    val = None
                result[col] = [val]
            
            grouped = pd.DataFrame(result)

        # Rename aggregated columns to match expected output
        grouped = grouped.rename(columns=rename_map)

        # Select only the columns specified in SELECT (group_by cols + aggregates)
        result_cols = ast.group_by.copy() if ast.group_by else []
        for agg in ast.aggregates:
            alias = agg.alias if agg.alias else f"{agg.function.lower()}_{agg.column}"
            result_cols.append(alias)

        # Try to select columns, use what's available
        available_cols = [c for c in result_cols if c in grouped.columns]
        if available_cols:
            grouped = grouped[available_cols]

        return grouped

    def _apply_orderby(
        self, df: pd.DataFrame, ast: SelectStatement
    ) -> pd.DataFrame:
        """Apply ORDER BY"""
        # Build column list and ascending flags
        by_cols = []
        ascending = []

        for order_col in ast.order_by:
            by_cols.append(order_col.column)
            ascending.append(order_col.direction == "ASC")

        # Sort (na_position='last' to match SQL NULL behavior)
        return df.sort_values(by=by_cols, ascending=ascending, na_position="last")

    def _apply_projection(
        self, df: pd.DataFrame, columns: List[str]
    ) -> pd.DataFrame:
        """Apply column selection (PROJECT)"""
        if columns == ["*"]:
            return df

        # Select only available columns
        available_cols = [c for c in columns if c in df.columns]

        if not available_cols:
            # No columns match, return empty DataFrame
            return df.iloc[:0]

        return df[available_cols]

    def explain(self, ast: SelectStatement, source: str) -> str:
        """
        Generate execution plan explanation

        Shows the pandas operations that will be performed.
        """
        operations = ["Pandas Execution Plan:", "=" * 40]

        operations.append(f"1. Load DataFrame from {source}")

        if ast.join:
            operations.append(
                f"2. Merge with {ast.join.right_source} "
                f"({ast.join.join_type} JOIN on {ast.join.on_left} = {ast.join.on_right})"
            )

        if ast.where:
            conditions = " AND ".join(str(c) for c in ast.where.conditions)
            operations.append(f"3. Filter: {conditions}")

        if ast.group_by:
            aggs = ", ".join(f"{a.function}({a.column})" for a in ast.aggregates)
            operations.append(
                f"4. GroupBy {', '.join(ast.group_by)} with {aggs}"
            )

        if ast.order_by:
            order_spec = ", ".join(
                f"{o.column} {o.direction}" for o in ast.order_by
            )
            operations.append(f"5. Sort by {order_spec}")

        if ast.columns != ["*"]:
            operations.append(f"6. Select columns: {', '.join(ast.columns)}")

        if ast.limit:
            operations.append(f"7. Limit to {ast.limit} rows")

        operations.append("")
        operations.append(
            "Note: Pandas backend uses vectorized operations for high performance"
        )

        return "\n".join(operations)
