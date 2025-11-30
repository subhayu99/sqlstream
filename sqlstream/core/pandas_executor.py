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
        if ast.group_by:
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

    def _load_dataframe(self, source: str) -> pd.DataFrame:
        """
        Load data file into DataFrame

        Supports CSV and Parquet formats, including HTTP URLs.
        """
        # Handle HTTP URLs by using HTTPReader (for caching)
        if source.startswith(("http://", "https://")):
            from sqlstream.readers.http_reader import HTTPReader

            # Use HTTPReader to download/cache, then get local path
            reader = HTTPReader(source)
            source = str(reader.local_path)

        if source.endswith(".parquet"):
            return pd.read_parquet(source)
        elif source.endswith(".csv"):
            return pd.read_csv(source)
        else:
            # Try CSV as default
            try:
                return pd.read_csv(source)
            except Exception:
                raise ValueError(f"Unsupported file format: {source}")

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

        # Perform groupby
        grouped = df.groupby(ast.group_by, as_index=False).agg(agg_dict)

        # Rename aggregated columns to match expected output
        grouped = grouped.rename(columns=rename_map)

        # Select only the columns specified in SELECT (group_by cols + aggregates)
        result_cols = ast.group_by.copy()
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
