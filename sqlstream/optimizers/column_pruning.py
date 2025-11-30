"""
Column Pruning Optimizer

Tells the reader which columns are actually needed for the query,
allowing it to skip reading unused columns.

This is especially effective for columnar formats like Parquet.
"""

from typing import List, Set

from sqlstream.optimizers.base import Optimizer
from sqlstream.readers.base import BaseReader
from sqlstream.sql.ast_nodes import SelectStatement


class ColumnPruningOptimizer(Optimizer):
    """
    Prune (skip reading) unused columns

    Benefits:
    - Massive I/O reduction for wide tables
    - Reduces memory usage
    - Critical for columnar formats (Parquet, ORC)
    - Can read 10x faster if selecting 1 column from 10

    Example:
        SELECT name, age FROM employees  -- 100 columns total

        Without pruning: Read all 100 columns → Project 2
        With pruning: Read only 2 columns → Much faster
    """

    def get_name(self) -> str:
        return "Column pruning"

    def can_optimize(self, ast: SelectStatement, reader: BaseReader) -> bool:
        """
        Check if column pruning is applicable

        Conditions:
        1. Reader supports column selection
        2. Not SELECT * (can't prune if all columns needed)

        Args:
            ast: Parsed SQL statement
            reader: Data source reader

        Returns:
            True if optimization can be applied
        """
        # Reader must support column selection
        if not reader.supports_column_selection():
            return False

        # Can't prune with SELECT *
        if "*" in ast.columns:
            return False

        return True

    def optimize(self, ast: SelectStatement, reader: BaseReader) -> None:
        """
        Apply column pruning optimization

        Args:
            ast: Parsed SQL statement
            reader: Data source reader
        """
        # Analyze which columns are actually needed
        needed_columns = self._analyze_column_dependencies(ast)

        # Don't apply if SELECT * found during analysis
        if "*" in needed_columns:
            return

        reader.set_columns(needed_columns)
        self.applied = True
        self.description = f"{len(needed_columns)} column(s) selected"

    def _analyze_column_dependencies(self, ast: SelectStatement) -> List[str]:
        """
        Determine which columns are needed for the query

        Columns are needed if they appear in:
        - SELECT clause
        - WHERE clause
        - GROUP BY clause
        - ORDER BY clause
        - Aggregate functions
        - JOIN conditions

        Args:
            ast: Parsed SQL statement

        Returns:
            List of required column names (or ['*'] for all)
        """
        needed: Set[str] = set()

        # Columns from SELECT clause
        if "*" in ast.columns:
            return ["*"]  # Can't prune if SELECT *

        needed.update(ast.columns)

        # Columns from WHERE clause
        if ast.where:
            for condition in ast.where.conditions:
                needed.add(condition.column)

        # Columns from GROUP BY clause
        if ast.group_by:
            needed.update(ast.group_by)

        # Columns from ORDER BY clause
        if ast.order_by:
            for order_col in ast.order_by:
                needed.add(order_col.column)

        # Columns from aggregate functions
        if ast.aggregates:
            for agg in ast.aggregates:
                if agg.column != "*":  # COUNT(*) doesn't need a column
                    needed.add(agg.column)

        # Columns from JOIN conditions
        if ast.join:
            # Need the left join key from the left table
            needed.add(ast.join.on_left)
            # Note: right join key is from right table, handled separately

        return list(needed)