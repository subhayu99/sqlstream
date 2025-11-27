"""
Query Planner - applies optimizations before execution

The planner analyzes the query AST and applies optimization rules
such as predicate pushdown and column pruning before execution begins.

This is where the "smart" part of SQLStream happens!
"""

from typing import List, Set

from sqlstream.readers.base import BaseReader
from sqlstream.sql.ast_nodes import Condition, SelectStatement


class QueryPlanner:
    """
    Query planner and optimizer

    Analyzes queries and applies optimizations:
    1. Predicate pushdown - push WHERE filters to readers
    2. Column pruning - tell readers which columns to read
    3. (Future) Partition pruning, join reordering, etc.

    The planner modifies the reader in-place with optimization hints.
    """

    def __init__(self):
        """Initialize planner"""
        self.optimizations_applied: List[str] = []

    def optimize(self, ast: SelectStatement, reader: BaseReader) -> None:
        """
        Apply all applicable optimizations

        Args:
            ast: Parsed SQL statement
            reader: Data source reader

        Modifies:
            - reader: Sets filter conditions and column selections
            - self.optimizations_applied: Tracks what was optimized
        """
        self.optimizations_applied = []

        # Optimization 1: Predicate Pushdown
        if ast.where and reader.supports_pushdown():
            self._apply_predicate_pushdown(ast, reader)

        # Optimization 2: Column Pruning
        if reader.supports_column_selection():
            self._apply_column_pruning(ast, reader)

    def _apply_predicate_pushdown(
        self, ast: SelectStatement, reader: BaseReader
    ) -> None:
        """
        Push WHERE conditions to the reader

        This allows readers to filter data as they read it,
        reducing I/O and memory usage.

        Args:
            ast: Parsed SQL statement
            reader: Data source reader
        """
        if not ast.where:
            return

        # Determine which conditions can be pushed down
        pushable = self._extract_pushable_conditions(ast.where.conditions)

        if pushable:
            reader.set_filter(pushable)
            self.optimizations_applied.append(
                f"Predicate pushdown: {len(pushable)} condition(s)"
            )

    def _extract_pushable_conditions(
        self, conditions: List[Condition]
    ) -> List[Condition]:
        """
        Determine which conditions can be safely pushed to readers

        Pushable conditions are simple column comparisons:
        - column = value
        - column > value
        - etc.

        NOT pushable (in future phases):
        - Complex expressions: LENGTH(name) > 5
        - Cross-column comparisons: age > salary
        - User-defined functions

        Args:
            conditions: List of WHERE conditions

        Returns:
            List of conditions safe to push down
        """
        pushable = []

        for condition in conditions:
            # For now, all simple conditions are pushable
            # In the future, we'd check for complex expressions here
            if self._is_simple_condition(condition):
                pushable.append(condition)

        return pushable

    def _is_simple_condition(self, condition: Condition) -> bool:
        """
        Check if condition is a simple column comparison

        Simple conditions:
        - column op value (where value is a literal)

        Args:
            condition: Condition to check

        Returns:
            True if condition is simple and pushable
        """
        # For now, all our conditions are simple
        # Future: check if value is a literal vs expression
        return True

    def _apply_column_pruning(self, ast: SelectStatement, reader: BaseReader) -> None:
        """
        Tell reader which columns to read

        This is a major optimization for columnar formats like Parquet.
        Reading fewer columns = less I/O and memory.

        Args:
            ast: Parsed SQL statement
            reader: Data source reader
        """
        # Analyze which columns are actually needed
        needed_columns = self._analyze_column_dependencies(ast)

        # Don't apply if SELECT *
        if "*" in needed_columns:
            return

        reader.set_columns(needed_columns)
        self.optimizations_applied.append(
            f"Column pruning: {len(needed_columns)} column(s) selected"
        )

    def _analyze_column_dependencies(self, ast: SelectStatement) -> List[str]:
        """
        Determine which columns are needed for the query

        Columns are needed if they appear in:
        - SELECT clause
        - WHERE clause
        - ORDER BY clause (Phase 4)
        - GROUP BY clause (Phase 4)
        - JOIN conditions (Phase 5)

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

        # Future: Add columns from ORDER BY, GROUP BY, JOIN

        return list(needed)

    def get_optimization_summary(self) -> str:
        """
        Get summary of optimizations applied

        Returns:
            Human-readable summary

        Example:
            "Optimizations applied:
             - Predicate pushdown: 2 condition(s)
             - Column pruning: 3 column(s) selected"
        """
        if not self.optimizations_applied:
            return "No optimizations applied"

        summary = "Optimizations applied:\n"
        for opt in self.optimizations_applied:
            summary += f"  - {opt}\n"

        return summary.strip()
