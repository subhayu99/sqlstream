"""
Predicate Pushdown Optimizer

Pushes WHERE clause conditions down to the data source reader,
allowing filtering to happen during data reading rather than after.

This is one of the most important optimizations for reducing I/O and memory usage.
"""

from sqlstream.optimizers.base import Optimizer
from sqlstream.readers.base import BaseReader
from sqlstream.sql.ast_nodes import Condition, SelectStatement


class PredicatePushdownOptimizer(Optimizer):
    """
    Push WHERE conditions to the reader

    Benefits:
    - Reduces I/O by filtering at the source
    - Reduces memory usage
    - Especially effective for columnar formats (Parquet)
    - Can leverage indexes if available

    Example:
        SELECT * FROM data WHERE age > 30

        Without pushdown: Read all rows → Filter in memory
        With pushdown: Filter while reading → Less data read
    """

    def get_name(self) -> str:
        return "Predicate pushdown"

    def can_optimize(self, ast: SelectStatement, reader: BaseReader) -> bool:
        """
        Check if predicate pushdown is applicable

        Conditions:
        1. Query has WHERE clause
        2. Reader supports pushdown
        3. Not a JOIN query (complex - needs smarter analysis)

        Args:
            ast: Parsed SQL statement
            reader: Data source reader

        Returns:
            True if optimization can be applied
        """
        # Must have WHERE clause
        if not ast.where:
            return False

        # Reader must support pushdown
        if not reader.supports_pushdown():
            return False

        # Skip JOINs for now - WHERE conditions may reference either table
        # TODO: Make this smarter by analyzing which conditions apply to which table
        if ast.join:
            return False

        return True

    def optimize(self, ast: SelectStatement, reader: BaseReader) -> None:
        """
        Apply predicate pushdown optimization

        Args:
            ast: Parsed SQL statement
            reader: Data source reader
        """
        # Extract conditions that can be pushed down
        pushable = self._extract_pushable_conditions(ast.where.conditions)

        if pushable:
            reader.set_filter(pushable)
            self.applied = True
            self.description = f"{len(pushable)} condition(s)"

    def _extract_pushable_conditions(self, conditions: list[Condition]) -> list[Condition]:
        """
        Determine which conditions can be safely pushed to readers

        Pushable conditions:
        - Simple column comparisons: column op value
        - Where value is a literal (not another column or expression)

        NOT pushable (future work):
        - Complex expressions: LENGTH(name) > 5
        - Cross-column comparisons: age > salary
        - User-defined functions
        - Conditions involving aggregates

        Args:
            conditions: List of WHERE conditions

        Returns:
            List of conditions safe to push down
        """
        pushable = []

        for condition in conditions:
            # For now, all simple conditions are pushable
            # Future: Check for complex expressions, UDFs, etc.
            if self._is_simple_condition(condition):
                pushable.append(condition)

        return pushable

    def _is_simple_condition(self, condition: Condition) -> bool:
        """
        Check if condition is a simple column comparison

        Simple conditions:
        - column = value (literal)
        - column > value (literal)
        - column < value (literal)
        - etc.

        Args:
            condition: Condition to check

        Returns:
            True if condition is simple and pushable
        """
        # For now, all our conditions are simple
        # Future improvements:
        # - Check if value is a literal vs expression
        # - Detect cross-column comparisons
        # - Detect function calls
        return True
