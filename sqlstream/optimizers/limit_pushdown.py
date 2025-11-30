"""
Limit Pushdown Optimizer

Pushes LIMIT clause down to the data source reader,
allowing early termination of data reading.

This is especially useful for "top N" queries on large datasets.
"""

from sqlstream.optimizers.base import Optimizer
from sqlstream.readers.base import BaseReader
from sqlstream.sql.ast_nodes import SelectStatement


class LimitPushdownOptimizer(Optimizer):
    """
    Push LIMIT to the reader for early termination

    Benefits:
    - Stop reading after N rows
    - Massive speedup for large files
    - Reduces memory usage

    Example:
        SELECT * FROM large_file.csv LIMIT 10

        Without pushdown: Read entire file → Take first 10
        With pushdown: Stop reading after 10 rows → Much faster

    Note:
        Cannot push down if query has:
        - ORDER BY (need to see all rows to sort)
        - GROUP BY (need to see all rows to group)
        - Aggregates (need all rows to aggregate)
        - JOIN (complex - may need all rows)
    """

    def get_name(self) -> str:
        return "Limit pushdown"

    def can_optimize(self, ast: SelectStatement, reader: BaseReader) -> bool:
        """
        Check if limit pushdown is applicable

        Conditions:
        1. Query has LIMIT clause
        2. No ORDER BY (would need to read all rows first)
        3. No GROUP BY (would need to read all rows first)
        4. No aggregates (would need to read all rows first)
        5. No JOIN (complex - skip for now)

        Args:
            ast: Parsed SQL statement
            reader: Data source reader

        Returns:
            True if optimization can be applied
        """
        # Must have LIMIT
        if ast.limit is None:
            return False

        # Cannot push down with ORDER BY
        if ast.order_by:
            return False

        # Cannot push down with GROUP BY
        if ast.group_by:
            return False

        # Cannot push down with aggregates
        if ast.aggregates:
            return False

        # Cannot push down with JOIN (for now)
        if ast.join:
            return False

        return True

    def optimize(self, ast: SelectStatement, reader: BaseReader) -> None:
        """
        Apply limit pushdown optimization

        Note: This optimization is currently a no-op because BaseReader
        doesn't have a set_limit() method. This is here as a placeholder
        for future implementation.

        Args:
            ast: Parsed SQL statement
            reader: Data source reader
        """
        # TODO: Add set_limit() method to BaseReader
        # For now, this is a placeholder to show the optimization opportunity
        # reader.set_limit(ast.limit)

        self.applied = True
        self.description = f"limit {ast.limit} (not yet implemented in readers)"