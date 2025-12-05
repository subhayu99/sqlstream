"""
GroupBy Operator

Performs hash-based aggregation with GROUP BY support.
Groups rows by specified columns and computes aggregate functions.
"""

from collections.abc import Iterator
from typing import Any

from sqlstream.operators.base import Operator
from sqlstream.sql.ast_nodes import AggregateFunction
from sqlstream.utils.aggregates import create_aggregator


class GroupByOperator(Operator):
    """
    GROUP BY operator with aggregation

    Uses hash-based aggregation:
    1. Scan all input rows
    2. Group by key columns
    3. Maintain aggregators for each group
    4. Yield one row per group

    Note: This operator materializes all data in memory (not lazy).
    For large datasets, consider external sorting/grouping.
    """

    def __init__(
        self,
        source: Operator,
        group_by_columns: list[str],
        aggregates: list[AggregateFunction],
        select_columns: list[str],
    ):
        """
        Initialize GroupBy operator

        Args:
            source: Source operator
            group_by_columns: List of columns to group by
            aggregates: List of aggregate functions to compute
            select_columns: List of columns in SELECT clause (for output order)
        """
        super().__init__(source)
        self.group_by_columns = group_by_columns
        self.aggregates = aggregates
        self.select_columns = select_columns

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """
        Execute GROUP BY aggregation

        Yields:
            One row per group with group columns and aggregated values
        """
        # Hash map: group_key -> aggregators
        groups: dict[tuple, list] = {}

        # Scan all input rows and build groups
        for row in self.child:
            # Extract group key
            group_key = self._extract_group_key(row)

            # Initialize aggregators for new group
            if group_key not in groups:
                groups[group_key] = self._create_aggregators()

            # Update aggregators
            aggregators = groups[group_key]
            for i, agg_func in enumerate(self.aggregates):
                value = row.get(agg_func.column) if agg_func.column != "*" else None
                aggregators[i].update(value)

        # Yield one row per group
        for group_key, aggregators in groups.items():
            row = self._build_output_row(group_key, aggregators)
            yield row

    def _extract_group_key(self, row: dict[str, Any]) -> tuple:
        """
        Extract group key from row

        Args:
            row: Input row

        Returns:
            Tuple of group key values
        """
        key_values = []
        for col in self.group_by_columns:
            value = row.get(col)
            # Handle unhashable types (e.g., lists, dicts)
            # Convert to string representation for hashing
            if isinstance(value, (list, dict)):
                value = str(value)
            key_values.append(value)

        return tuple(key_values)

    def _create_aggregators(self) -> list:
        """
        Create fresh aggregators for a new group

        Returns:
            List of aggregator instances
        """
        aggregators = []
        for agg_func in self.aggregates:
            aggregator = create_aggregator(agg_func.function, agg_func.column)
            aggregators.append(aggregator)
        return aggregators

    def _build_output_row(self, group_key: tuple, aggregators: list) -> dict[str, Any]:
        """
        Build output row from group key and aggregated values

        Args:
            group_key: Tuple of group key values
            aggregators: List of aggregators with final values

        Returns:
            Output row dictionary
        """
        row = {}

        # Add group key columns
        for i, col_name in enumerate(self.group_by_columns):
            row[col_name] = group_key[i]

        # Add aggregated columns
        for i, agg_func in enumerate(self.aggregates):
            # Use alias if provided, otherwise generate name
            col_name = (
                agg_func.alias
                if agg_func.alias
                else f"{agg_func.function.lower()}_{agg_func.column}"
            )
            row[col_name] = aggregators[i].result()

        return row

    def explain(self, indent: int = 0) -> list[str]:
        """Generate execution plan explanation"""
        lines = [" " * indent + f"GroupBy(keys={self.group_by_columns})"]

        # Show aggregate functions
        for agg in self.aggregates:
            lines.append(" " * (indent + 2) + f"→ {agg}")

        # Add source explanation
        lines.extend(self.child.explain(indent + 2))

        return lines
