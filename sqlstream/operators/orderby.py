"""
OrderBy Operator

Sorts rows by specified columns with ASC/DESC directions.
"""

from typing import Any, Dict, Iterator, List

from sqlstream.operators.base import Operator
from sqlstream.sql.ast_nodes import OrderByColumn


class OrderByOperator(Operator):
    """
    ORDER BY operator

    Sorts all input rows by specified columns.

    Note: This operator materializes all data in memory (not lazy).
    For large datasets that don't fit in memory, consider external sorting.
    """

    def __init__(self, source: Operator, order_by: List[OrderByColumn]):
        """
        Initialize OrderBy operator

        Args:
            source: Source operator
            order_by: List of OrderByColumn specifications
        """
        super().__init__(source)
        self.order_by = order_by

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """
        Execute ORDER BY sorting

        Yields:
            Rows in sorted order
        """
        # Materialize all rows
        rows = list(self.child)

        # Sort rows using multi-key sort
        sorted_rows = sorted(rows, key=self._sort_key)

        # Yield sorted rows
        yield from sorted_rows

    def _sort_key(self, row: Dict[str, Any]) -> tuple:
        """
        Generate sort key for a row

        Args:
            row: Input row

        Returns:
            Tuple of (value, reverse_flag) for multi-key sorting
        """
        key_parts = []

        for order_col in self.order_by:
            value = row.get(order_col.column)

            # Handle NULL values - sort them last
            if value is None:
                # Use a sentinel that sorts last
                value = (1, None)  # (sort_last_flag, None)
            else:
                value = (0, value)  # (sort_first_flag, actual_value)

            # For DESC, we need to reverse the comparison
            if order_col.direction == "DESC":
                # Invert the sort order by negating numbers or using reverse wrapper
                if isinstance(value[1], (int, float)):
                    value = (value[0], -value[1] if value[1] is not None else None)
                else:
                    # For non-numeric types, we'll use a reverse wrapper
                    value = (value[0], ReverseCompare(value[1]))

            key_parts.append(value)

        return tuple(key_parts)

    def explain(self, indent: int = 0) -> List[str]:
        """Generate execution plan explanation"""
        order_spec = ", ".join(f"{col.column} {col.direction}" for col in self.order_by)
        lines = [" " * indent + f"OrderBy({order_spec})"]
        lines.extend(self.child.explain(indent + 2))
        return lines


class ReverseCompare:
    """
    Wrapper class to reverse comparison order for non-numeric types

    Used for DESC sorting of strings and other non-numeric types.
    """

    def __init__(self, value):
        self.value = value

    def __lt__(self, other):
        if isinstance(other, ReverseCompare):
            return self.value > other.value
        return self.value > other

    def __le__(self, other):
        if isinstance(other, ReverseCompare):
            return self.value >= other.value
        return self.value >= other

    def __gt__(self, other):
        if isinstance(other, ReverseCompare):
            return self.value < other.value
        return self.value < other

    def __ge__(self, other):
        if isinstance(other, ReverseCompare):
            return self.value <= other.value
        return self.value <= other

    def __eq__(self, other):
        if isinstance(other, ReverseCompare):
            return self.value == other.value
        return self.value == other

    def __ne__(self, other):
        if isinstance(other, ReverseCompare):
            return self.value != other.value
        return self.value != other

    def __repr__(self):
        return f"ReverseCompare({self.value!r})"
