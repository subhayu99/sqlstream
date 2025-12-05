"""
Filter operator - implements WHERE clause

Evaluates conditions and only yields rows that match.
"""

from collections.abc import Iterator
from typing import Any

from sqlstream.operators.base import Operator
from sqlstream.sql.ast_nodes import Condition


class Filter(Operator):
    """
    Filter operator - evaluates WHERE conditions

    Pulls rows from child and only yields those that satisfy
    all conditions (AND logic).
    """

    def __init__(self, child: Operator, conditions: list[Condition]):
        """
        Initialize filter operator

        Args:
            child: Child operator to pull rows from
            conditions: List of conditions (AND'd together)
        """
        super().__init__(child)
        self.conditions = conditions

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """
        Yield only rows that match all conditions

        For each row from child:
        1. Evaluate all conditions
        2. If all are True, yield the row
        3. Otherwise, skip it
        """
        for row in self.child:
            if self._matches(row):
                yield row

    def _matches(self, row: dict[str, Any]) -> bool:
        """
        Check if row matches all conditions

        Args:
            row: Row to check

        Returns:
            True if all conditions are satisfied (AND logic)
        """
        for condition in self.conditions:
            if not self._evaluate_condition(row, condition):
                return False
        return True

    def _evaluate_condition(self, row: dict[str, Any], condition: Condition) -> bool:
        """
        Evaluate a single condition against a row

        Args:
            row: Row to check
            condition: Condition to evaluate

        Returns:
            True if condition is satisfied
        """
        # Get column value
        if condition.column not in row:
            return False

        value = row[condition.column]

        # Handle NULL values
        if value is None:
            return False

        # Get expected value
        expected = condition.value

        # Evaluate operator
        op = condition.operator

        try:
            if op == "=":
                return value == expected
            elif op == ">":
                return value > expected
            elif op == "<":
                return value < expected
            elif op == ">=":
                return value >= expected
            elif op == "<=":
                return value <= expected
            elif op == "!=":
                return value != expected
            else:
                # Unknown operator - default to True to avoid filtering
                return True

        except TypeError:
            # Type mismatch (e.g., comparing string to int)
            return False

    def __repr__(self) -> str:
        cond_str = " AND ".join(str(c) for c in self.conditions)
        return f"Filter({cond_str})"
