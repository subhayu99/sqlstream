"""
Project operator - implements SELECT column list

Selects specific columns from rows (or all columns with *).
"""

from typing import Any, Dict, Iterator, List

from sqlstream.operators.base import Operator


class Project(Operator):
    """
    Project operator - selects columns (SELECT clause)

    Pulls rows from child and yields only the requested columns.

    For efficiency, we use dict views rather than copying data.
    """

    def __init__(self, child: Operator, columns: List[str]):
        """
        Initialize project operator

        Args:
            child: Child operator to pull rows from
            columns: List of column names to select (or ['*'] for all)
        """
        super().__init__(child)
        self.columns = columns

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """
        Yield rows with only selected columns

        If columns is ['*'], yields all columns unchanged.
        Otherwise, creates a new dict with only the requested columns.
        """
        # SELECT *
        if self.columns == ["*"]:
            yield from self.child
            return

        # SELECT specific columns
        for row in self.child:
            # Create projected row with only selected columns
            projected = {}

            for col in self.columns:
                if col in row:
                    projected[col] = row[col]
                else:
                    # Column not found - set to None
                    projected[col] = None

            yield projected

    def __repr__(self) -> str:
        col_str = ", ".join(self.columns)
        return f"Project({col_str})"
