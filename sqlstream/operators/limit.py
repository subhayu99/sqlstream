"""
Limit operator - implements LIMIT clause

Yields only the first N rows, then stops.
"""

from collections.abc import Iterator
from typing import Any

from sqlstream.operators.base import Operator


class Limit(Operator):
    """
    Limit operator - restricts number of rows (LIMIT clause)

    Pulls rows from child and yields only the first N rows.
    This allows for early termination - we stop pulling from
    child once we've yielded enough rows.
    """

    def __init__(self, child: Operator, limit: int):
        """
        Initialize limit operator

        Args:
            child: Child operator to pull rows from
            limit: Maximum number of rows to yield
        """
        super().__init__(child)
        self.limit = limit

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """
        Yield at most limit rows

        This is efficient because it stops pulling from child
        as soon as we've yielded enough rows (early termination).
        """
        count = 0

        for row in self.child:
            if count >= self.limit:
                break

            yield row
            count += 1

    def __repr__(self) -> str:
        return f"Limit({self.limit})"
