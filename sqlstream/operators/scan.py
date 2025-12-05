"""
Scan operator - reads data from a source

This is a leaf operator (has no child).
It wraps a reader and yields rows from it.
"""

from collections.abc import Iterator
from typing import Any

from sqlstream.operators.base import Operator
from sqlstream.readers.base import BaseReader


class Scan(Operator):
    """
    Scan operator - wrapper around a data source reader

    This is the leaf of the operator tree. It pulls data from
    a reader and yields it to parent operators.
    """

    def __init__(self, reader: BaseReader):
        """
        Initialize scan operator

        Args:
            reader: Data source reader to scan
        """
        super().__init__(child=None)  # Scan has no child
        self.reader = reader

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """
        Yield all rows from the reader

        This delegates directly to the reader's lazy iterator.
        """
        yield from self.reader.read_lazy()

    def __repr__(self) -> str:
        return f"Scan({self.reader.__class__.__name__})"
