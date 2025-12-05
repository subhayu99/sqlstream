"""
Base operator class for Volcano-style query execution

The Volcano model uses pull-based execution where each operator
pulls data from its child operator(s) on demand.
"""

from collections.abc import Iterator
from typing import Any, Optional


class Operator:
    """
    Base class for all query operators

    Operators form a tree where:
    - Leaf operators (e.g., Scan) read from data sources
    - Internal operators (e.g., Filter, Project) transform data
    - Root operator is pulled by the executor to get results

    The pull-based execution model means:
    - Operators are lazy (generators)
    - Data flows through the tree on-demand
    - Memory usage is O(pipeline depth), not O(data size)
    """

    def __init__(self, child: Optional["Operator"] = None):
        """
        Initialize operator

        Args:
            child: Child operator to pull data from (None for leaf operators)
        """
        self.child = child

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """
        Execute operator and yield results

        This is the core method that defines operator behavior.
        Subclasses must implement this to define how they process data.

        Yields:
            Rows as dictionaries
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement __iter__()")

    def __repr__(self) -> str:
        """String representation for debugging"""
        return f"{self.__class__.__name__}()"
