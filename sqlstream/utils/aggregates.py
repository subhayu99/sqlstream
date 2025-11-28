"""
Aggregation function implementations

Provides COUNT, SUM, AVG, MIN, MAX aggregations.
Each aggregator maintains state and can be updated incrementally.
"""

from typing import Any, List, Optional


class Aggregator:
    """Base class for aggregators"""

    def update(self, value: Any) -> None:
        """Update aggregator with a new value"""
        raise NotImplementedError

    def result(self) -> Any:
        """Get final aggregated result"""
        raise NotImplementedError


class CountAggregator(Aggregator):
    """COUNT aggregator - counts non-NULL values"""

    def __init__(self, count_star: bool = False):
        """
        Initialize COUNT aggregator

        Args:
            count_star: If True, counts all rows (COUNT(*))
                       If False, counts non-NULL values (COUNT(column))
        """
        self.count_star = count_star
        self.count = 0

    def update(self, value: Any) -> None:
        """Update count"""
        if self.count_star or value is not None:
            self.count += 1

    def result(self) -> int:
        """Return total count"""
        return self.count


class SumAggregator(Aggregator):
    """SUM aggregator - sums numeric values"""

    def __init__(self):
        self.sum: Optional[float] = None

    def update(self, value: Any) -> None:
        """Add value to sum"""
        if value is None:
            return

        if self.sum is None:
            self.sum = 0

        try:
            self.sum += value
        except TypeError:
            # Skip non-numeric values
            pass

    def result(self) -> Optional[float]:
        """Return sum, or None if no valid values"""
        return self.sum


class AvgAggregator(Aggregator):
    """AVG aggregator - computes average of numeric values"""

    def __init__(self):
        self.sum = 0
        self.count = 0

    def update(self, value: Any) -> None:
        """Add value to average calculation"""
        if value is None:
            return

        try:
            self.sum += value
            self.count += 1
        except TypeError:
            # Skip non-numeric values
            pass

    def result(self) -> Optional[float]:
        """Return average, or None if no valid values"""
        if self.count == 0:
            return None
        return self.sum / self.count


class MinAggregator(Aggregator):
    """MIN aggregator - finds minimum value"""

    def __init__(self):
        self.min: Optional[Any] = None

    def update(self, value: Any) -> None:
        """Update minimum"""
        if value is None:
            return

        if self.min is None or value < self.min:
            self.min = value

    def result(self) -> Optional[Any]:
        """Return minimum value, or None if no valid values"""
        return self.min


class MaxAggregator(Aggregator):
    """MAX aggregator - finds maximum value"""

    def __init__(self):
        self.max: Optional[Any] = None

    def update(self, value: Any) -> None:
        """Update maximum"""
        if value is None:
            return

        if self.max is None or value > self.max:
            self.max = value

    def result(self) -> Optional[Any]:
        """Return maximum value, or None if no valid values"""
        return self.max


def create_aggregator(function: str, column: str) -> Aggregator:
    """
    Factory function to create appropriate aggregator

    Args:
        function: Aggregate function name (COUNT, SUM, AVG, MIN, MAX)
        column: Column name (or '*' for COUNT(*))

    Returns:
        Aggregator instance

    Raises:
        ValueError: If function is not recognized
    """
    function = function.upper()

    if function == "COUNT":
        return CountAggregator(count_star=(column == "*"))
    elif function == "SUM":
        return SumAggregator()
    elif function == "AVG":
        return AvgAggregator()
    elif function == "MIN":
        return MinAggregator()
    elif function == "MAX":
        return MaxAggregator()
    else:
        raise ValueError(f"Unknown aggregate function: {function}")
