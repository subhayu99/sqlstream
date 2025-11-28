"""
Tests for aggregation functions
"""

import pytest

from sqlstream.utils.aggregates import (
    AvgAggregator,
    CountAggregator,
    MaxAggregator,
    MinAggregator,
    SumAggregator,
    create_aggregator,
)


class TestCountAggregator:
    """Test COUNT aggregator"""

    def test_count_star(self):
        """Test COUNT(*) - counts all rows"""
        agg = CountAggregator(count_star=True)

        agg.update(5)
        agg.update(None)
        agg.update(10)

        assert agg.result() == 3  # Counts NULLs too

    def test_count_column(self):
        """Test COUNT(column) - counts non-NULL values"""
        agg = CountAggregator(count_star=False)

        agg.update(5)
        agg.update(None)
        agg.update(10)

        assert agg.result() == 2  # Skips NULLs

    def test_count_empty(self):
        """Test count with no values"""
        agg = CountAggregator()
        assert agg.result() == 0


class TestSumAggregator:
    """Test SUM aggregator"""

    def test_sum_integers(self):
        """Test summing integers"""
        agg = SumAggregator()

        agg.update(1)
        agg.update(2)
        agg.update(3)

        assert agg.result() == 6

    def test_sum_floats(self):
        """Test summing floats"""
        agg = SumAggregator()

        agg.update(1.5)
        agg.update(2.5)
        agg.update(3.0)

        assert agg.result() == 7.0

    def test_sum_with_nulls(self):
        """Test SUM ignores NULL values"""
        agg = SumAggregator()

        agg.update(10)
        agg.update(None)
        agg.update(20)

        assert agg.result() == 30

    def test_sum_empty(self):
        """Test SUM with no values returns None"""
        agg = SumAggregator()
        assert agg.result() is None

    def test_sum_non_numeric(self):
        """Test SUM skips non-numeric values"""
        agg = SumAggregator()

        agg.update(10)
        agg.update("abc")  # Should be skipped
        agg.update(20)

        assert agg.result() == 30


class TestAvgAggregator:
    """Test AVG aggregator"""

    def test_avg_integers(self):
        """Test averaging integers"""
        agg = AvgAggregator()

        agg.update(10)
        agg.update(20)
        agg.update(30)

        assert agg.result() == 20.0

    def test_avg_floats(self):
        """Test averaging floats"""
        agg = AvgAggregator()

        agg.update(1.5)
        agg.update(2.5)
        agg.update(3.0)

        assert agg.result() == pytest.approx(2.333, rel=0.01)

    def test_avg_with_nulls(self):
        """Test AVG ignores NULL values"""
        agg = AvgAggregator()

        agg.update(10)
        agg.update(None)
        agg.update(20)

        assert agg.result() == 15.0  # (10 + 20) / 2

    def test_avg_empty(self):
        """Test AVG with no values returns None"""
        agg = AvgAggregator()
        assert agg.result() is None


class TestMinAggregator:
    """Test MIN aggregator"""

    def test_min_integers(self):
        """Test MIN with integers"""
        agg = MinAggregator()

        agg.update(30)
        agg.update(10)
        agg.update(20)

        assert agg.result() == 10

    def test_min_strings(self):
        """Test MIN with strings (lexicographic order)"""
        agg = MinAggregator()

        agg.update("banana")
        agg.update("apple")
        agg.update("cherry")

        assert agg.result() == "apple"

    def test_min_with_nulls(self):
        """Test MIN ignores NULL values"""
        agg = MinAggregator()

        agg.update(20)
        agg.update(None)
        agg.update(10)

        assert agg.result() == 10

    def test_min_empty(self):
        """Test MIN with no values returns None"""
        agg = MinAggregator()
        assert agg.result() is None


class TestMaxAggregator:
    """Test MAX aggregator"""

    def test_max_integers(self):
        """Test MAX with integers"""
        agg = MaxAggregator()

        agg.update(10)
        agg.update(30)
        agg.update(20)

        assert agg.result() == 30

    def test_max_strings(self):
        """Test MAX with strings (lexicographic order)"""
        agg = MaxAggregator()

        agg.update("banana")
        agg.update("apple")
        agg.update("cherry")

        assert agg.result() == "cherry"

    def test_max_with_nulls(self):
        """Test MAX ignores NULL values"""
        agg = MaxAggregator()

        agg.update(10)
        agg.update(None)
        agg.update(20)

        assert agg.result() == 20

    def test_max_empty(self):
        """Test MAX with no values returns None"""
        agg = MaxAggregator()
        assert agg.result() is None


class TestAggregatorFactory:
    """Test create_aggregator factory function"""

    def test_create_count(self):
        """Test creating COUNT aggregator"""
        agg = create_aggregator("COUNT", "*")
        assert isinstance(agg, CountAggregator)
        assert agg.count_star is True

        agg = create_aggregator("COUNT", "id")
        assert isinstance(agg, CountAggregator)
        assert agg.count_star is False

    def test_create_sum(self):
        """Test creating SUM aggregator"""
        agg = create_aggregator("SUM", "amount")
        assert isinstance(agg, SumAggregator)

    def test_create_avg(self):
        """Test creating AVG aggregator"""
        agg = create_aggregator("AVG", "price")
        assert isinstance(agg, AvgAggregator)

    def test_create_min(self):
        """Test creating MIN aggregator"""
        agg = create_aggregator("MIN", "age")
        assert isinstance(agg, MinAggregator)

    def test_create_max(self):
        """Test creating MAX aggregator"""
        agg = create_aggregator("MAX", "age")
        assert isinstance(agg, MaxAggregator)

    def test_create_case_insensitive(self):
        """Test factory is case-insensitive"""
        agg = create_aggregator("count", "*")
        assert isinstance(agg, CountAggregator)

        agg = create_aggregator("SuM", "amount")
        assert isinstance(agg, SumAggregator)

    def test_create_unknown_function(self):
        """Test error on unknown function"""
        with pytest.raises(ValueError, match="Unknown aggregate function"):
            create_aggregator("MEDIAN", "value")
