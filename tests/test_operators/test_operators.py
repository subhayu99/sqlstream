"""
Tests for query operators (Volcano model)
"""

import pytest

from sqlstream.operators.filter import Filter
from sqlstream.operators.limit import Limit
from sqlstream.operators.project import Project
from sqlstream.operators.scan import Scan
from sqlstream.readers.base import BaseReader
from sqlstream.sql.ast_nodes import Condition


class MockReader(BaseReader):
    """Mock reader for testing operators"""

    def __init__(self, data):
        self.data = data

    def read_lazy(self):
        yield from self.data


@pytest.fixture
def sample_data():
    """Sample data for testing"""
    return [
        {"name": "Alice", "age": 30, "city": "NYC", "salary": 75000},
        {"name": "Bob", "age": 25, "city": "LA", "salary": 65000},
        {"name": "Charlie", "age": 35, "city": "SF", "salary": 85000},
        {"name": "Diana", "age": 28, "city": "NYC", "salary": 70000},
        {"name": "Eve", "age": 32, "city": "LA", "salary": 80000},
    ]


class TestScanOperator:
    """Test Scan operator"""

    def test_scan_all_rows(self, sample_data):
        """Test that scan yields all rows from reader"""
        reader = MockReader(sample_data)
        scan = Scan(reader)

        rows = list(scan)

        assert len(rows) == 5
        assert rows[0]["name"] == "Alice"
        assert rows[-1]["name"] == "Eve"

    def test_scan_empty(self):
        """Test scan with empty data"""
        reader = MockReader([])
        scan = Scan(reader)

        rows = list(scan)

        assert len(rows) == 0

    def test_scan_lazy(self, sample_data):
        """Test that scan is lazy (generator)"""
        reader = MockReader(sample_data)
        scan = Scan(reader)

        # Should be a generator
        iterator = iter(scan)
        first = next(iterator)

        assert first["name"] == "Alice"

        second = next(iterator)
        assert second["name"] == "Bob"


class TestFilterOperator:
    """Test Filter operator"""

    def test_filter_equals(self, sample_data):
        """Test filter with equals condition"""
        reader = MockReader(sample_data)
        scan = Scan(reader)
        filter_op = Filter(scan, [Condition("city", "=", "NYC")])

        rows = list(filter_op)

        assert len(rows) == 2
        assert all(row["city"] == "NYC" for row in rows)

    def test_filter_greater_than(self, sample_data):
        """Test filter with > condition"""
        reader = MockReader(sample_data)
        scan = Scan(reader)
        filter_op = Filter(scan, [Condition("age", ">", 30)])

        rows = list(filter_op)

        assert len(rows) == 2
        assert all(row["age"] > 30 for row in rows)

    def test_filter_multiple_conditions(self, sample_data):
        """Test filter with multiple AND conditions"""
        reader = MockReader(sample_data)
        scan = Scan(reader)
        filter_op = Filter(scan, [Condition("age", ">", 25), Condition("city", "=", "NYC")])

        rows = list(filter_op)

        # Alice (30, NYC) and Diana (28, NYC)
        assert len(rows) == 2
        assert all(row["age"] > 25 and row["city"] == "NYC" for row in rows)

    def test_filter_no_matches(self, sample_data):
        """Test filter that matches nothing"""
        reader = MockReader(sample_data)
        scan = Scan(reader)
        filter_op = Filter(scan, [Condition("age", ">", 100)])

        rows = list(filter_op)

        assert len(rows) == 0

    def test_filter_all_operators(self, sample_data):
        """Test all comparison operators"""
        operators_and_expected = [
            ("=", 30, 1),  # age = 30 -> Alice
            (">", 30, 2),  # age > 30 -> Charlie, Eve
            ("<", 30, 2),  # age < 30 -> Bob, Diana
            (">=", 30, 3),  # age >= 30 -> Alice, Charlie, Eve
            ("<=", 30, 3),  # age <= 30 -> Alice, Bob, Diana
            ("!=", 30, 4),  # age != 30 -> all except Alice
        ]

        for op, value, expected_count in operators_and_expected:
            reader = MockReader(sample_data)
            scan = Scan(reader)
            filter_op = Filter(scan, [Condition("age", op, value)])

            rows = list(filter_op)
            assert len(rows) == expected_count, f"Operator {op} failed"


class TestProjectOperator:
    """Test Project operator"""

    def test_project_all(self, sample_data):
        """Test SELECT * (all columns)"""
        reader = MockReader(sample_data)
        scan = Scan(reader)
        project = Project(scan, ["*"])

        rows = list(project)

        assert len(rows) == 5
        # All columns should be present
        assert set(rows[0].keys()) == {"name", "age", "city", "salary"}

    def test_project_single_column(self, sample_data):
        """Test SELECT single column"""
        reader = MockReader(sample_data)
        scan = Scan(reader)
        project = Project(scan, ["name"])

        rows = list(project)

        assert len(rows) == 5
        assert set(rows[0].keys()) == {"name"}
        assert rows[0]["name"] == "Alice"

    def test_project_multiple_columns(self, sample_data):
        """Test SELECT multiple columns"""
        reader = MockReader(sample_data)
        scan = Scan(reader)
        project = Project(scan, ["name", "age"])

        rows = list(project)

        assert len(rows) == 5
        assert set(rows[0].keys()) == {"name", "age"}
        assert "city" not in rows[0]
        assert "salary" not in rows[0]

    def test_project_nonexistent_column(self, sample_data):
        """Test SELECT with nonexistent column"""
        reader = MockReader(sample_data)
        scan = Scan(reader)
        project = Project(scan, ["name", "nonexistent"])

        rows = list(project)

        assert len(rows) == 5
        assert rows[0]["name"] == "Alice"
        assert rows[0]["nonexistent"] is None


class TestLimitOperator:
    """Test Limit operator"""

    def test_limit(self, sample_data):
        """Test LIMIT n"""
        reader = MockReader(sample_data)
        scan = Scan(reader)
        limit = Limit(scan, 3)

        rows = list(limit)

        assert len(rows) == 3
        assert rows[0]["name"] == "Alice"
        assert rows[2]["name"] == "Charlie"

    def test_limit_zero(self, sample_data):
        """Test LIMIT 0"""
        reader = MockReader(sample_data)
        scan = Scan(reader)
        limit = Limit(scan, 0)

        rows = list(limit)

        assert len(rows) == 0

    def test_limit_greater_than_data(self, sample_data):
        """Test LIMIT greater than available rows"""
        reader = MockReader(sample_data)
        scan = Scan(reader)
        limit = Limit(scan, 100)

        rows = list(limit)

        # Should return all available rows
        assert len(rows) == 5

    def test_limit_early_termination(self, sample_data):
        """Test that limit stops pulling from child early"""

        class CountingReader(BaseReader):
            def __init__(self, data):
                self.data = data
                self.rows_read = 0

            def read_lazy(self):
                for row in self.data:
                    self.rows_read += 1
                    yield row

        reader = CountingReader(sample_data)
        scan = Scan(reader)
        limit = Limit(scan, 2)

        rows = list(limit)

        # Should only return 2 rows
        assert len(rows) == 2
        # Generator may read one ahead, so should read at most 3 rows (not all 5)
        assert reader.rows_read <= 3


class TestOperatorChaining:
    """Test chaining multiple operators together"""

    def test_scan_filter_project(self, sample_data):
        """Test: Scan -> Filter -> Project"""
        reader = MockReader(sample_data)
        scan = Scan(reader)
        filter_op = Filter(scan, [Condition("age", ">", 30)])
        project = Project(filter_op, ["name", "age"])

        rows = list(project)

        assert len(rows) == 2  # Charlie and Eve
        assert set(rows[0].keys()) == {"name", "age"}
        assert all(row["age"] > 30 for row in rows)

    def test_scan_filter_project_limit(self, sample_data):
        """Test: Scan -> Filter -> Project -> Limit"""
        reader = MockReader(sample_data)
        scan = Scan(reader)
        filter_op = Filter(scan, [Condition("age", ">", 25)])
        project = Project(filter_op, ["name", "age"])
        limit = Limit(project, 2)

        rows = list(limit)

        assert len(rows) == 2
        assert set(rows[0].keys()) == {"name", "age"}
        assert all(row["age"] > 25 for row in rows)

    def test_scan_project_filter_limit(self, sample_data):
        """Test different operator order: Scan -> Project -> Filter -> Limit"""
        reader = MockReader(sample_data)
        scan = Scan(reader)
        project = Project(scan, ["name", "age", "city"])
        filter_op = Filter(project, [Condition("city", "=", "NYC")])
        limit = Limit(filter_op, 1)

        rows = list(limit)

        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"
        assert set(rows[0].keys()) == {"name", "age", "city"}


class TestEdgeCases:
    """Test edge cases and error handling"""

    def test_filter_with_null_values(self):
        """Test filter with NULL values"""
        data = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": None},
            {"name": "Charlie", "age": 35},
        ]

        reader = MockReader(data)
        scan = Scan(reader)
        filter_op = Filter(scan, [Condition("age", ">", 25)])

        rows = list(filter_op)

        # NULL values should be filtered out
        assert len(rows) == 2
        assert all(row["age"] is not None for row in rows)

    def test_filter_type_mismatch(self):
        """Test filter with type mismatch"""
        data = [
            {"name": "Alice", "age": "thirty"},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]

        reader = MockReader(data)
        scan = Scan(reader)
        # Try to compare string "thirty" > 30
        filter_op = Filter(scan, [Condition("age", ">", 30)])

        rows = list(filter_op)

        # String "thirty" won't match, but numeric 35 will
        assert len(rows) == 1
        assert rows[0]["name"] == "Charlie"

    def test_project_empty_column_list(self):
        """Test project with empty column list"""
        data = [{"name": "Alice", "age": 30}]

        reader = MockReader(data)
        scan = Scan(reader)
        project = Project(scan, [])

        rows = list(project)

        # Should yield empty dicts
        assert len(rows) == 1
        assert rows[0] == {}
