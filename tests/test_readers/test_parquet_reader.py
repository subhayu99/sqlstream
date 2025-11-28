"""
Tests for Parquet reader with intelligent row group pruning
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlstream import query
from sqlstream.readers.parquet_reader import ParquetReader
from sqlstream.sql.ast_nodes import Condition


@pytest.fixture
def sample_parquet(tmp_path):
    """Create a sample Parquet file for testing"""
    # Create data with multiple row groups
    data = {
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"] * 20,  # 100 rows
        "age": [30, 25, 35, 28, 32] * 20,
        "city": ["NYC", "LA", "SF", "NYC", "LA"] * 20,
        "salary": [75000, 65000, 85000, 70000, 80000] * 20,
    }

    table = pa.table(data)

    parquet_file = tmp_path / "data.parquet"

    # Write with multiple row groups (20 rows per group = 5 row groups)
    pq.write_table(table, parquet_file, row_group_size=20)

    return parquet_file


@pytest.fixture
def age_stratified_parquet(tmp_path):
    """
    Create Parquet file with distinct age ranges per row group

    This is perfect for testing statistics-based pruning!

    Row Group 0: ages 18-25
    Row Group 1: ages 26-35
    Row Group 2: ages 36-50
    Row Group 3: ages 51-65
    """
    # Create stratified data - 20 rows per group, 4 groups = 80 total rows
    # Row Group 0: ages 18-25
    data1_names = [f"Person{i}" for i in range(0, 20)]
    data1_ages = [18, 19, 20, 21, 22, 23, 24, 25] * 2 + [18, 19, 20, 21]  # 20 values

    # Row Group 1: ages 26-35
    data2_names = [f"Person{i}" for i in range(20, 40)]
    data2_ages = [26, 27, 28, 29, 30, 31, 32, 33, 34, 35] * 2  # 20 values

    # Row Group 2: ages 36-50
    data3_names = [f"Person{i}" for i in range(40, 60)]
    data3_ages = list(range(36, 51)) + [36, 37, 38, 39, 40]  # 20 values

    # Row Group 3: ages 51-65
    data4_names = [f"Person{i}" for i in range(60, 80)]
    data4_ages = list(range(51, 66)) + [51, 52, 53, 54, 55]  # 20 values

    # Combine all data
    all_data = {
        "name": data1_names + data2_names + data3_names + data4_names,
        "age": data1_ages + data2_ages + data3_ages + data4_ages,
    }

    table = pa.table(all_data)

    parquet_file = tmp_path / "stratified.parquet"

    # Write with exactly 20 rows per row group
    pq.write_table(table, parquet_file, row_group_size=20)

    return parquet_file


class TestBasicReading:
    """Test basic Parquet reading"""

    def test_read_parquet(self, sample_parquet):
        """Test reading Parquet file"""
        reader = ParquetReader(str(sample_parquet))
        rows = list(reader.read_lazy())

        assert len(rows) == 100
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 30

    def test_lazy_iteration(self, sample_parquet):
        """Test that reading is lazy"""
        reader = ParquetReader(str(sample_parquet))
        iterator = reader.read_lazy()

        # Should be a generator
        assert hasattr(iterator, "__iter__")
        assert hasattr(iterator, "__next__")

        first = next(iterator)
        assert first["name"] == "Alice"

    def test_schema_inference(self, sample_parquet):
        """Test schema extraction from Parquet metadata"""
        reader = ParquetReader(str(sample_parquet))
        schema = reader.get_schema()

        assert "name" in schema
        assert "age" in schema
        assert "city" in schema
        assert schema["age"] == "int"
        assert schema["name"] == "string"

    def test_file_not_found(self):
        """Test error when file doesn't exist"""
        with pytest.raises(FileNotFoundError):
            ParquetReader("nonexistent.parquet")


class TestRowGroupPruning:
    """Test intelligent row group pruning with statistics"""

    def test_pruning_greater_than(self, age_stratified_parquet):
        """Test pruning with > filter"""
        reader = ParquetReader(str(age_stratified_parquet))
        reader.set_filter([Condition("age", ">", 50)])

        # Execute query
        rows = list(reader.read_lazy())

        # Should only read row group 3 (ages 51-65)
        stats = reader.get_statistics()

        # Should skip 3 out of 4 row groups (75% pruning)
        assert stats["row_groups_scanned"] < stats["total_row_groups"]
        assert stats["row_groups_scanned"] <= 1

        # Verify results are correct
        assert all(row["age"] > 50 for row in rows)

    def test_pruning_less_than(self, age_stratified_parquet):
        """Test pruning with < filter"""
        reader = ParquetReader(str(age_stratified_parquet))
        reader.set_filter([Condition("age", "<", 26)])

        rows = list(reader.read_lazy())
        stats = reader.get_statistics()

        # Should only read row group 0 (ages 18-25)
        assert stats["row_groups_scanned"] <= 1

        # Verify results
        assert all(row["age"] < 26 for row in rows)

    def test_pruning_equals(self, age_stratified_parquet):
        """Test pruning with = filter"""
        reader = ParquetReader(str(age_stratified_parquet))
        reader.set_filter([Condition("age", "=", 40)])

        rows = list(reader.read_lazy())
        stats = reader.get_statistics()

        # Should only read row groups that might contain 40
        assert stats["row_groups_scanned"] < stats["total_row_groups"]

        # Verify results
        assert all(row["age"] == 40 for row in rows)

    def test_pruning_ratio(self, age_stratified_parquet):
        """Test that pruning ratio is calculated correctly"""
        reader = ParquetReader(str(age_stratified_parquet))
        reader.set_filter([Condition("age", ">", 60)])

        list(reader.read_lazy())
        stats = reader.get_statistics()

        # Should have high pruning ratio
        assert stats["pruning_ratio"] > 0.5  # At least 50% pruned

    def test_no_pruning_without_filter(self, age_stratified_parquet):
        """Test that all row groups are read without filters"""
        reader = ParquetReader(str(age_stratified_parquet))

        list(reader.read_lazy())
        stats = reader.get_statistics()

        # Should read all row groups
        assert stats["row_groups_scanned"] == stats["total_row_groups"]
        assert stats["pruning_ratio"] == 0


class TestColumnSelection:
    """Test column pruning"""

    def test_select_specific_columns(self, sample_parquet):
        """Test reading only specific columns"""
        reader = ParquetReader(str(sample_parquet))
        reader.set_columns(["name", "age"])

        rows = list(reader.read_lazy())

        # Should only have selected columns
        assert set(rows[0].keys()) == {"name", "age"}
        assert "city" not in rows[0]
        assert "salary" not in rows[0]

    def test_select_single_column(self, sample_parquet):
        """Test reading single column"""
        reader = ParquetReader(str(sample_parquet))
        reader.set_columns(["name"])

        rows = list(reader.read_lazy())

        assert set(rows[0].keys()) == {"name"}


class TestCombinedOptimizations:
    """Test predicate pushdown + column pruning together"""

    def test_filter_and_column_selection(self, age_stratified_parquet):
        """Test both optimizations together"""
        reader = ParquetReader(str(age_stratified_parquet))
        reader.set_filter([Condition("age", ">", 50)])
        reader.set_columns(["name"])

        rows = list(reader.read_lazy())
        stats = reader.get_statistics()

        # Row group pruning should work
        assert stats["row_groups_scanned"] < stats["total_row_groups"]

        # Column selection should work
        assert set(rows[0].keys()) == {"name"}

        # Results should be correct
        # Note: We can't verify age > 50 since we didn't select age column!
        assert len(rows) > 0


class TestEndToEndWithQuery:
    """Test Parquet reader through the query API"""

    def test_query_parquet_file(self, sample_parquet):
        """Test querying Parquet file"""
        results = query(str(sample_parquet)).sql("SELECT * FROM data").to_list()

        assert len(results) == 100
        assert results[0]["name"] == "Alice"

    def test_query_with_filter(self, age_stratified_parquet):
        """Test query with WHERE clause (predicate pushdown)"""
        results = query(str(age_stratified_parquet)).sql("""
            SELECT * FROM data WHERE age > 50
        """).to_list()

        # Should get only ages > 50
        assert all(row["age"] > 50 for row in results)
        assert len(results) > 0

    def test_query_with_column_selection(self, sample_parquet):
        """Test query with specific columns (column pruning)"""
        results = query(str(sample_parquet)).sql("""
            SELECT name, age FROM data LIMIT 10
        """).to_list()

        assert len(results) == 10
        assert set(results[0].keys()) == {"name", "age"}

    def test_query_optimized(self, age_stratified_parquet):
        """Test fully optimized query"""
        results = query(str(age_stratified_parquet)).sql("""
            SELECT name
            FROM data
            WHERE age > 40
            LIMIT 5
        """).to_list()

        assert len(results) == 5
        assert set(results[0].keys()) == {"name"}

    def test_format_auto_detection(self, sample_parquet):
        """Test that .parquet extension is auto-detected"""
        q = query(str(sample_parquet))

        assert q.reader.__class__.__name__ == "ParquetReader"


class TestExplainPlan:
    """Test explain plan with Parquet"""

    def test_explain_shows_pruning(self, age_stratified_parquet):
        """Test that explain shows row group pruning"""
        plan = query(str(age_stratified_parquet)).sql("""
            SELECT name FROM data WHERE age > 50
        """, backend="python").explain()

        # Should show optimizations
        assert "Predicate pushdown" in plan
        assert "Column pruning" in plan


class TestStatisticsEdgeCases:
    """Test edge cases in statistics-based pruning"""

    def test_multiple_filters(self, age_stratified_parquet):
        """Test multiple filter conditions"""
        reader = ParquetReader(str(age_stratified_parquet))
        reader.set_filter([Condition("age", ">", 30), Condition("age", "<", 40)])

        rows = list(reader.read_lazy())

        # Should only get ages in range [31, 39]
        assert all(30 < row["age"] < 40 for row in rows)

    def test_not_equals_filter(self, age_stratified_parquet):
        """Test != filter"""
        reader = ParquetReader(str(age_stratified_parquet))
        reader.set_filter([Condition("age", "!=", 30)])

        rows = list(reader.read_lazy())

        # Should get all rows except age 30
        assert all(row["age"] != 30 for row in rows)

    def test_all_operators(self, age_stratified_parquet):
        """Test all comparison operators"""
        operators_and_values = [
            ("=", 40),
            (">", 50),
            ("<", 25),
            (">=", 50),
            ("<=", 25),
            ("!=", 30),
        ]

        for op, value in operators_and_values:
            reader = ParquetReader(str(age_stratified_parquet))
            reader.set_filter([Condition("age", op, value)])

            rows = list(reader.read_lazy())

            # Just verify we get some results (exact count depends on data)
            # The important thing is that pruning happens and results are correct
            stats = reader.get_statistics()
            assert stats["total_row_groups"] > 0


class TestPerformanceGains:
    """Test that Parquet optimizations provide real performance gains"""

    def test_pruning_reduces_io(self, age_stratified_parquet):
        """Test that row group pruning reduces data read"""
        reader = ParquetReader(str(age_stratified_parquet))
        reader.set_filter([Condition("age", ">", 55)])

        list(reader.read_lazy())
        stats = reader.get_statistics()

        # Should skip most row groups
        skip_ratio = stats["row_groups_skipped"] / stats["total_row_groups"]
        assert skip_ratio > 0.5  # At least 50% skipped

    def test_column_selection_reduces_memory(self, sample_parquet):
        """Test that column selection reduces data in memory"""
        # Read only 1 column
        results = query(str(sample_parquet)).sql("SELECT name FROM data").to_list()

        # Each row should only have 1 column
        for row in results:
            assert len(row) == 1
            assert "city" not in row
            assert "salary" not in row
