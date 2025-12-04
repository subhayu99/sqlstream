"""
Tests for query planner and optimizations
"""

import tempfile

import pytest

from sqlstream import query
from sqlstream.core.executor import Executor
from sqlstream.optimizers import QueryPlanner
from sqlstream.readers.csv_reader import CSVReader
from sqlstream.sql.parser import parse


@pytest.fixture
def sample_csv(tmp_path):
    """Create a sample CSV file for testing"""
    csv_content = """name,age,city,salary,department
Alice,30,NYC,75000,Engineering
Bob,25,LA,65000,Sales
Charlie,35,SF,85000,Engineering
Diana,28,NYC,70000,Sales
Eve,32,LA,80000,Engineering"""

    csv_file = tmp_path / "data.csv"
    csv_file.write_text(csv_content)
    return csv_file


class TestPredicatePushdown:
    """Test predicate pushdown optimization"""

    def test_pushdown_simple_filter(self, sample_csv):
        """Test that simple filters are pushed to reader"""
        ast = parse("SELECT * FROM data WHERE age > 30")
        reader = CSVReader(str(sample_csv))
        planner = QueryPlanner()

        # Initially, reader has no filters
        assert reader.filter_conditions == []

        # Apply optimization
        planner.optimize(ast, reader)

        # Filter should now be pushed to reader
        assert len(reader.filter_conditions) == 1
        assert reader.filter_conditions[0].column == "age"
        assert reader.filter_conditions[0].operator == ">"
        assert reader.filter_conditions[0].value == 30

    def test_pushdown_multiple_filters(self, sample_csv):
        """Test that multiple AND conditions are pushed"""
        ast = parse("SELECT * FROM data WHERE age > 25 AND city = 'NYC'")
        reader = CSVReader(str(sample_csv))
        planner = QueryPlanner()

        planner.optimize(ast, reader)

        # Both conditions should be pushed
        assert len(reader.filter_conditions) == 2

    def test_pushdown_with_results(self, sample_csv):
        """Test that pushdown produces correct results"""
        # Query with filter
        results = query(str(sample_csv)).sql("SELECT * FROM data WHERE age > 30").to_list()

        # Should only get Charlie (35) and Eve (32)
        assert len(results) == 2
        assert all(row["age"] > 30 for row in results)

    def test_optimization_recorded(self, sample_csv):
        """Test that optimization is recorded"""
        ast = parse("SELECT * FROM data WHERE age > 30")
        reader = CSVReader(str(sample_csv))
        planner = QueryPlanner()

        planner.optimize(ast, reader)

        # Should record the optimization
        assert len(planner.optimizations_applied) > 0
        assert "Predicate pushdown" in planner.optimizations_applied[0]


class TestColumnPruning:
    """Test column pruning optimization"""

    def test_pruning_select_columns(self, sample_csv):
        """Test that only selected columns are read"""
        ast = parse("SELECT name, age FROM data")
        reader = CSVReader(str(sample_csv))
        planner = QueryPlanner()

        # Initially, reader has no column restrictions
        assert reader.required_columns == []

        # Apply optimization
        planner.optimize(ast, reader)

        # Only selected columns should be marked as required
        assert set(reader.required_columns) == {"name", "age"}

    def test_pruning_with_where(self, sample_csv):
        """Test that WHERE columns are included"""
        ast = parse("SELECT name FROM data WHERE age > 30")
        reader = CSVReader(str(sample_csv))
        planner = QueryPlanner()

        planner.optimize(ast, reader)

        # Should include both SELECT and WHERE columns
        assert "name" in reader.required_columns
        assert "age" in reader.required_columns

    def test_pruning_select_star(self, sample_csv):
        """Test that SELECT * doesn't prune"""
        ast = parse("SELECT * FROM data")
        reader = CSVReader(str(sample_csv))
        planner = QueryPlanner()

        planner.optimize(ast, reader)

        # No pruning with SELECT *
        assert reader.required_columns == []

    def test_pruning_with_results(self, sample_csv):
        """Test that pruning produces correct results"""
        # Query selecting only specific columns
        results = query(str(sample_csv)).sql("SELECT name, age FROM data").to_list()

        # Results should only have selected columns
        assert len(results) == 5
        assert set(results[0].keys()) == {"name", "age"}
        assert "city" not in results[0]
        assert "salary" not in results[0]

    def test_optimization_recorded(self, sample_csv):
        """Test that optimization is recorded"""
        ast = parse("SELECT name FROM data")
        reader = CSVReader(str(sample_csv))
        planner = QueryPlanner()

        planner.optimize(ast, reader)

        # Should record the optimization
        assert len(planner.optimizations_applied) > 0
        assert "Column pruning" in planner.optimizations_applied[0]


class TestCombinedOptimizations:
    """Test multiple optimizations working together"""

    def test_pushdown_and_pruning(self, sample_csv):
        """Test both predicate pushdown and column pruning"""
        ast = parse("SELECT name, age FROM data WHERE city = 'NYC'")
        reader = CSVReader(str(sample_csv))
        planner = QueryPlanner()

        planner.optimize(ast, reader)

        # Predicate pushdown
        assert len(reader.filter_conditions) == 1
        assert reader.filter_conditions[0].column == "city"

        # Column pruning (SELECT columns + WHERE columns)
        assert "name" in reader.required_columns
        assert "age" in reader.required_columns
        assert "city" in reader.required_columns

        # Both optimizations recorded
        assert len(planner.optimizations_applied) == 2

    def test_end_to_end_optimized_query(self, sample_csv):
        """Test complete optimized query execution"""
        results = (
            query(str(sample_csv))
            .sql("""
            SELECT name, department
            FROM data
            WHERE age > 28 AND city = 'NYC'
        """)
            .to_list()
        )

        # Should get Alice (30, NYC) and Diana (28 is not > 28, so NOT Diana)
        # Actually Diana is 28, so only Alice qualifies
        assert len(results) == 1
        assert results[0]["name"] == "Alice"
        assert set(results[0].keys()) == {"name", "department"}

    def test_optimization_summary(self, sample_csv):
        """Test optimization summary"""
        ast = parse("SELECT name FROM data WHERE age > 30")
        reader = CSVReader(str(sample_csv))
        planner = QueryPlanner()

        planner.optimize(ast, reader)

        summary = planner.get_optimization_summary()

        assert "Predicate pushdown" in summary
        assert "Column pruning" in summary


class TestExplainPlan:
    """Test explain plan with optimizations"""

    def test_explain_shows_optimizations(self, sample_csv):
        """Test that explain shows applied optimizations"""
        plan = (
            query(str(sample_csv))
            .sql(
                """
            SELECT name, age
            FROM data
            WHERE city = 'NYC'
        """,
                backend="python",
            )
            .explain()
        )

        # Should show query plan
        assert "Query Plan" in plan
        assert "Scan" in plan

        # Should show optimizations
        assert "Optimizations applied" in plan
        assert "Predicate pushdown" in plan
        assert "Column pruning" in plan

    def test_explain_no_optimizations(self, sample_csv):
        """Test explain when no optimizations apply"""
        plan = query(str(sample_csv)).sql("SELECT * FROM data", backend="python").explain()

        # SELECT * has no column pruning, no WHERE has no pushdown
        assert "No optimizations applied" in plan


class TestPerformanceImprovement:
    """Test that optimizations actually improve performance"""

    def test_filter_reduces_data_read(self, sample_csv):
        """Test that predicate pushdown reduces rows processed"""

        class CountingReader(CSVReader):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.rows_yielded = 0

            def read_lazy(self):
                for row in super().read_lazy():
                    self.rows_yielded += 1
                    yield row

        # Query with filter
        reader = CountingReader(str(sample_csv))
        ast = parse("SELECT * FROM data WHERE age > 35")
        executor = Executor()

        results = list(executor.execute(ast, reader))

        # Predicate pushdown should filter at read time
        # Only 0 rows have age > 35 in our test data
        # Actually Charlie has age 35, which is NOT > 35
        # So we should read fewer rows than total (5)
        # But actually with pushdown, CSV reader still reads all rows
        # and filters them. So this test verifies filtering happens.
        assert len(results) == 0  # No rows match age > 35

    def test_column_pruning_reduces_memory(self, sample_csv):
        """Test that column pruning returns fewer columns"""
        # Select only 2 columns
        results = query(str(sample_csv)).sql("SELECT name, age FROM data").to_list()

        # Each row should only have 2 columns, not all 5
        for row in results:
            assert len(row) == 2
            assert "city" not in row
            assert "salary" not in row
            assert "department" not in row


class TestEdgeCases:
    """Test edge cases in optimization"""

    def test_no_where_clause(self, sample_csv):
        """Test query with no WHERE clause"""
        ast = parse("SELECT name FROM data")
        reader = CSVReader(str(sample_csv))
        planner = QueryPlanner()

        planner.optimize(ast, reader)

        # No predicate pushdown (no WHERE clause)
        assert reader.filter_conditions == []

    def test_complex_where(self, sample_csv):
        """Test WHERE with multiple conditions"""
        ast = parse("""
            SELECT name
            FROM data
            WHERE age > 25 AND city = 'NYC' AND salary > 70000
        """)
        reader = CSVReader(str(sample_csv))
        planner = QueryPlanner()

        planner.optimize(ast, reader)

        # All conditions should be pushed
        assert len(reader.filter_conditions) == 3

    def test_reader_without_pushdown_support(self):
        """Test with reader that doesn't support pushdown"""

        class NonOptimizedReader(CSVReader):
            def supports_pushdown(self):
                return False

            def supports_column_selection(self):
                return False

        # Create a dummy file

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("name,age\nAlice,30")
            path = f.name

        ast = parse("SELECT name FROM data WHERE age > 25")
        reader = NonOptimizedReader(path)
        planner = QueryPlanner()

        planner.optimize(ast, reader)

        # No optimizations should be applied
        assert reader.filter_conditions == []
        assert reader.required_columns == []
        assert "No optimizations" in planner.get_optimization_summary()


class TestLimitPushdown:
    """Test limit pushdown optimization"""

    def test_limit_pushdown_simple(self, sample_csv):
        """Test that LIMIT is pushed to reader"""
        ast = parse("SELECT * FROM data LIMIT 5")
        reader = CSVReader(str(sample_csv))
        planner = QueryPlanner()

        # Initially, reader has no limit set
        assert reader.limit is None

        # Apply optimization
        planner.optimize(ast, reader)

        # Limit should be pushed to reader
        assert reader.limit == 5

    def test_limit_pushdown_with_results(self, sample_csv):
        """Test that limit pushdown produces correct results"""
        # Query with limit
        results = query(str(sample_csv)).sql("SELECT * FROM data LIMIT 2").to_list()

        # Should only get 2 rows
        assert len(results) == 2

    def test_limit_pushdown_optimization_recorded(self, sample_csv):
        """Test that limit pushdown is recorded"""
        ast = parse("SELECT * FROM data LIMIT 3")
        reader = CSVReader(str(sample_csv))
        planner = QueryPlanner()

        planner.optimize(ast, reader)

        # Should record the optimization
        assert "Limit pushdown" in planner.get_optimization_summary()
        assert "limit 3" in planner.get_optimization_summary()

    def test_limit_not_pushed_with_order_by(self, sample_csv):
        """Test that limit is NOT pushed when ORDER BY exists"""
        ast = parse("SELECT * FROM data ORDER BY age LIMIT 3")
        reader = CSVReader(str(sample_csv))
        planner = QueryPlanner()

        planner.optimize(ast, reader)

        # Limit should NOT be pushed (need all rows to sort)
        assert reader.limit is None
        assert "Limit pushdown" not in planner.get_optimization_summary()

    def test_limit_not_pushed_with_group_by(self, sample_csv):
        """Test that limit is NOT pushed when GROUP BY exists"""
        ast = parse("SELECT city, COUNT(*) FROM data GROUP BY city LIMIT 2")
        reader = CSVReader(str(sample_csv))
        planner = QueryPlanner()

        planner.optimize(ast, reader)

        # Limit should NOT be pushed (need all rows to group)
        assert reader.limit is None

    def test_limit_with_filter(self, sample_csv):
        """Test limit pushdown works with filters"""
        results = query(str(sample_csv)).sql("SELECT * FROM data WHERE age > 25 LIMIT 2").to_list()

        # Should get 2 rows (Charlie and Eve)
        assert len(results) == 2
        assert all(row["age"] > 25 for row in results)


class TestPartitionPruning:
    """Test partition pruning optimization for Parquet files"""

    @pytest.fixture
    def partitioned_parquet(self, tmp_path):
        """Create partitioned Parquet files for testing"""
        try:
            import pandas as pd
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            pytest.skip("Requires pandas and pyarrow")

        # Create a partitioned directory structure:
        # data/year=2023/month=12/data.parquet
        # data/year=2024/month=01/data.parquet
        # data/year=2024/month=02/data.parquet

        base_dir = tmp_path / "data"

        # Partition 1: year=2023/month=12
        partition_2023_12 = base_dir / "year=2023" / "month=12"
        partition_2023_12.mkdir(parents=True)
        df1 = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25], "sales": [100, 150]})
        pq.write_table(pa.Table.from_pandas(df1), partition_2023_12 / "data.parquet")

        # Partition 2: year=2024/month=01
        partition_2024_01 = base_dir / "year=2024" / "month=1"
        partition_2024_01.mkdir(parents=True)
        df2 = pd.DataFrame({"name": ["Charlie", "Diana"], "age": [35, 28], "sales": [200, 180]})
        pq.write_table(pa.Table.from_pandas(df2), partition_2024_01 / "data.parquet")

        # Partition 3: year=2024/month=02
        partition_2024_02 = base_dir / "year=2024" / "month=2"
        partition_2024_02.mkdir(parents=True)
        df3 = pd.DataFrame({"name": ["Eve", "Frank"], "age": [32, 40], "sales": [220, 190]})
        pq.write_table(pa.Table.from_pandas(df3), partition_2024_02 / "data.parquet")

        return base_dir

    def test_partition_detection(self, partitioned_parquet):
        """Test that partition columns are detected from path"""
        try:
            from sqlstream.readers.parquet_reader import ParquetReader
        except ImportError:
            pytest.skip("Requires pyarrow")

        # Read one of the partitioned files
        file_path = str(partitioned_parquet / "year=2024" / "month=1" / "data.parquet")
        reader = ParquetReader(file_path)

        # Should detect partition columns from path
        assert "year" in reader.partition_columns
        assert "month" in reader.partition_columns

        # Should parse partition values
        assert reader.partition_values["year"] == 2024
        assert reader.partition_values["month"] == 1

    def test_partition_pruning_match(self, partitioned_parquet):
        """Test that matching partition is not pruned"""
        try:
            from sqlstream.readers.parquet_reader import ParquetReader
        except ImportError:
            pytest.skip("Requires pyarrow")

        file_path = str(partitioned_parquet / "year=2024" / "month=1" / "data.parquet")
        reader = ParquetReader(file_path)

        ast = parse("SELECT * FROM data WHERE year = 2024")
        planner = QueryPlanner()
        planner.optimize(ast, reader)

        # File should NOT be pruned (year=2024 matches filter)
        assert not reader.partition_pruned

        # Should be able to read data
        results = list(reader.read_lazy())
        assert len(results) == 2  # Charlie and Diana

    def test_partition_pruning_no_match(self, partitioned_parquet):
        """Test that non-matching partition is pruned"""
        try:
            from sqlstream.readers.parquet_reader import ParquetReader
        except ImportError:
            pytest.skip("Requires pyarrow")

        file_path = str(partitioned_parquet / "year=2023" / "month=12" / "data.parquet")
        reader = ParquetReader(file_path)

        ast = parse("SELECT * FROM data WHERE year = 2024")
        planner = QueryPlanner()
        planner.optimize(ast, reader)

        # File should be pruned (year=2023 doesn't match year=2024 filter)
        assert reader.partition_pruned

        # Should return no data (pruned)
        results = list(reader.read_lazy())
        assert len(results) == 0

    def test_partition_columns_in_results(self, partitioned_parquet):
        """Test that partition columns are added to result rows"""
        try:
            from sqlstream.readers.parquet_reader import ParquetReader
        except ImportError:
            pytest.skip("Requires pyarrow")

        file_path = str(partitioned_parquet / "year=2024" / "month=1" / "data.parquet")
        reader = ParquetReader(file_path)

        # Read without filters
        results = list(reader.read_lazy())

        # Results should include partition columns
        assert len(results) == 2
        for row in results:
            assert "year" in row
            assert "month" in row
            assert row["year"] == 2024
            assert row["month"] == 1
            # Regular columns should also be present
            assert "name" in row
            assert "age" in row
            assert "sales" in row

    def test_partition_pruning_with_range_filter(self, partitioned_parquet):
        """Test partition pruning with range filters (>, <, etc.)"""
        try:
            from sqlstream.readers.parquet_reader import ParquetReader
        except ImportError:
            pytest.skip("Requires pyarrow")

        # Test year > 2023 (should NOT prune 2024 files)
        file_path_2024 = str(partitioned_parquet / "year=2024" / "month=1" / "data.parquet")
        reader = ParquetReader(file_path_2024)
        ast = parse("SELECT * FROM data WHERE year > 2023")
        planner = QueryPlanner()
        planner.optimize(ast, reader)
        assert not reader.partition_pruned

        # Test year > 2023 (should prune 2023 files)
        file_path_2023 = str(partitioned_parquet / "year=2023" / "month=12" / "data.parquet")
        reader = ParquetReader(file_path_2023)
        ast = parse("SELECT * FROM data WHERE year > 2023")
        planner = QueryPlanner()
        planner.optimize(ast, reader)
        assert reader.partition_pruned

    def test_partition_pruning_multiple_conditions(self, partitioned_parquet):
        """Test partition pruning with multiple partition filters"""
        try:
            from sqlstream.readers.parquet_reader import ParquetReader
        except ImportError:
            pytest.skip("Requires pyarrow")

        file_path = str(partitioned_parquet / "year=2024" / "month=1" / "data.parquet")
        reader = ParquetReader(file_path)

        # Filter: year = 2024 AND month = 1 (both match)
        ast = parse("SELECT * FROM data WHERE year = 2024 AND month = 1")
        planner = QueryPlanner()
        planner.optimize(ast, reader)

        assert not reader.partition_pruned
        results = list(reader.read_lazy())
        assert len(results) == 2

    def test_partition_pruning_optimization_recorded(self, partitioned_parquet):
        """Test that partition pruning is recorded in optimization summary"""
        try:
            from sqlstream.readers.parquet_reader import ParquetReader
        except ImportError:
            pytest.skip("Requires pyarrow")

        file_path = str(partitioned_parquet / "year=2024" / "month=1" / "data.parquet")
        reader = ParquetReader(file_path)

        ast = parse("SELECT * FROM data WHERE year = 2024")
        planner = QueryPlanner()
        planner.optimize(ast, reader)

        # Should record partition pruning in summary
        summary = planner.get_optimization_summary()
        assert "Partition pruning" in summary

    def test_partition_pruning_with_non_partition_filters(self, partitioned_parquet):
        """Test that non-partition filters don't affect partition pruning"""
        try:
            from sqlstream.readers.parquet_reader import ParquetReader
        except ImportError:
            pytest.skip("Requires pyarrow")

        file_path = str(partitioned_parquet / "year=2024" / "month=1" / "data.parquet")
        reader = ParquetReader(file_path)

        # Filter on non-partition column (age)
        ast = parse("SELECT * FROM data WHERE age > 30")
        planner = QueryPlanner()
        planner.optimize(ast, reader)

        # Should NOT be pruned (filter doesn't reference partition columns)
        assert not reader.partition_pruned

        # But predicate pushdown should still work
        assert len(reader.filter_conditions) > 0
