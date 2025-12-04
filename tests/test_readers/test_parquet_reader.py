"""
Tests for Parquet reader with intelligent row group pruning
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sqlstream import query
from sqlstream.core.types import DataType
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

        assert schema.get_column_type("age") == DataType.INTEGER
        assert schema.get_column_type("name") == DataType.STRING

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


class TestArrowTypeMapping:
    """Test Arrow type to SQLStream DataType conversion"""

    def test_integer_types(self, tmp_path):
        """Test various Arrow integer types"""
        # Create Parquet with different int types
        data = {
            "int8_col": pa.array([1, 2, 3], type=pa.int8()),
            "int16_col": pa.array([100, 200, 300], type=pa.int16()),
            "int32_col": pa.array([1000, 2000, 3000], type=pa.int32()),
            "int64_col": pa.array([10000, 20000, 30000], type=pa.int64()),
            "uint8_col": pa.array([1, 2, 3], type=pa.uint8()),
        }
        table = pa.table(data)
        parquet_file = tmp_path / "int_types.parquet"
        pq.write_table(table, parquet_file)

        # Test schema inference
        reader = ParquetReader(str(parquet_file))
        schema = reader.get_schema()

        # All should map to INTEGER
        assert schema.get_column_type("int8_col") == DataType.INTEGER
        assert schema.get_column_type("int16_col") == DataType.INTEGER
        assert schema.get_column_type("int32_col") == DataType.INTEGER
        assert schema.get_column_type("int64_col") == DataType.INTEGER
        assert schema.get_column_type("uint8_col") == DataType.INTEGER

    def test_float_types(self, tmp_path):
        """Test Arrow float and double types"""
        data = {
            "float32_col": pa.array([1.5, 2.5, 3.5], type=pa.float32()),
            "float64_col": pa.array([10.5, 20.5, 30.5], type=pa.float64()),
        }
        table = pa.table(data)
        parquet_file = tmp_path / "float_types.parquet"
        pq.write_table(table, parquet_file)

        reader = ParquetReader(str(parquet_file))
        schema = reader.get_schema()

        assert schema.get_column_type("float32_col") == DataType.FLOAT
        assert schema.get_column_type("float64_col") == DataType.FLOAT

    def test_string_types(self, tmp_path):
        """Test Arrow string types"""
        data = {
            "string_col": pa.array(["foo", "bar", "baz"], type=pa.string()),
            "utf8_col": pa.array(["hello", "world", "test"], type=pa.utf8()),
        }
        table = pa.table(data)
        parquet_file = tmp_path / "string_types.parquet"
        pq.write_table(table, parquet_file)

        reader = ParquetReader(str(parquet_file))
        schema = reader.get_schema()

        assert schema.get_column_type("string_col") == DataType.STRING
        assert schema.get_column_type("utf8_col") == DataType.STRING

    def test_boolean_type(self, tmp_path):
        """Test Arrow boolean type"""
        data = {"bool_col": pa.array([True, False, True], type=pa.bool_())}
        table = pa.table(data)
        parquet_file = tmp_path / "bool_types.parquet"
        pq.write_table(table, parquet_file)

        reader = ParquetReader(str(parquet_file))
        schema = reader.get_schema()

        assert schema.get_column_type("bool_col") == DataType.BOOLEAN

    def test_date_type(self, tmp_path):
        """Test Arrow date type"""
        from datetime import date

        data = {
            "date_col": pa.array(
                [date(2024, 1, 1), date(2024, 6, 15), date(2024, 12, 31)],
                type=pa.date32(),
            )
        }
        table = pa.table(data)
        parquet_file = tmp_path / "date_types.parquet"
        pq.write_table(table, parquet_file)

        reader = ParquetReader(str(parquet_file))
        schema = reader.get_schema()

        assert schema.get_column_type("date_col") == DataType.DATE

    def test_timestamp_type(self, tmp_path):
        """Test Arrow timestamp type"""
        from datetime import datetime

        data = {
            "timestamp_col": pa.array(
                [
                    datetime(2024, 1, 1, 12, 0, 0),
                    datetime(2024, 6, 15, 14, 30, 0),
                    datetime(2024, 12, 31, 23, 59, 59),
                ],
                type=pa.timestamp("us"),
            )
        }
        table = pa.table(data)
        parquet_file = tmp_path / "timestamp_types.parquet"
        pq.write_table(table, parquet_file)

        reader = ParquetReader(str(parquet_file))
        schema = reader.get_schema()

        assert schema.get_column_type("timestamp_col") == DataType.DATETIME

    def test_decimal_type(self, tmp_path):
        """Test Arrow decimal type"""
        from decimal import Decimal

        data = {
            "decimal_col": pa.array(
                [Decimal("123.45"), Decimal("678.90"), Decimal("999.99")],
                type=pa.decimal128(10, 2),
            )
        }
        table = pa.table(data)
        parquet_file = tmp_path / "decimal_types.parquet"
        pq.write_table(table, parquet_file)

        reader = ParquetReader(str(parquet_file))
        schema = reader.get_schema()

        assert schema.get_column_type("decimal_col") == DataType.DECIMAL


class TestComplexDataTypes:
    """Test reading Parquet files with complex/nested data types"""

    def test_list_type(self, tmp_path):
        """Test reading list/array columns"""
        data = {
            "name": ["Alice", "Bob", "Charlie"],
            "scores": [[85, 90, 95], [70, 75, 80], [95, 98, 100]],
        }
        table = pa.table(data)
        parquet_file = tmp_path / "list_data.parquet"
        pq.write_table(table, parquet_file)

        reader = ParquetReader(str(parquet_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 3
        assert rows[0]["name"] == "Alice"
        assert rows[0]["scores"] == [85, 90, 95]
        assert rows[1]["scores"] == [70, 75, 80]

    def test_struct_type(self, tmp_path):
        """Test reading struct columns"""
        # Create struct type data
        struct_array = pa.array(
            [
                {"city": "NYC", "zip": 10001},
                {"city": "LA", "zip": 90001},
                {"city": "SF", "zip": 94102},
            ],
            type=pa.struct([("city", pa.string()), ("zip", pa.int32())]),
        )

        data = {"name": ["Alice", "Bob", "Charlie"], "address": struct_array}

        table = pa.table(data)
        parquet_file = tmp_path / "struct_data.parquet"
        pq.write_table(table, parquet_file)

        reader = ParquetReader(str(parquet_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 3
        assert rows[0]["name"] == "Alice"
        # Struct should be preserved as dict
        assert rows[0]["address"]["city"] == "NYC"
        assert rows[0]["address"]["zip"] == 10001

    def test_nested_lists(self, tmp_path):
        """Test reading nested list structures"""
        data = {
            "name": ["Alice", "Bob"],
            "matrix": [[[1, 2], [3, 4]], [[5, 6], [7, 8]]],
        }
        table = pa.table(data)
        parquet_file = tmp_path / "nested_list.parquet"
        pq.write_table(table, parquet_file)

        reader = ParquetReader(str(parquet_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 2
        assert rows[0]["matrix"] == [[1, 2], [3, 4]]
        assert rows[1]["matrix"] == [[5, 6], [7, 8]]


class TestCompressionCodecs:
    """Test different Parquet compression codecs"""

    def test_snappy_compression(self, tmp_path):
        """Test reading Snappy-compressed Parquet"""
        data = {"name": ["Alice", "Bob", "Charlie"] * 100, "age": [30, 25, 35] * 100}
        table = pa.table(data)
        parquet_file = tmp_path / "snappy.parquet"
        pq.write_table(table, parquet_file, compression="SNAPPY")

        reader = ParquetReader(str(parquet_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 300
        assert rows[0]["name"] == "Alice"

    def test_gzip_compression(self, tmp_path):
        """Test reading GZIP-compressed Parquet"""
        data = {"name": ["Alice", "Bob", "Charlie"] * 100, "age": [30, 25, 35] * 100}
        table = pa.table(data)
        parquet_file = tmp_path / "gzip.parquet"
        pq.write_table(table, parquet_file, compression="GZIP")

        reader = ParquetReader(str(parquet_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 300
        assert rows[0]["name"] == "Alice"

    def test_uncompressed(self, tmp_path):
        """Test reading uncompressed Parquet"""
        data = {"name": ["Alice", "Bob", "Charlie"], "age": [30, 25, 35]}
        table = pa.table(data)
        parquet_file = tmp_path / "uncompressed.parquet"
        pq.write_table(table, parquet_file, compression="NONE")

        reader = ParquetReader(str(parquet_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 3
        assert rows[0]["name"] == "Alice"

    def test_compression_with_filters(self, tmp_path):
        """Test that compression works with predicate pushdown"""
        data = {"name": ["Alice", "Bob", "Charlie"] * 100, "age": [30, 25, 35] * 100}
        table = pa.table(data)
        parquet_file = tmp_path / "compressed_filtered.parquet"
        pq.write_table(table, parquet_file, compression="SNAPPY", row_group_size=50)

        reader = ParquetReader(str(parquet_file))
        reader.set_filter([Condition("age", ">", 27)])

        rows = list(reader.read_lazy())

        # Should get Alice (30) and Charlie (35), but not Bob (25)
        ages = [row["age"] for row in rows]
        assert all(age > 27 for age in ages)
        assert 25 not in ages


class TestErrorHandling:
    """Test error handling for various failure scenarios"""

    def test_corrupted_file(self, tmp_path):
        """Test reading corrupted Parquet file"""
        parquet_file = tmp_path / "corrupted.parquet"
        # Write garbage data
        with open(parquet_file, "wb") as f:
            f.write(b"This is not a valid Parquet file!")

        with pytest.raises(Exception):  # PyArrow raises various exceptions
            ParquetReader(str(parquet_file))

    def test_empty_file(self, tmp_path):
        """Test reading empty Parquet file"""
        parquet_file = tmp_path / "empty.parquet"
        # Create empty file
        parquet_file.touch()

        with pytest.raises(Exception):
            ParquetReader(str(parquet_file))

    def test_missing_column_in_filter(self, sample_parquet):
        """Test filter on non-existent column"""
        reader = ParquetReader(str(sample_parquet))
        reader.set_filter([Condition("nonexistent_column", "=", 42)])

        # Should not crash, just return no results or all results
        rows = list(reader.read_lazy())
        # The implementation may vary - just ensure it doesn't crash
        assert isinstance(rows, list)

    def test_type_mismatch_in_filter(self, sample_parquet):
        """Test filter with incompatible type"""
        reader = ParquetReader(str(sample_parquet))
        # Try to compare age (int) with string
        reader.set_filter([Condition("age", "=", "not_a_number")])

        # Should handle gracefully (no match, or error)
        try:
            rows = list(reader.read_lazy())
            # If it succeeds, should return empty or handle gracefully
            assert isinstance(rows, list)
        except (TypeError, ValueError):
            # Acceptable to raise type error
            pass

    def test_invalid_column_selection(self, sample_parquet):
        """Test selecting non-existent columns"""
        reader = ParquetReader(str(sample_parquet))
        reader.set_columns(["nonexistent_col"])

        # Should handle gracefully
        rows = list(reader.read_lazy())
        # May return empty rows or None values
        assert isinstance(rows, list)


class TestLargeFiles:
    """Test handling of large Parquet files"""

    def test_many_row_groups(self, tmp_path):
        """Test file with many row groups"""
        # Create file with 100 small row groups
        data = {"id": list(range(10000)), "value": list(range(10000))}
        table = pa.table(data)
        parquet_file = tmp_path / "many_groups.parquet"
        # 100 rows per group = 100 row groups
        pq.write_table(table, parquet_file, row_group_size=100)

        reader = ParquetReader(str(parquet_file))
        assert reader.total_row_groups == 100

        # Filter should prune many groups
        reader.set_filter([Condition("id", ">", 9500)])
        rows = list(reader.read_lazy())

        stats = reader.get_statistics()
        # Should skip most row groups
        assert stats["row_groups_scanned"] < 10
        assert len(rows) == 499  # ids 9501-9999

    def test_many_columns(self, tmp_path):
        """Test file with many columns"""
        # Create 100 columns
        data = {f"col_{i}": list(range(100)) for i in range(100)}
        table = pa.table(data)
        parquet_file = tmp_path / "many_cols.parquet"
        pq.write_table(table, parquet_file)

        reader = ParquetReader(str(parquet_file))
        schema = reader.get_schema()

        # Should have all 100 columns
        assert len(schema.columns) == 100

        # Column selection should work
        reader.set_columns(["col_0", "col_99"])
        rows = list(reader.read_lazy())
        assert set(rows[0].keys()) == {"col_0", "col_99"}

    def test_wide_rows(self, tmp_path):
        """Test file with very wide rows (many columns)"""
        # Create 50 columns with varied types
        data = {}
        for i in range(50):
            if i % 3 == 0:
                data[f"str_col_{i}"] = [f"value_{j}" for j in range(100)]
            elif i % 3 == 1:
                data[f"int_col_{i}"] = list(range(100))
            else:
                data[f"float_col_{i}"] = [float(j) for j in range(100)]

        table = pa.table(data)
        parquet_file = tmp_path / "wide_rows.parquet"
        pq.write_table(table, parquet_file)

        reader = ParquetReader(str(parquet_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 100
        # Each row should have 50 columns
        assert len(rows[0]) == 50
