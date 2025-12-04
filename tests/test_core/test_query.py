"""
Tests for main Query API
"""

import pytest

from sqlstream import query
from sqlstream.core.query import Query


@pytest.fixture
def sample_csv(tmp_path):
    """Create a sample CSV file for testing"""
    csv_content = """name,age,city,salary
Alice,30,NYC,75000
Bob,25,LA,65000
Charlie,35,SF,85000
Diana,28,NYC,70000
Eve,32,LA,80000"""

    csv_file = tmp_path / "data.csv"
    csv_file.write_text(csv_content)
    return csv_file


class TestQueryAPI:
    """Test main query API"""

    def test_basic_query(self, sample_csv):
        """Test basic query construction"""
        q = query(str(sample_csv))

        assert isinstance(q, Query)
        assert q.source == str(sample_csv)

    def test_sql_method(self, sample_csv):
        """Test .sql() method"""
        result = query(str(sample_csv)).sql("SELECT * FROM data")

        # Should return QueryResult
        rows = list(result)

        assert len(rows) == 5
        assert rows[0]["name"] == "Alice"

    def test_fluent_api(self, sample_csv):
        """Test fluent API chaining"""
        # Should be able to chain query() -> sql() -> iterate
        rows = list(query(str(sample_csv)).sql("SELECT * FROM data"))

        assert len(rows) == 5

    def test_to_list(self, sample_csv):
        """Test .to_list() method"""
        results = query(str(sample_csv)).sql("SELECT * FROM data").to_list()

        assert isinstance(results, list)
        assert len(results) == 5


class TestEndToEndQueries:
    """Test end-to-end query execution"""

    def test_select_all(self, sample_csv):
        """Test SELECT *"""
        results = query(str(sample_csv)).sql("SELECT * FROM data").to_list()

        assert len(results) == 5
        assert all("name" in row for row in results)

    def test_select_columns(self, sample_csv):
        """Test SELECT specific columns"""
        results = query(str(sample_csv)).sql("SELECT name, age FROM data").to_list()

        assert len(results) == 5
        assert set(results[0].keys()) == {"name", "age"}

    def test_where_clause(self, sample_csv):
        """Test WHERE clause"""
        results = query(str(sample_csv)).sql("SELECT * FROM data WHERE age > 30").to_list()

        assert len(results) == 2
        assert all(row["age"] > 30 for row in results)

    def test_limit_clause(self, sample_csv):
        """Test LIMIT clause"""
        results = query(str(sample_csv)).sql("SELECT * FROM data LIMIT 3").to_list()

        assert len(results) == 3

    def test_complex_query(self, sample_csv):
        """Test complex query with all clauses"""
        results = (
            query(str(sample_csv))
            .sql("""
            SELECT name, salary
            FROM data
            WHERE city = 'NYC' AND age > 25
            LIMIT 10
        """)
            .to_list()
        )

        assert len(results) == 2
        assert all(row for row in results)  # All non-empty

    def test_real_world_example(self, sample_csv):
        """Test realistic use case"""
        # Find people over 30 in LA
        results = (
            query(str(sample_csv))
            .sql("""
            SELECT name, age
            FROM data
            WHERE city = 'LA' AND age > 30
        """)
            .to_list()
        )

        assert len(results) == 1
        assert results[0]["name"] == "Eve"
        assert results[0]["age"] == 32


class TestLazyExecution:
    """Test that execution is lazy"""

    def test_lazy_iteration(self, sample_csv):
        """Test that results are lazy (not materialized)"""
        result = query(str(sample_csv)).sql("SELECT * FROM data")

        # Should be an iterator
        assert hasattr(result, "__iter__")

        # Can iterate one at a time
        iterator = iter(result)
        first = next(iterator)
        assert first["name"] == "Alice"

        second = next(iterator)
        assert second["name"] == "Bob"

    def test_multiple_iterations(self, sample_csv):
        """Test that result can be iterated multiple times"""
        result = query(str(sample_csv)).sql("SELECT * FROM data LIMIT 3")

        # First iteration
        rows1 = list(result)
        assert len(rows1) == 3

        # Second iteration should work
        rows2 = list(result)
        assert len(rows2) == 3


class TestSchemaInference:
    """Test schema inference"""

    def test_get_schema(self, sample_csv):
        """Test .schema() method"""
        from sqlstream.core.types import DataType, Schema

        schema = query(str(sample_csv)).schema()

        assert isinstance(schema, Schema)
        assert "name" in schema
        assert "age" in schema
        assert schema["name"] == DataType.STRING
        assert schema["age"] == DataType.INTEGER


class TestExplain:
    """Test query plan explanation"""

    def test_explain(self, sample_csv):
        """Test .explain() method"""
        plan = (
            query(str(sample_csv))
            .sql(
                """
            SELECT name
            FROM data
            WHERE age > 25
            LIMIT 10
        """,
                backend="python",
            )
            .explain()
        )

        assert isinstance(plan, str)
        assert "Limit" in plan
        assert "Project" in plan
        assert "Filter" in plan
        assert "Scan" in plan


class TestFormatDetection:
    """Test automatic format detection"""

    def test_csv_detection(self, sample_csv):
        """Test CSV format detection by extension"""
        q = query(str(sample_csv))

        assert q.reader is not None
        # Should create CSV reader
        assert q.reader.__class__.__name__ == "CSVReader"

    def test_unsupported_format(self, tmp_path):
        """Test that unknown formats default to CSV"""
        unknown_file = tmp_path / "data.unknown"
        unknown_file.write_text("name,age\nAlice,30")

        # Should default to CSV reader
        q = query(str(unknown_file))
        assert q.reader.__class__.__name__ == "CSVReader"

        # Should be able to query it
        results = q.sql("SELECT * FROM data").to_list()
        assert len(results) == 1


class TestDocumentedExamples:
    """Test all examples from documentation"""

    def test_readme_example(self, sample_csv):
        """Test example from README"""
        # This is the example shown in README.md
        results = query(str(sample_csv)).sql("""
            SELECT name, age
            FROM data
            WHERE age > 25
            LIMIT 10
        """)

        for row in results:
            assert "name" in row
            assert "age" in row
            assert row["age"] > 25

    def test_api_doc_example(self, sample_csv):
        """Test example from API docs"""
        from sqlstream import query

        results = query(str(sample_csv)).sql("SELECT * FROM data WHERE age > 25")
        rows = list(results)

        assert all(row["age"] > 25 for row in rows)
