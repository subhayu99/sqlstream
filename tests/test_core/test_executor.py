"""
Tests for query executor
"""

import pytest

from sqlstream.core.executor import Executor
from sqlstream.readers.csv_reader import CSVReader
from sqlstream.sql.parser import parse


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


class TestBasicExecution:
    """Test basic query execution"""

    def test_select_all(self, sample_csv):
        """Test SELECT * FROM data"""
        ast = parse("SELECT * FROM data")
        reader = CSVReader(str(sample_csv))
        executor = Executor()

        results = list(executor.execute(ast, reader))

        assert len(results) == 5
        assert results[0]["name"] == "Alice"
        assert results[0]["age"] == 30

    def test_select_columns(self, sample_csv):
        """Test SELECT name, age FROM data"""
        ast = parse("SELECT name, age FROM data")
        reader = CSVReader(str(sample_csv))
        executor = Executor()

        results = list(executor.execute(ast, reader))

        assert len(results) == 5
        assert set(results[0].keys()) == {"name", "age"}
        assert "city" not in results[0]

    def test_where_clause(self, sample_csv):
        """Test SELECT * FROM data WHERE age > 30"""
        ast = parse("SELECT * FROM data WHERE age > 30")
        reader = CSVReader(str(sample_csv))
        executor = Executor()

        results = list(executor.execute(ast, reader))

        assert len(results) == 2
        assert all(row["age"] > 30 for row in results)

    def test_limit_clause(self, sample_csv):
        """Test SELECT * FROM data LIMIT 3"""
        ast = parse("SELECT * FROM data LIMIT 3")
        reader = CSVReader(str(sample_csv))
        executor = Executor()

        results = list(executor.execute(ast, reader))

        assert len(results) == 3


class TestComplexQueries:
    """Test complex queries with multiple clauses"""

    def test_select_where_limit(self, sample_csv):
        """Test SELECT name WHERE age > 25 LIMIT 2"""
        ast = parse("SELECT name FROM data WHERE age > 25 LIMIT 2")
        reader = CSVReader(str(sample_csv))
        executor = Executor()

        results = list(executor.execute(ast, reader))

        assert len(results) == 2
        assert set(results[0].keys()) == {"name"}
        # Results should be rows where age > 25
        # (Alice, Charlie, Diana, Eve in CSV order)

    def test_multiple_filters(self, sample_csv):
        """Test SELECT * WHERE age > 25 AND city = 'NYC'"""
        ast = parse("SELECT * FROM data WHERE age > 25 AND city = 'NYC'")
        reader = CSVReader(str(sample_csv))
        executor = Executor()

        results = list(executor.execute(ast, reader))

        # Alice (30, NYC) and Diana (28, NYC)
        assert len(results) == 2
        assert all(row["age"] > 25 and row["city"] == "NYC" for row in results)

    def test_full_query(self, sample_csv):
        """Test SELECT name, age WHERE city = 'LA' LIMIT 1"""
        ast = parse("SELECT name, age FROM data WHERE city = 'LA' LIMIT 1")
        reader = CSVReader(str(sample_csv))
        executor = Executor()

        results = list(executor.execute(ast, reader))

        assert len(results) == 1
        assert set(results[0].keys()) == {"name", "age"}
        assert results[0]["name"] == "Bob"  # First LA person


class TestEmptyResults:
    """Test queries that return no results"""

    def test_filter_no_matches(self, sample_csv):
        """Test WHERE clause that matches nothing"""
        ast = parse("SELECT * FROM data WHERE age > 100")
        reader = CSVReader(str(sample_csv))
        executor = Executor()

        results = list(executor.execute(ast, reader))

        assert len(results) == 0

    def test_limit_zero(self, sample_csv):
        """Test LIMIT 0"""
        ast = parse("SELECT * FROM data LIMIT 0")
        reader = CSVReader(str(sample_csv))
        executor = Executor()

        results = list(executor.execute(ast, reader))

        assert len(results) == 0


class TestLazyExecution:
    """Test that execution is lazy (generator-based)"""

    def test_lazy_iterator(self, sample_csv):
        """Test that execute returns a generator"""
        ast = parse("SELECT * FROM data")
        reader = CSVReader(str(sample_csv))
        executor = Executor()

        results = executor.execute(ast, reader)

        # Should be a generator
        assert hasattr(results, "__iter__")
        assert hasattr(results, "__next__")

        # Can pull one result at a time
        first = next(results)
        assert first["name"] == "Alice"

        second = next(results)
        assert second["name"] == "Bob"


class TestExplainPlan:
    """Test query plan explanation"""

    def test_explain_simple(self, sample_csv):
        """Test explain for simple query"""
        ast = parse("SELECT * FROM data")
        reader = CSVReader(str(sample_csv))
        executor = Executor()

        plan = executor.explain(ast, reader)

        # Should contain operator names in the plan
        assert "Project" in plan
        assert "Scan" in plan

    def test_explain_complex(self, sample_csv):
        """Test explain for complex query"""
        ast = parse("SELECT name FROM data WHERE age > 25 LIMIT 10")
        reader = CSVReader(str(sample_csv))
        executor = Executor()

        plan = executor.explain(ast, reader)

        # Should show full operator tree
        assert "Limit" in plan
        assert "Project" in plan
        assert "Filter" in plan
        assert "Scan" in plan

        # Operators should be in tree order (top to bottom)
        limit_pos = plan.index("Limit")
        project_pos = plan.index("Project")
        filter_pos = plan.index("Filter")
        scan_pos = plan.index("Scan")

        assert limit_pos < project_pos < filter_pos < scan_pos


class TestEndToEnd:
    """End-to-end integration tests"""

    def test_real_world_query(self, sample_csv):
        """Test realistic query"""
        # Find top 2 highest earners in NYC
        ast = parse("""
            SELECT name, salary
            FROM data
            WHERE city = 'NYC'
            LIMIT 2
        """)
        reader = CSVReader(str(sample_csv))
        executor = Executor()

        results = list(executor.execute(ast, reader))

        assert len(results) == 2
        assert all("name" in row and "salary" in row for row in results)
        assert all(row for row in results)  # All rows non-empty

    def test_parse_and_execute(self, sample_csv):
        """Test full pipeline: parse -> execute"""
        sql = "SELECT name, age FROM data WHERE age >= 30 AND city = 'NYC'"

        # Parse SQL
        ast = parse(sql)

        # Execute query
        reader = CSVReader(str(sample_csv))
        executor = Executor()
        results = list(executor.execute(ast, reader))

        # Verify results
        assert len(results) == 1
        assert results[0]["name"] == "Alice"
        assert results[0]["age"] == 30
