"""
Main Query API - user-facing interface for SQLStream

This is the primary entry point for users. It provides a simple,
fluent API for querying data sources.

Example:
    >>> from sqlstream import query
    >>> results = query("data.csv").sql("SELECT * WHERE age > 25 LIMIT 10")
    >>> for row in results:
    ...     print(row)
"""

from pathlib import Path
from typing import Any, Dict, Iterator, List

from sqlstream.core.executor import Executor
from sqlstream.readers.base import BaseReader
from sqlstream.readers.csv_reader import CSVReader
from sqlstream.sql.parser import parse


class Query:
    """
    Main query builder class

    Provides a fluent API for building and executing queries.
    """

    def __init__(self, source: str):
        """
        Initialize query with a data source

        Args:
            source: Path to data file or URL

        Example:
            >>> query = Query("data.csv")
            >>> query = Query("/path/to/data.parquet")
            >>> query = Query("https://example.com/data.csv")
        """
        self.source = source
        self.reader = self._create_reader(source)

    def _create_reader(self, source: str) -> BaseReader:
        """
        Auto-detect source type and create appropriate reader

        Args:
            source: Path to data file or URL

        Returns:
            Reader instance for the source

        Raises:
            ValueError: If file format is not supported
        """
        path = Path(source)

        # Check file extension to determine format
        suffix = path.suffix.lower()

        if suffix == ".csv":
            return CSVReader(source)
        elif suffix == ".parquet":
            from sqlstream.readers.parquet_reader import ParquetReader

            return ParquetReader(source)
        # elif suffix in [".json", ".jsonl"]:
        #     return JSONReader(source)  # Future
        else:
            # Try CSV as default
            try:
                return CSVReader(source)
            except Exception as e:
                raise ValueError(
                    f"Unsupported file format: {suffix}. "
                    f"Supported formats: .csv, .parquet"
                ) from e

    def sql(self, query: str) -> "QueryResult":
        """
        Execute SQL query on the data source

        Args:
            query: SQL query string

        Returns:
            QueryResult object that can be iterated over

        Example:
            >>> result = query("data.csv").sql("SELECT * WHERE age > 25")
            >>> for row in result:
            ...     print(row)
        """
        # Parse SQL query
        ast = parse(query)

        # Create QueryResult
        return QueryResult(ast, self.reader)

    def schema(self) -> Dict[str, str]:
        """
        Get schema information for the data source

        Returns:
            Dictionary mapping column names to types

        Example:
            >>> schema = query("data.csv").schema()
            >>> print(schema)
            {'name': 'str', 'age': 'int', 'salary': 'float'}
        """
        return self.reader.get_schema()


class QueryResult:
    """
    Query result - lazy iterator over query results

    This class wraps the execution of a query and provides
    a lazy iterator over the results.
    """

    def __init__(self, ast, reader: BaseReader):
        """
        Initialize query result

        Args:
            ast: Parsed SQL AST
            reader: Data source reader
        """
        self.ast = ast
        self.reader = reader
        self.executor = Executor()

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """
        Execute query and yield results lazily

        Yields:
            Result rows as dictionaries
        """
        yield from self.executor.execute(self.ast, self.reader)

    def to_list(self) -> List[Dict[str, Any]]:
        """
        Materialize all results into a list

        Returns:
            List of all result rows

        Example:
            >>> results = query("data.csv").sql("SELECT *").to_list()
            >>> print(len(results))
            100
        """
        return list(self)

    def explain(self) -> str:
        """
        Get query execution plan

        Returns:
            Human-readable execution plan

        Example:
            >>> plan = query("data.csv").sql("SELECT * WHERE age > 25").explain()
            >>> print(plan)
            Limit(10)
              Project(name, age)
                Filter(age > 25)
                  Scan(CSVReader)
        """
        return self.executor.explain(self.ast, self.reader)


# Convenience function for top-level API
def query(source: str) -> Query:
    """
    Create a query for a data source

    This is the main entry point for the SQLStream API.

    Args:
        source: Path to data file or URL

    Returns:
        Query object

    Example:
        >>> from sqlstream import query
        >>> results = query("data.csv").sql("SELECT * WHERE age > 25")
        >>> for row in results:
        ...     print(row)
    """
    return Query(source)
