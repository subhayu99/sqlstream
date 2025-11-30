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
from typing import Any, Callable, Dict, Iterator, List, Literal, Optional

from sqlstream.core.executor import Executor
from sqlstream.readers.base import BaseReader
from sqlstream.readers.csv_reader import CSVReader
from sqlstream.sql.parser import parse
from sqlstream.core.types import Schema

# Try to import pandas executor
try:
    from sqlstream.core.pandas_executor import PANDAS_AVAILABLE, PandasExecutor
except ImportError:
    PANDAS_AVAILABLE = False
    PandasExecutor = None


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
        
        Supports URL fragments: source#format:table

        Args:
            source: Path to data file or URL, optionally with #format:table fragment

        Returns:
            Reader instance for the source

        Raises:
            ValueError: If file format is not supported
        """
        from sqlstream.core.fragment_parser import parse_source_fragment
        
        # Parse URL fragment if present
        source_path, format_hint, table_hint = parse_source_fragment(source)
        
        # Check if source is HTTP/HTTPS URL
        if source_path.startswith(("http://", "https://")):
            from sqlstream.readers.http_reader import HTTPReader

            kwargs = {}
            if format_hint:
                kwargs['format'] = format_hint
            if table_hint is not None:
                kwargs['table'] = table_hint
            return HTTPReader(source_path, **kwargs)

        path = Path(source_path)

        # Check file extension to determine format
        suffix = path.suffix.lower()
        
        # Explicit format from fragment takes precedence
        if format_hint == 'html' or (not format_hint and suffix in ['.html', '.htm']):
            from sqlstream.readers.html_reader import HTMLReader
            table = table_hint if table_hint is not None else 0
            return HTMLReader(source_path, table=table)
        
        elif format_hint == 'markdown' or (not format_hint and suffix in ['.md', '.markdown']):
            from sqlstream.readers.markdown_reader import MarkdownReader
            table = table_hint if table_hint is not None else 0
            return MarkdownReader(source_path, table=table)

        elif format_hint == 'parquet' or (not format_hint and suffix == ".parquet"):
            from sqlstream.readers.parquet_reader import ParquetReader
            return ParquetReader(source_path)
        
        elif format_hint == 'csv' or (not format_hint and suffix == ".csv"):
            return CSVReader(source_path)
        
        else:
            # Try CSV as default
            try:
                return CSVReader(source_path)
            except Exception as e:
                raise ValueError(
                    f"Unsupported file format: {suffix}. "
                    f"Supported formats: .csv, .parquet, .html, .md"
                ) from e

    def sql(
        self, query: str, backend: Optional[Literal["auto", "pandas", "python"]] = "auto"
    ) -> "QueryResult":
        """
        Execute SQL query on the data source

        Args:
            query: SQL query string
            backend: Execution backend to use
                - "auto": Use pandas if available, fallback to python
                - "pandas": Force pandas backend (raises if not installed)
                - "python": Force pure Python Volcano model

        Returns:
            QueryResult object that can be iterated over

        Example:
            >>> result = query("data.csv").sql("SELECT * WHERE age > 25")
            >>> for row in result:
            ...     print(row)
            >>>
            >>> # Force pandas backend for performance
            >>> result = query("data.csv").sql("SELECT * WHERE age > 25", backend="pandas")
        """
        # Parse SQL query
        ast = parse(query)

        # Create QueryResult with reader factory for JOIN support
        return QueryResult(ast, self.reader, self._create_reader, self.source, backend)

    def schema(self) -> Optional[Schema]:
        """
        Get schema information for the data source

        Returns:
            Schema object with inferred types, or None if schema cannot be inferred

        Example:
            >>> schema = query("data.csv").schema()
            >>> print(schema)
            Schema(name: STRING, age: INTEGER, salary: FLOAT)
        """
        return self.reader.get_schema()


class QueryResult:
    """
    Query result - lazy iterator over query results

    This class wraps the execution of a query and provides
    a lazy iterator over the results.
    """

    def __init__(
        self,
        ast,
        reader: BaseReader,
        reader_factory: Callable[[str], BaseReader],
        source: str,
        backend: str = "auto",
    ):
        """
        Initialize query result

        Args:
            ast: Parsed SQL AST
            reader: Data source reader
            reader_factory: Factory function to create readers for JOIN tables
            source: Path to data source file
            backend: Execution backend ("auto", "pandas", or "python")
        """
        self.ast = ast
        self.reader = reader
        self.reader_factory = reader_factory
        self.source = source
        self.backend = backend

        # Select executor based on backend
        self._select_backend()

    def _select_backend(self):
        """Select appropriate backend based on configuration"""
        if self.backend == "pandas":
            # Force pandas backend
            if not PANDAS_AVAILABLE:
                raise ImportError(
                    "Pandas backend requested but pandas is not installed. "
                    "Install `sqlstream[pandas]`"
                )
            self.executor = PandasExecutor()
            self.use_pandas = True
        elif self.backend == "python":
            # Force pure Python backend
            self.executor = Executor()
            self.use_pandas = False
        else:  # auto
            # Use pandas if available, fallback to Python
            if PANDAS_AVAILABLE:
                self.executor = PandasExecutor()
                self.use_pandas = True
            else:
                self.executor = Executor()
                self.use_pandas = False

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """
        Execute query and yield results lazily

        Yields:
            Result rows as dictionaries
        """
        if self.use_pandas:
            # Pandas executor takes file path directly
            right_source = self.ast.join.right_source if self.ast.join else None
            yield from self.executor.execute(self.ast, self.source, right_source)
        else:
            # Python executor uses reader objects
            yield from self.executor.execute(self.ast, self.reader, self.reader_factory)

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
        if self.use_pandas:
            # Pandas executor explain
            return self.executor.explain(self.ast, self.source)
        else:
            # Python executor explain
            return self.executor.explain(self.ast, self.reader, self.reader_factory)


class QueryInline:
    """
    Query builder for inline file path mode

    This class allows SQL queries with file paths embedded directly in the SQL,
    instead of pre-specifying a source file.

    Example:
        >>> q = QueryInline()
        >>> results = q.sql("SELECT * FROM 'data.csv' WHERE age > 25")
        >>> for row in results:
        ...     print(row)
    """

    def __init__(self):
        """Initialize inline query (no source required)"""
        pass

    def _create_reader(self, source: str) -> BaseReader:
        """
        Auto-detect source type and create appropriate reader
        
        Supports URL fragments: source#format:table

        Args:
            source: Path to data file or URL, optionally with #format:table fragment

        Returns:
            Reader instance for the source

        Raises:
            ValueError: If file format is not supported
        """
        from sqlstream.core.fragment_parser import parse_source_fragment
        
        # Parse URL fragment if present
        source_path, format_hint, table_hint = parse_source_fragment(source)
        
        # Check if source is HTTP/HTTPS URL
        if source_path.startswith(("http://", "https://")):
            from sqlstream.readers.http_reader import HTTPReader

            kwargs = {}
            if format_hint:
                kwargs['format'] = format_hint
            if table_hint is not None:
                kwargs['table'] = table_hint
            return HTTPReader(source_path, **kwargs)

        path = Path(source_path)

        # Check file extension to determine format
        suffix = path.suffix.lower()
        
        # Explicit format from fragment takes precedence
        if format_hint == 'html' or (not format_hint and suffix in ['.html', '.htm']):
            from sqlstream.readers.html_reader import HTMLReader
            table = table_hint if table_hint is not None else 0
            return HTMLReader(source_path, table=table)
        
        elif format_hint == 'markdown' or (not format_hint and suffix in ['.md', '.markdown']):
            from sqlstream.readers.markdown_reader import MarkdownReader
            table = table_hint if table_hint is not None else 0
            return MarkdownReader(source_path, table=table)

        elif format_hint == 'parquet' or (not format_hint and suffix == ".parquet"):
            from sqlstream.readers.parquet_reader import ParquetReader
            return ParquetReader(source_path)
        
        elif format_hint == 'csv' or (not format_hint and suffix == ".csv"):
            return CSVReader(source_path)
        
        else:
            # Try CSV as default
            try:
                return CSVReader(source_path)
            except Exception as e:
                raise ValueError(
                    f"Unsupported file format: {suffix}. "
                    f"Supported formats: .csv, .parquet, .html, .md"
                ) from e

    def sql(
        self, query: str, backend: Optional[Literal["auto", "pandas", "python"]] = "auto"
    ) -> "QueryResult":
        """
        Execute SQL query with inline file paths

        The file paths are extracted from the SQL query itself (FROM and JOIN clauses).

        Args:
            query: SQL query string with inline file paths
            backend: Execution backend to use

        Returns:
            QueryResult object that can be iterated over

        Example:
            >>> q = QueryInline()
            >>> result = q.sql("SELECT * FROM 'data.csv' WHERE age > 25")
            >>> # Multi-file JOIN
            >>> result = q.sql("SELECT x.*, y.name FROM 'left.csv' x JOIN 'right.csv' y ON x.id = y.id")
        """
        # Parse SQL query to extract source file paths
        ast = parse(query)

        # Create reader for the main source
        reader = self._create_reader(ast.source)

        # Create QueryResult with inline mode (source extracted from AST)
        return QueryResult(ast, reader, self._create_reader, ast.source, backend)


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
