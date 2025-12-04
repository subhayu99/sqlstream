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

import os
from pathlib import Path
import re
from typing import Any, Callable, Dict, Iterator, List, Literal, Optional, Tuple

from sqlstream.core.executor import Executor
from sqlstream.core.fragment_parser import parse_source_fragment
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

# Try to import duckdb executor
try:
    from sqlstream.core.duckdb_executor import DuckDBExecutor, is_duckdb_available
    DUCKDB_AVAILABLE = is_duckdb_available()
except ImportError:
    DUCKDB_AVAILABLE = False
    DuckDBExecutor = None


def _can_parse_with_custom_parser(sql: str) -> bool:
    """
    Determine if SQL can be handled by the custom parser (Python/Pandas backends)

    Returns True if SQL contains only basic features:
    - SELECT, FROM, WHERE, JOIN, GROUP BY, ORDER BY, LIMIT
    - Simple aggregates (COUNT, SUM, AVG, MIN, MAX)

    Returns False if SQL requires DuckDB:
    - CTEs (WITH clause)
    - Window functions (OVER, PARTITION BY)
    - Complex expressions (CASE, CAST, EXTRACT)
    - Subqueries
    - Set operations (UNION, INTERSECT, EXCEPT)
    - HAVING clause

    Args:
        sql: SQL query string to analyze

    Returns:
        True if custom parser can handle it, False if DuckDB is needed
    """
    sql_upper = sql.upper()

    # Check for advanced SQL features that require DuckDB
    advanced_keywords = [
        'WITH',  # CTEs
        'OVER',  # Window functions
        'PARTITION BY',  # Window functions
        'WINDOW',  # Window functions
        'HAVING',  # HAVING clause
        'UNION',  # Set operations
        'INTERSECT',  # Set operations
        'EXCEPT',  # Set operations
        'CASE',  # CASE expressions
        'CAST',  # Type casting
        'EXTRACT',  # Date extraction
        'ROW_NUMBER',  # Window functions
        'RANK',  # Window functions
        'DENSE_RANK',  # Window functions
        'LAG',  # Window functions
        'LEAD',  # Window functions
    ]

    for keyword in advanced_keywords:
        if keyword in sql_upper:
            return False  # Needs DuckDB

    # Check for subqueries - look for SELECT inside parentheses
    # This is a simple heuristic
    if '(' in sql:
        # Extract content inside parentheses
        import re
        paren_content = re.findall(r'\(([^)]+)\)', sql_upper)
        for content in paren_content:
            if 'SELECT' in content:
                return False  # Has subquery, needs DuckDB

    return True  # Can use custom parser


class Query:
    """
    Main query builder class

    Provides a fluent API for building and executing queries.
    """

    def __init__(self, source: Optional[str] = None):
        """
        Initialize query with an optional data source

        Args:
            source: Optional path to data file or URL. If not provided,
                   sources will be extracted from the SQL query itself.

        Example:
            >>> # With explicit source
            >>> query = Query("data.csv")
            >>> query = Query("/path/to/data.parquet")
            >>> query = Query("https://example.com/data.csv")
            >>>
            >>> # Without source - extracted from SQL
            >>> query = Query()
            >>> query.sql("SELECT * FROM 'data.csv' WHERE age > 25")
        """
        self.source = source
        self.reader = self._create_reader(source) if source else None

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

        elif format_hint == 'xml' or (not format_hint and suffix == '.xml'):
            from sqlstream.readers.xml_reader import XMLReader
            # For XML, table_hint is used as element name/path
            element = str(table_hint) if table_hint is not None else None
            return XMLReader(source_path, element=element)

        elif format_hint == 'parquet' or (not format_hint and suffix == ".parquet"):
            from sqlstream.readers.parquet_reader import ParquetReader
            return ParquetReader(source_path)

        elif format_hint == 'json' or (not format_hint and suffix == ".json"):
            from sqlstream.readers.json_reader import JSONReader
            # Ensure key is a string for JSON lookups
            key = str(table_hint) if table_hint is not None else None
            return JSONReader(source_path, records_key=key)

        elif format_hint == 'jsonl' or (not format_hint and suffix == ".jsonl"):
            from sqlstream.readers.jsonl_reader import JSONLReader
            return JSONLReader(source_path)

        elif format_hint == 'csv' or (not format_hint and suffix == ".csv"):
            return CSVReader(source_path)

        else:
            # Try CSV as default
            try:
                return CSVReader(source_path)
            except Exception as e:
                raise ValueError(
                    f"Unsupported file format: {suffix}. "
                    f"Supported formats: .csv, .parquet, .json, .jsonl, .html, .md, .xml"
                ) from e

    def sql(
        self, query: str, backend: Optional[Literal["auto", "pandas", "python", "duckdb"]] = "auto"
    ) -> "QueryResult":
        """
        Execute SQL query on the data source

        Args:
            query: SQL query string
            backend: Execution backend to use
                - "auto": Smart selection - uses custom parser for simple queries,
                         DuckDB for complex queries, pandas if available, else python
                - "duckdb": Force DuckDB backend (full SQL support)
                - "pandas": Force pandas backend (10-100x faster than python)
                - "python": Force pure Python Volcano model (educational)

        Returns:
            QueryResult object that can be iterated over

        Example:
            >>> # With explicit source
            >>> result = query("data.csv").sql("SELECT * WHERE age > 25")
            >>> for row in result:
            ...     print(row)
            >>>
            >>> # Without source - from SQL
            >>> result = query().sql("SELECT * FROM 'data.csv' WHERE age > 25")
            >>>
            >>> # Force DuckDB backend for full SQL support
            >>> result = query("data.csv").sql(
            ...     "SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rank "
            ...     "FROM data",
            ...     backend="duckdb"
            ... )
        """
        # Note: Backend selection is now handled in QueryResult._select_backend()
        # to preserve the 'auto' value while still doing smart selection

        # If query can be parsed with custom parser
        if _can_parse_with_custom_parser(query):
            # Try to parse with custom parser
            try:
                ast = parse(query)
            except Exception:
                # Cannot be parsed with custom parser, need DuckDB
                ast = None
        else:
            # Cannot be parsed with custom parser, need DuckDB
            ast = None

        # Create QueryResult with reader factory for JOIN support
        return QueryResult(ast=ast, reader=self.reader, reader_factory=self._create_reader,
                          source=self.source, backend=backend, raw_sql=query)

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
        if not self.reader:
            raise ValueError("Cannot get schema without a source. Provide a source when creating the Query object.")
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
        raw_sql: str = None,
    ):
        """
        Initialize query result

        Args:
            ast: Parsed SQL AST (None for DuckDB backend)
            reader: Data source reader
            reader_factory: Factory function to create readers for JOIN tables
            source: Path to data source file
            backend: Execution backend ("auto", "pandas", "python", or "duckdb")
            raw_sql: Original SQL query string (required for DuckDB)
        """
        self.ast = ast
        self.reader = reader
        self.reader_factory = reader_factory
        self.source = source
        self.backend = backend
        self.raw_sql = raw_sql

        # Select executor based on backend
        self._select_backend()

    def _select_backend(self):
        """Select appropriate backend based on configuration"""

        # Determine effective backend to use
        target_backend = self.backend

        if target_backend == "auto":
            # Smart backend selection logic

            # Case 1: AST already present
            # This means custom parser already succeeded
            if self.ast:
                if PANDAS_AVAILABLE:
                    target_backend = "pandas"
                else:
                    target_backend = "python"

            # Case 2: No AST, analyze raw SQL
            elif self.raw_sql:
                if _can_parse_with_custom_parser(self.raw_sql):
                    # Simple query - prefer pandas > python
                    if PANDAS_AVAILABLE:
                        target_backend = "pandas"
                    else:
                        target_backend = "python"
                elif DUCKDB_AVAILABLE:
                    # Complex query - use DuckDB
                    target_backend = "duckdb"
                else:
                    # Complex query but no DuckDB
                    raise ImportError(
                        "This query requires advanced SQL features not supported by the basic parser. "
                        "Install `sqlstream[duckdb]`"
                    )

            # Case 3: Fallback (shouldn't happen in normal usage)
            else:
                if PANDAS_AVAILABLE:
                    target_backend = "pandas"
                elif DUCKDB_AVAILABLE:
                    target_backend = "duckdb"
                else:
                    target_backend = "python"

        # Configure executor based on target_backend
        if target_backend == "duckdb":
            # Force DuckDB backend
            if not DUCKDB_AVAILABLE:
                raise ImportError(
                    "DuckDB backend requested but duckdb is not installed. "
                    "Install `sqlstream[duckdb]`"
                )
            self.executor = DuckDBExecutor()
            self.use_duckdb = True
            self.use_pandas = False

        elif target_backend == "pandas":
            # Force pandas backend
            if not PANDAS_AVAILABLE:
                raise ImportError(
                    "Pandas backend requested but pandas is not installed. "
                    "Install `sqlstream[pandas]`"
                )

            # Ensure AST is parsed if not already present
            if not self.ast and self.raw_sql:
                try:
                    self.ast = parse(self.raw_sql)
                except Exception as e:
                    # If auto selected pandas but parsing failed, try fallback to DuckDB
                    if self.backend == "auto" and DUCKDB_AVAILABLE:
                        self.executor = DuckDBExecutor()
                        self.use_duckdb = True
                        self.use_pandas = False
                        return

                    raise ValueError(
                        f"Failed to parse SQL query: {e}. "
                        "Consider installing DuckDB for full SQL support: pip install sqlstream[duckdb]"
                    ) from e

            self.executor = PandasExecutor()
            self.use_pandas = True
            self.use_duckdb = False

        elif target_backend == "python":
            # Force pure Python backend

            # Ensure AST is parsed if not already present
            if not self.ast and self.raw_sql:
                try:
                    self.ast = parse(self.raw_sql)
                except Exception as e:
                    # If auto selected python but parsing failed, try fallback to DuckDB
                    if self.backend == "auto" and DUCKDB_AVAILABLE:
                        self.executor = DuckDBExecutor()
                        self.use_duckdb = True
                        self.use_pandas = False
                        return

                    raise ValueError(f"Failed to parse SQL query: {e}") from e

            self.executor = Executor()
            self.use_pandas = False
            self.use_duckdb = False

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """
        Execute query and yield results lazily

        Yields:
            Result rows as dictionaries
        """
        if self.use_duckdb:
            # DuckDB executor - pass raw SQL and discover tables
            if not self.raw_sql:
                raise ValueError("DuckDB backend requires raw SQL query")

            sources = self._discover_sources()

            # Use reader factory to create DataFrames efficiently
            yield from self.executor.execute_raw(
                self.raw_sql,
                sources,
                reader_factory=self.reader_factory
            )
        elif self.use_pandas:
            # Pandas executor takes file path directly
            right_source = self.ast.join.right_source if self.ast.join else None
            yield from self.executor.execute(self.ast, self.source or self.ast.source, right_source)
        else:
            # Python executor uses reader objects
            # If no reader exists (sourceless query), create one from AST
            reader = self.reader
            if not reader and self.ast and self.ast.source:
                reader = self.reader_factory(self.ast.source)
            yield from self.executor.execute(self.ast, reader, self.reader_factory)

    @staticmethod
    def _get_sanitized_name_and_table_hint(source: str) -> Tuple[str, Optional[int]]:
        # Parse fragment if present to get base path
        clean_path, format_hint, table_hint = parse_source_fragment(source)

        # Generate a table name from the file path
        # Extract base filename (without extension or parent directories)
        base_name = os.path.splitext(os.path.basename(clean_path))[0]

        # Clean up the name to be SQL-safe (only alphanumeric and underscore)
        sanitized_name = re.sub(r'[^a-zA-Z0-9_]', '_', base_name)
        return sanitized_name, table_hint

    @staticmethod
    def _get_table_name(source: str) -> str:
        sanitized_name, table_hint = QueryResult._get_sanitized_name_and_table_hint(source)

        # Make the table name unique if multiple tables from same file
        # This handles cases like complex.html#html:0, complex.html#html:1, etc.
        if table_hint is not None:
            # Include table/fragment index in the name
            table_name = f"{sanitized_name}_{table_hint}"
        else:
            table_name = sanitized_name
        return table_name

    def _discover_sources(self) -> Dict[str, str]:
        """
        Discover all table sources from raw SQL or AST

        For DuckDB backend, this extracts all file paths from the SQL query.
        Handles multiple files in JOINs, subqueries, CTEs, etc.
        Properly handles URL fragments like #html:0
        """
        sources: Dict[str, str] = {}

        if self.raw_sql:
            # Extract file paths from raw SQL for DuckDB
            # Pattern matches quoted file paths/URLs (including those with fragments)
            # Matches: FROM 'file.csv' or FROM "file.csv" or JOIN 'url#format:table'
            # The key is to NOT stop at # - we need to capture the full fragment

            # Pattern for quoted file paths - captures everything inside quotes
            # This handles fragments like #html:3, #csv:0, etc.
            quoted_pattern = r"(?:FROM|JOIN)\s+(['\"])([^\1]+?)\1"
            matches = re.findall(quoted_pattern, self.raw_sql, re.IGNORECASE)

            # Track table name usage to avoid conflicts
            name_counter = {}

            for _quote_char, file_path in matches:
                sanitized_name, _ = self._get_sanitized_name_and_table_hint(file_path)
                table_name = self._get_table_name(file_path)

                # Ensure uniqueness by adding counter if needed
                if table_name in sources:
                    counter = name_counter.get(sanitized_name, 0) + 1
                    name_counter[sanitized_name] = counter
                    table_name = f"{table_name}_{counter}"

                # Store the mapping: table_name -> original_file_path
                sources[table_name] = file_path

            # Also check for unquoted file paths (e.g., from f-strings in tests)
            # Pattern: FROM /path/to/file.ext or FROM file.ext
            # This is tricky because we need to stop at keywords or whitespace
            unquoted_pattern = r"(?:FROM|JOIN)\s+([/\w.#:-]+?)(?:\s+(?:ON|WHERE|GROUP|ORDER|LIMIT|INNER|LEFT|RIGHT|JOIN|,|\))|$)"
            unquoted_matches: List[str] = re.findall(unquoted_pattern, self.raw_sql, re.IGNORECASE)

            for file_path in unquoted_matches:
                # Skip if already found as quoted
                if file_path in set(sources.values()):
                    continue

                # Skip SQL keywords
                if file_path.upper() in ['INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS']:
                    continue

                # Only process if it looks like a file path
                if '/' in file_path or '.' in file_path or '#' in file_path:
                    sanitized_name, _ = self._get_sanitized_name_and_table_hint(file_path)
                    table_name = self._get_table_name(file_path)

                    # Ensure uniqueness
                    if table_name in sources:
                        counter = name_counter.get(sanitized_name, 0) + 1
                        name_counter[sanitized_name] = counter
                        table_name = f"{table_name}_{counter}"

                    sources[table_name] = file_path

            # If no sources found from SQL, use the main source
            if not sources and self.source:
                table_name = self._get_table_name(self.source)
                sources[table_name] = self.source

        elif self.ast:
            table_name = self._get_table_name(self.ast.source)

            # Extract from AST for Python/Pandas backends
            # Main table
            if hasattr(self.ast, 'table') and self.ast.table:
                sources[self.ast.table] = self.ast.source
            else:
                # If no explicit table name, use 'data' as default
                sources[table_name] = self.ast.source

            # JOIN table
            if self.ast.join:
                join_table = self.ast.join.right_source
                join_table_name = self._get_table_name(join_table)
                sources[join_table_name] = join_table

        return sources

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
        if self.use_duckdb:
            # DuckDB executor explain
            if not self.raw_sql:
                raise ValueError("DuckDB backend requires raw SQL query")
            sources = self._discover_sources()
            return self.executor.explain(self.raw_sql, sources)
        elif self.use_pandas:
            # Pandas executor explain
            return self.executor.explain(self.ast, self.source)
        else:
            # Python executor explain
            return self.executor.explain(self.ast, self.reader, self.reader_factory)


# Convenience function for top-level API
def query(source: Optional[str] = None) -> Query:
    """
    Create a query for a data source

    This is the main entry point for the SQLStream API.

    Args:
        source: Optional path to data file or URL. If not provided,
                sources will be extracted from the SQL query.

    Returns:
        Query object

    Example:
        >>> from sqlstream import query
        >>>
        >>> # With explicit source
        >>> results = query("data.csv").sql("SELECT * WHERE age > 25")
        >>> for row in results:
        ...     print(row)
        >>>
        >>> # Without source - extracted from SQL
        >>> results = query().sql("SELECT * FROM 'data.csv' WHERE age > 25")
        >>> for row in results:
        ...     print(row)
    """
    return Query(source)
