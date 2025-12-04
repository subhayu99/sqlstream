"""
DuckDB Query Executor - full SQL support with maximum performance

Passes SQL queries directly to DuckDB for complete SQL feature support.
This backend bypasses SQLStream's parser and uses DuckDB's native SQL engine.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterator, Optional

from sqlstream.readers.base import BaseReader

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    duckdb = None


class DuckDBExecutor:
    """
    DuckDB-based executor for full SQL support

    Features:
    - Complete SQL support (CTEs, window functions, subqueries, etc.)
    - Maximum performance (10-1000x faster than Python backend)
    - Native Parquet/CSV reading
    - Zero-copy data access where possible

    This executor:
    1. Creates an in-memory DuckDB connection
    2. Registers data files as tables
    3. Executes raw SQL query in DuckDB
    4. Returns results in SQLStream format

    Example:
        >>> executor = DuckDBExecutor()
        >>> results = executor.execute_raw(
        ...     "SELECT * FROM data WHERE age > 25",
        ...     {"data": "employees.csv"}
        ... )
    """

    def __init__(self):
        """Initialize DuckDB executor"""
        if not DUCKDB_AVAILABLE:
            raise ImportError(
                "DuckDB backend requires duckdb library. "
                "Install with: pip install duckdb"
            )

        # Create in-memory DuckDB connection
        self.conn = duckdb.connect(":memory:")

    def execute_raw(
        self,
        sql: str,
        sources: Dict[str, str],
        read_only: bool = True,
        use_dataframes: bool = True,
        reader_factory: Optional[Callable[[str], Any]] = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Execute raw SQL query with DuckDB

        Args:
            sql: Raw SQL query string
            sources: Dict mapping table names to file paths
            read_only: If True, uses read_csv/read_parquet for safety
            use_dataframes: If True, loads files as pandas DataFrames first
            reader_factory: Callable that takes a file path and returns a Reader object
        """
        try:
            # Step 1: Register all data sources
            if use_dataframes and reader_factory:
                # Use existing Reader infrastructure to get DataFrames
                self._register_sources_with_readers(sources, reader_factory)
            elif use_dataframes:
                # Fallback to internal logic if no factory provided (legacy/testing)
                self._register_sources_as_dataframes(sources)
            else:
                # Let DuckDB read directly from files
                for table_name, file_path in sources.items():
                    self._register_source(table_name, file_path, read_only)

            # Step 2: Replace file paths in SQL with table names
            transformed_sql = self._replace_sources_in_sql(sql, sources)

            # Step 3: Execute transformed query
            result = self.conn.execute(transformed_sql)

            # Step 4: Fetch column names
            columns = [desc[0] for desc in result.description]

            # Step 5: Yield results as dictionaries
            for row in result.fetchall():
                yield dict(zip(columns, row))

        except Exception as e:
            raise RuntimeError(f"DuckDB execution error: {e}") from e

    def _replace_sources_in_sql(self, sql: str, sources: Dict[str, str]) -> str:
        """
        Replace file paths in SQL with registered table names

        Args:
            sql: Original SQL query with file paths
            sources: Dict mapping table names to file paths

        Returns:
            Transformed SQL with quoted table names instead of file paths

        Example:
            Input SQL: "SELECT * FROM 'https://example.com/data.csv#html:0'"
            Input sources: {"data": "https://example.com/data.csv#html:0"}
            Output SQL: "SELECT * FROM \"data\""

        Note:
            Table names are quoted with double quotes to avoid conflicts with
            SQL keywords (e.g., 'right', 'left', 'order', etc.)
        """
        import re
        transformed_sql = sql

        # Sort by length (longest first) to avoid partial replacements
        for table_name, file_path in sorted(sources.items(), key=lambda x: len(x[1]), reverse=True):
            # Quote table name to avoid conflicts with SQL keywords
            # DuckDB uses double quotes for identifiers
            quoted_table = f'"{table_name}"'

            # Replace both quoted and unquoted versions of the file path
            # Try single quotes first
            if f"'{file_path}'" in transformed_sql:
                transformed_sql = transformed_sql.replace(f"'{file_path}'", quoted_table)
            # Try double quotes
            if f'"{file_path}"' in transformed_sql:
                transformed_sql = transformed_sql.replace(f'"{file_path}"', quoted_table)

            # Try unquoted - use lookahead/lookbehind to ensure we don't match partial paths
            # This handles cases like: FROM /path/to/file.csv or JOIN file.csv
            if file_path in transformed_sql:
                # Match file_path when preceded/followed by whitespace or keywords
                # Use negative lookbehind/lookahead to avoid matching inside other paths
                escaped_path = re.escape(file_path)
                # Pattern: match file path when NOT surrounded by alphanumeric/underscore/dot/slash
                # This ensures we match complete paths
                pattern = '(?<![\\w/.])' + escaped_path + '(?![\\w/.])'
                transformed_sql = re.sub(pattern, quoted_table, transformed_sql)

        return transformed_sql

    def _register_sources_with_readers(self, sources: Dict[str, str], reader_factory: Callable[[str], BaseReader]):
        """
        Register sources using Reader objects to get DataFrames
        """
        for table_name, file_path in sources.items():
            try:
                # Create reader using the factory (handles format detection, S3, etc.)
                reader = reader_factory(file_path)

                # Convert to DataFrame (efficiently)
                df = reader.to_dataframe()

                # Register in DuckDB
                self.conn.register(table_name, df)

            except Exception as e:
                # Fallback to file-based if reader fails
                print(f"Warning: Could not load {file_path} via Reader, using file-based: {e}")
                self._register_source(table_name, file_path)

    def _register_sources_as_dataframes(self, sources: Dict[str, str]):
        """
        Legacy method: Load files as pandas DataFrames manually
        (Kept for backward compatibility or when no reader_factory provided)
        """
        try:
            import pandas as pd
        except ImportError:
            # Fallback to file-based if pandas not available
            for table_name, file_path in sources.items():
                self._register_source(table_name, file_path)
            return

        for table_name, file_path in sources.items():
            try:
                file_path = file_path.strip("'\"")

                # Load file as DataFrame
                if file_path.endswith(('.parquet', '.pq')):
                    df = pd.read_parquet(file_path)
                elif file_path.endswith('.csv'):
                    df = pd.read_csv(file_path)
                elif file_path.endswith('.json'):
                    df = pd.read_json(file_path)
                elif file_path.startswith(('s3://', 'http://', 'https://')):
                    # For remote files, use pandas readers with appropriate storage options
                    if file_path.endswith(('.parquet', '.pq')):
                        df = pd.read_parquet(file_path)
                    else:
                        df = pd.read_csv(file_path)
                else:
                    # Try CSV as default
                    df = pd.read_csv(file_path)

                # Register DataFrame in DuckDB
                # DuckDB can query pandas DataFrames directly!
                self.conn.register(table_name, df)

            except Exception:
                self._register_source(table_name, file_path)


    def _register_source(self, table_name: str, file_path: str, read_only: bool = True):
        """
        Register a data source as a DuckDB table

        Args:
            table_name: Name to use for the table in SQL
            file_path: Path to data file (CSV, Parquet, JSON, etc.)
            read_only: If True, uses read_csv/read_parquet

        Supports:
            - CSV files (.csv)
            - Parquet files (.parquet, .pq)
            - JSON files (.json)
            - S3 files (s3://...)
            - HTTP URLs (https://...)
        """
        file_path = file_path.strip("'\"")  # Remove quotes if present

        # Determine file type
        if file_path.endswith('.parquet') or file_path.endswith('.pq'):
            # Use DuckDB's native Parquet reader
            self.conn.execute(
                f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{file_path}')"
            )
        elif file_path.endswith('.csv'):
            # Use DuckDB's CSV reader with auto-detection
            self.conn.execute(
                f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_csv('{file_path}', "
                f"auto_detect=true, header=true)"
            )
        elif file_path.endswith('.json'):
            # Use DuckDB's JSON reader
            self.conn.execute(
                f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_json('{file_path}')"
            )
        elif file_path.startswith('s3://'):
            # S3 support (requires httpfs extension)
            self._ensure_httpfs()
            if file_path.endswith('.parquet') or file_path.endswith('.pq'):
                self.conn.execute(
                    f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{file_path}')"
                )
            else:
                self.conn.execute(
                    f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_csv('{file_path}', auto_detect=true)"
                )
        elif file_path.startswith(('http://', 'https://')):
            # HTTP support (requires httpfs extension)
            self._ensure_httpfs()
            if file_path.endswith('.parquet') or file_path.endswith('.pq'):
                self.conn.execute(
                    f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{file_path}')"
                )
            elif file_path.endswith('.csv'):
                self.conn.execute(
                    f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_csv('{file_path}', auto_detect=true)"
                )
            else:
                # Try to auto-detect
                self.conn.execute(
                    f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM '{file_path}'"
                )
        else:
            # Generic - let DuckDB auto-detect
            self.conn.execute(
                f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM '{file_path}'"
            )

    def _ensure_httpfs(self):
        """Ensure httpfs extension is loaded for S3/HTTP support"""
        try:
            self.conn.execute("INSTALL httpfs")
            self.conn.execute("LOAD httpfs")
        except Exception:
            # httpfs might already be loaded or not needed
            pass

    def explain(self, sql: str, sources: Dict[str, str]) -> str:
        """
        Get DuckDB query execution plan

        Args:
            sql: SQL query
            sources: Table name to file path mapping

        Returns:
            DuckDB EXPLAIN output
        """
        # Register sources
        for table_name, file_path in sources.items():
            self._register_source(table_name, file_path)

        # Replace file paths in SQL with table names
        transformed_sql = self._replace_sources_in_sql(sql, sources)

        # Get explain plan
        result = self.conn.execute(f"EXPLAIN {transformed_sql}")
        return "\n".join([str(row[0]) for row in result.fetchall()])

    def close(self):
        """Close DuckDB connection"""
        if self.conn:
            self.conn.close()

    def __del__(self):
        """Cleanup on deletion"""
        self.close()

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


def is_duckdb_available() -> bool:
    """Check if DuckDB is available"""
    return DUCKDB_AVAILABLE
