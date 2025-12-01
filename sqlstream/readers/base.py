"""
Base reader interface for all data sources

All readers implement this interface to provide a consistent API
for the query engine.
"""

from typing import Any, Dict, Iterator, List, Optional

from sqlstream.sql.ast_nodes import Condition
from sqlstream.core.types import Schema


class BaseReader:
    """
    Base class for all data source readers

    Readers are responsible for:
    1. Reading data from a source (file, URL, database, etc.)
    2. Yielding rows as dictionaries (lazy evaluation)
    3. Optionally supporting predicate pushdown
    4. Optionally supporting column pruning
    """

    def read_lazy(self) -> Iterator[Dict[str, Any]]:
        """
        Yield rows as dictionaries

        This is the core method that all readers must implement.
        It should yield one row at a time (lazy evaluation) rather than
        loading all data into memory.

        Yields:
            Dictionary representing one row of data

        Example:
            {'name': 'Alice', 'age': 30, 'city': 'NYC'}
        """
        raise NotImplementedError("Subclasses must implement read_lazy()")

    def supports_pushdown(self) -> bool:
        """
        Does this reader support predicate pushdown?

        If True, the query optimizer can call set_filter() to push
        WHERE conditions down to the reader for more efficient execution.

        Returns:
            True if predicate pushdown is supported
        """
        return False

    def set_filter(self, conditions: List[Condition]) -> None:
        """
        Set filter conditions for predicate pushdown

        Args:
            conditions: List of WHERE conditions to apply during read

        Note:
            Only called if supports_pushdown() returns True
        """
        pass

    def supports_column_selection(self) -> bool:
        """
        Does this reader support column pruning?

        If True, the query optimizer can call set_columns() to specify
        which columns are needed, allowing the reader to skip reading
        unnecessary columns.

        Returns:
            True if column selection is supported
        """
        return False

    def set_columns(self, columns: List[str]) -> None:
        """
        Set which columns to read (column pruning)

        Args:
            columns: List of column names to read

        Note:
            Only called if supports_column_selection() returns True
        """
        pass

    def supports_limit(self) -> bool:
        """
        Does this reader support early termination with LIMIT?

        If True, the query optimizer can call set_limit() to specify
        the maximum number of rows to read, allowing early termination.

        Returns:
            True if limit pushdown is supported
        """
        return False

    def set_limit(self, limit: int) -> None:
        """
        Set maximum number of rows to read (limit pushdown)

        Args:
            limit: Maximum number of rows to yield

        Note:
            Only called if supports_limit() returns True
            Reader should stop yielding rows after 'limit' rows
        """
        pass

    def supports_partition_pruning(self) -> bool:
        """
        Does this reader support partition pruning?

        If True, the query optimizer can call set_partition_filters() to specify
        which partitions to read based on filter conditions.

        Returns:
            True if partition pruning is supported
        """
        return False

    def get_partition_columns(self) -> set:
        """
        Get partition column names for Hive-style partitioning

        Returns:
            Set of partition column names (e.g., {'year', 'month', 'day'})
            Empty set if not partitioned

        Example:
            For path: s3://bucket/data/year=2024/month=01/data.parquet
            Returns: {'year', 'month'}
        """
        return set()

    def set_partition_filters(self, conditions: List[Condition]) -> None:
        """
        Set filter conditions for partition pruning

        Args:
            conditions: List of WHERE conditions on partition columns

        Note:
            Only called if supports_partition_pruning() returns True
            Reader should skip partitions that don't match these conditions
        """
        pass

    def get_schema(self) -> Optional[Schema]:
        """
        Get schema information (column names and types)

        Returns:
            Schema object with inferred types, or None if schema cannot be inferred

        Note:
            Optional method. Returns None by default.
            Readers should override this to provide schema inference.
        """
        return None

    def __iter__(self):
        """Allow readers to be used directly in for loops"""
        return self.read_lazy()

    def to_dataframe(self):
        """
        Convert reader content to pandas DataFrame

        Returns:
            pandas.DataFrame containing all data

        Note:
            Default implementation iterates over read_lazy() and creates DataFrame.
            Subclasses should override this for better performance (e.g. using read_csv/read_parquet).
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("Pandas is required for to_dataframe()")

        # Default implementation: materialize iterator
        return pd.DataFrame(list(self.read_lazy()))
