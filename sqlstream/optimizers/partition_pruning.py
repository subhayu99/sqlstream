"""
Partition Pruning Optimizer

Skips reading entire partitions based on filter conditions.
Especially effective for Hive-style partitioned datasets.

Example:
    data/
      year=2023/
        month=01/data.parquet
        month=02/data.parquet
      year=2024/
        month=01/data.parquet
        month=02/data.parquet

    Query: SELECT * FROM data WHERE year = 2024
    → Only read year=2024 partitions (skip year=2023)
"""

from typing import Dict, List, Set

from sqlstream.optimizers.base import Optimizer
from sqlstream.readers.base import BaseReader
from sqlstream.sql.ast_nodes import Condition, SelectStatement


class PartitionPruningOptimizer(Optimizer):
    """
    Prune (skip) partitions that don't match filter conditions

    Benefits:
    - Massive I/O reduction for partitioned datasets
    - Skip entire directories/files
    - Critical for data lakes and big data
    - Can reduce data read by 10x-1000x

    Example:
        Dataset partitioned by date: year=YYYY/month=MM/day=DD/
        Query: WHERE date >= '2024-01-01'
        → Skip all partitions before 2024
    """

    def get_name(self) -> str:
        return "Partition pruning"

    def can_optimize(self, ast: SelectStatement, reader: BaseReader) -> bool:
        """
        Check if partition pruning is applicable

        Conditions:
        1. Reader supports partition pruning
        2. Query has WHERE clause
        3. WHERE clause references partition columns

        Args:
            ast: Parsed SQL statement
            reader: Data source reader

        Returns:
            True if optimization can be applied
        """
        # Reader must support partition pruning
        if not hasattr(reader, 'supports_partition_pruning'):
            return False

        if not reader.supports_partition_pruning():
            return False

        # Must have WHERE clause
        if not ast.where:
            return False

        # Check if any filter conditions reference partition columns
        partition_cols = reader.get_partition_columns()
        if not partition_cols:
            return False

        filter_cols = {cond.column for cond in ast.where.conditions}
        if not filter_cols.intersection(partition_cols):
            return False

        return True

    def optimize(self, ast: SelectStatement, reader: BaseReader) -> None:
        """
        Apply partition pruning optimization

        Args:
            ast: Parsed SQL statement
            reader: Data source reader
        """
        # Extract conditions that reference partition columns
        partition_cols = reader.get_partition_columns()
        partition_filters = []
        non_partition_filters = []

        for cond in ast.where.conditions:
            if cond.column in partition_cols:
                partition_filters.append(cond)
            else:
                non_partition_filters.append(cond)

        if partition_filters:
            reader.set_partition_filters(partition_filters)

            # IMPORTANT: Remove partition filters from WHERE clause
            # Partition columns are virtual (from directory path) and don't exist in data
            # They should only be used for partition pruning, not row-level filtering
            ast.where.conditions = non_partition_filters

            self.applied = True
            self.description = f"{len(partition_filters)} partition filter(s)"
