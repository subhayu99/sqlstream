"""
Query Planner - orchestrates optimization pipeline

The planner applies multiple optimization rules to improve query performance.
This is the main entry point for query optimization.
"""

from typing import List

from sqlstream.optimizers.base import OptimizerPipeline
from sqlstream.optimizers.column_pruning import ColumnPruningOptimizer
from sqlstream.optimizers.limit_pushdown import LimitPushdownOptimizer
from sqlstream.optimizers.predicate_pushdown import PredicatePushdownOptimizer
from sqlstream.optimizers.projection_pushdown import ProjectionPushdownOptimizer
from sqlstream.readers.base import BaseReader
from sqlstream.sql.ast_nodes import SelectStatement


class QueryPlanner:
    """
    Query planner and optimizer orchestrator

    Applies a pipeline of optimizations to improve query performance:
    1. Predicate pushdown - push WHERE filters to readers
    2. Column pruning - tell readers which columns to read
    3. Limit pushdown - early termination for LIMIT queries
    4. Projection pushdown - push computed expressions (future)

    The planner modifies the reader in-place with optimization hints.

    Example:
        ```python
        planner = QueryPlanner()
        planner.optimize(ast, reader)
        print(planner.get_optimization_summary())
        ```
    """

    def __init__(self):
        """
        Initialize planner with default optimization pipeline

        The order matters:
        1. Predicate pushdown first (reduces data read)
        2. Column pruning second (narrows columns)
        3. Limit pushdown third (early termination)
        4. Projection pushdown last (transform data at source)
        """
        self.pipeline = OptimizerPipeline(
            [
                PredicatePushdownOptimizer(),
                ColumnPruningOptimizer(),
                LimitPushdownOptimizer(),
                ProjectionPushdownOptimizer(),
            ]
        )
        # For backward compatibility with old API
        self.optimizations_applied: List[str] = []

    def optimize(self, ast: SelectStatement, reader: BaseReader) -> None:
        """
        Apply all applicable optimizations

        Args:
            ast: Parsed SQL statement
            reader: Data source reader

        Modifies:
            - reader: Sets optimization hints (filters, columns, limit, etc.)
            - self.optimizations_applied: List of applied optimizations
        """
        # Run the optimization pipeline
        self.pipeline.optimize(ast, reader)

        # Update the backward-compatible optimizations_applied list
        self.optimizations_applied = self.pipeline.get_applied_optimizations()

    def get_optimization_summary(self) -> str:
        """
        Get summary of optimizations applied

        Returns:
            Human-readable summary

        Example:
            "Optimizations applied:
             - Predicate pushdown: 2 condition(s)
             - Column pruning: 3 column(s) selected"
        """
        return self.pipeline.get_summary()

    def get_optimizers(self) -> List:
        """
        Get list of all optimizers in the pipeline

        Returns:
            List of optimizer instances
        """
        return self.pipeline.optimizers

    def add_optimizer(self, optimizer) -> None:
        """
        Add a custom optimizer to the pipeline

        Args:
            optimizer: Optimizer instance to add

        Example:
            ```python
            planner = QueryPlanner()
            planner.add_optimizer(MyCustomOptimizer())
            ```
        """
        self.pipeline.optimizers.append(optimizer)
