"""
Query Optimizers - improve query performance through smart transformations

The optimizer module provides a pipeline-based optimization framework:

- Base classes: Optimizer, OptimizerPipeline
- Optimizer rules: PredicatePushdown, ColumnPruning, LimitPushdown, etc.
- QueryPlanner: Main orchestrator that applies all optimizations

Example:
    ```python
    from sqlstream.optimizers import QueryPlanner

    planner = QueryPlanner()
    planner.optimize(ast, reader)
    print(planner.get_optimization_summary())
    ```
"""

from sqlstream.optimizers.base import Optimizer, OptimizerPipeline
from sqlstream.optimizers.column_pruning import ColumnPruningOptimizer
from sqlstream.optimizers.limit_pushdown import LimitPushdownOptimizer
from sqlstream.optimizers.planner import QueryPlanner
from sqlstream.optimizers.predicate_pushdown import PredicatePushdownOptimizer
from sqlstream.optimizers.projection_pushdown import ProjectionPushdownOptimizer

__all__ = [
    "Optimizer",
    "OptimizerPipeline",
    "QueryPlanner",
    "PredicatePushdownOptimizer",
    "ColumnPruningOptimizer",
    "LimitPushdownOptimizer",
    "ProjectionPushdownOptimizer",
]
