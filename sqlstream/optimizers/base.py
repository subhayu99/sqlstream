"""
Base classes for query optimizers

Optimizers analyze the query AST and apply transformations to improve performance.
Each optimizer implements a specific optimization rule.
"""

from abc import ABC, abstractmethod
from typing import List

from sqlstream.readers.base import BaseReader
from sqlstream.sql.ast_nodes import SelectStatement


class Optimizer(ABC):
    """
    Base class for all query optimizers

    Each optimizer implements a single optimization rule.
    Optimizers are applied in a pipeline before query execution.
    """

    def __init__(self):
        """Initialize optimizer"""
        self.applied = False
        self.description = ""

    @abstractmethod
    def can_optimize(self, ast: SelectStatement, reader: BaseReader) -> bool:
        """
        Check if this optimization can be applied

        Args:
            ast: Parsed SQL statement
            reader: Data source reader

        Returns:
            True if optimization is applicable
        """
        pass

    @abstractmethod
    def optimize(self, ast: SelectStatement, reader: BaseReader) -> None:
        """
        Apply the optimization

        Args:
            ast: Parsed SQL statement
            reader: Data source reader

        Modifies:
            - reader: Sets optimization hints
            - self.applied: Marks optimization as applied
            - self.description: Describes what was optimized
        """
        pass

    @abstractmethod
    def get_name(self) -> str:
        """
        Get the name of this optimizer

        Returns:
            Human-readable optimizer name
        """
        pass

    def get_description(self) -> str:
        """
        Get description of what was optimized

        Returns:
            Description string if applied, empty string otherwise
        """
        return self.description if self.applied else ""

    def was_applied(self) -> bool:
        """
        Check if optimization was applied

        Returns:
            True if optimization was applied
        """
        return self.applied


class OptimizerPipeline:
    """
    Pipeline that applies multiple optimizers in sequence

    Optimizers are applied in order, and each can build on
    the previous optimizations.
    """

    def __init__(self, optimizers: List[Optimizer]):
        """
        Initialize pipeline

        Args:
            optimizers: List of optimizers to apply in order
        """
        self.optimizers = optimizers

    def optimize(self, ast: SelectStatement, reader: BaseReader) -> None:
        """
        Apply all optimizers in sequence

        Args:
            ast: Parsed SQL statement
            reader: Data source reader
        """
        for optimizer in self.optimizers:
            if optimizer.can_optimize(ast, reader):
                optimizer.optimize(ast, reader)

    def get_applied_optimizations(self) -> List[str]:
        """
        Get list of optimizations that were applied

        Returns:
            List of optimization descriptions
        """
        return [
            f"{opt.get_name()}: {opt.get_description()}"
            for opt in self.optimizers
            if opt.was_applied()
        ]

    def get_summary(self) -> str:
        """
        Get summary of all applied optimizations

        Returns:
            Human-readable summary
        """
        applied = self.get_applied_optimizations()

        if not applied:
            return "No optimizations applied"

        summary = "Optimizations applied:\n"
        for opt_desc in applied:
            summary += f"  - {opt_desc}\n"

        return summary.strip()
