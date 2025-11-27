"""
Query Executor - builds and executes operator trees from AST

Takes a parsed SQL AST and constructs a tree of operators
that implement the query using the Volcano pull-based model.
"""

from typing import Any, Dict, Iterator

from sqlstream.core.planner import QueryPlanner
from sqlstream.operators.filter import Filter
from sqlstream.operators.limit import Limit
from sqlstream.operators.project import Project
from sqlstream.operators.scan import Scan
from sqlstream.readers.base import BaseReader
from sqlstream.sql.ast_nodes import SelectStatement


class Executor:
    """
    Query executor - builds operator tree from AST

    The executor is responsible for:
    1. Taking a parsed SQL statement (AST)
    2. Building an operator tree (Volcano model)
    3. Executing the tree by pulling from the root

    Operator tree is built bottom-up:
        Limit (root)
          ↓
        Project
          ↓
        Filter
          ↓
        Scan (leaf)
          ↓
        Reader
    """

    def __init__(self):
        """Initialize executor"""
        self.planner = QueryPlanner()

    def execute(
        self, ast: SelectStatement, reader: BaseReader
    ) -> Iterator[Dict[str, Any]]:
        """
        Execute query and return iterator over results

        Args:
            ast: Parsed SELECT statement
            reader: Data source reader

        Returns:
            Iterator over result rows

        Example:
            >>> ast = parse("SELECT name WHERE age > 25 LIMIT 10")
            >>> executor = Executor()
            >>> results = executor.execute(ast, csv_reader)
            >>> for row in results:
            ...     print(row)
        """
        # Step 1: Apply optimizations (predicate pushdown, column pruning)
        self.planner.optimize(ast, reader)

        # Step 2: Build operator tree bottom-up
        plan = self._build_plan(ast, reader)

        # Step 3: Execute by pulling from root operator
        yield from plan

    def _build_plan(self, ast: SelectStatement, reader: BaseReader):
        """
        Build operator tree from AST

        Builds the tree bottom-up in this order:
        1. Scan (always at bottom)
        2. Filter (if WHERE clause exists)
        3. Project (if specific columns selected)
        4. Limit (if LIMIT clause exists)

        Args:
            ast: Parsed SELECT statement
            reader: Data source reader

        Returns:
            Root operator of the tree
        """
        # Start with Scan operator (leaf)
        plan = Scan(reader)

        # Add Filter if WHERE clause exists
        if ast.where:
            plan = Filter(plan, ast.where.conditions)

        # Add Project if specific columns selected
        # (Note: even with SELECT *, we add Project for consistency)
        plan = Project(plan, ast.columns)

        # Add Limit if LIMIT clause exists
        if ast.limit is not None:
            plan = Limit(plan, ast.limit)

        return plan

    def explain(self, ast: SelectStatement, reader: BaseReader) -> str:
        """
        Explain query execution plan (for debugging)

        Args:
            ast: Parsed SELECT statement
            reader: Data source reader

        Returns:
            Human-readable execution plan

        Example output:
            Query Plan:
            ============
            Limit(10)
              Project(name, age)
                Filter(age > 25)
                  Scan(CSVReader)

            Optimizations applied:
              - Predicate pushdown: 1 condition(s)
              - Column pruning: 2 column(s) selected
        """
        # Apply optimizations
        self.planner.optimize(ast, reader)

        # Build plan
        plan = self._build_plan(ast, reader)

        # Format output
        output = ["Query Plan:", "=" * 40]
        output.append(self._format_plan(plan))
        output.append("")
        output.append(self.planner.get_optimization_summary())

        return "\n".join(output)

    def _format_plan(self, operator, indent: int = 0) -> str:
        """
        Format operator tree as string

        Args:
            operator: Root operator
            indent: Current indentation level

        Returns:
            Formatted string representation
        """
        lines = []
        prefix = "  " * indent

        # Add current operator
        lines.append(f"{prefix}{operator}")

        # Recursively add child operators
        if hasattr(operator, "child") and operator.child:
            child_lines = self._format_plan(operator.child, indent + 1)
            lines.append(child_lines)

        return "\n".join(lines)
