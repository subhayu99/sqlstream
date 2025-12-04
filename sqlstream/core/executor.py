"""
Query Executor - builds and executes operator trees from AST

Takes a parsed SQL AST and constructs a tree of operators
that implement the query using the Volcano pull-based model.
"""

from typing import Any, Callable, Dict, Iterator, Optional

from sqlstream.operators.filter import Filter
from sqlstream.operators.groupby import GroupByOperator
from sqlstream.operators.join import HashJoinOperator
from sqlstream.operators.limit import Limit
from sqlstream.operators.orderby import OrderByOperator
from sqlstream.operators.project import Project
from sqlstream.operators.scan import Scan
from sqlstream.optimizers import QueryPlanner
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
        self,
        ast: SelectStatement,
        reader: BaseReader,
        reader_factory: Optional[Callable[[str], BaseReader]] = None,
    ) -> Iterator[Dict[str, Any]]:
        """
        Execute query and return iterator over results

        Args:
            ast: Parsed SELECT statement
            reader: Data source reader for the main table
            reader_factory: Optional factory function to create readers for JOIN tables

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
        plan = self._build_plan(ast, reader, reader_factory)

        # Step 3: Execute by pulling from root operator
        yield from plan

    def _build_plan(
        self,
        ast: SelectStatement,
        reader: BaseReader,
        reader_factory: Optional[Callable[[str], BaseReader]] = None,
    ):
        """
        Build operator tree from AST

        Builds the tree bottom-up in this order:
        1. Scan (always at bottom)
        2. HashJoin (if JOIN clause exists)
        3. Filter (if WHERE clause exists)
        4. GroupBy (if GROUP BY clause exists)
        5. OrderBy (if ORDER BY clause exists)
        6. Project (if specific columns selected)
        7. Limit (if LIMIT clause exists)

        Args:
            ast: Parsed SELECT statement
            reader: Data source reader for main table
            reader_factory: Optional factory to create readers for JOIN tables

        Returns:
            Root operator of the tree
        """
        # Start with Scan operator for left table (leaf)
        plan = Scan(reader)

        # Add HashJoin if JOIN clause exists
        if ast.join:
            if not reader_factory:
                raise ValueError(
                    "JOIN requires a reader_factory to create readers for joined tables"
                )

            # Create reader for right table
            right_reader = reader_factory(ast.join.right_source)

            # Create scan operator for right table
            right_scan = Scan(right_reader)

            # Create hash join operator
            plan = HashJoinOperator(
                left=plan,
                right=right_scan,
                join_type=ast.join.join_type,
                left_key=ast.join.on_left,
                right_key=ast.join.on_right,
            )

        # Add Filter if WHERE clause exists
        if ast.where:
            plan = Filter(plan, ast.where.conditions)

        # Add GroupBy if GROUP BY clause exists
        # GroupBy performs aggregation and groups rows
        if ast.group_by:
            if not ast.aggregates:
                raise ValueError("GROUP BY requires aggregate functions in SELECT")
            plan = GroupByOperator(
                plan, ast.group_by, ast.aggregates, ast.columns
            )

        # Add OrderBy if ORDER BY clause exists
        if ast.order_by:
            plan = OrderByOperator(plan, ast.order_by)

        # Add Project if specific columns selected and no GROUP BY
        # (GroupBy already handles column selection)
        if not ast.group_by:
            plan = Project(plan, ast.columns)

        # Add Limit if LIMIT clause exists
        if ast.limit is not None:
            plan = Limit(plan, ast.limit)

        return plan

    def explain(
        self,
        ast: SelectStatement,
        reader: BaseReader,
        reader_factory: Optional[Callable[[str], BaseReader]] = None,
    ) -> str:
        """
        Explain query execution plan (for debugging)

        Args:
            ast: Parsed SELECT statement
            reader: Data source reader
            reader_factory: Optional factory to create readers for JOIN tables

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
        plan = self._build_plan(ast, reader, reader_factory)

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
