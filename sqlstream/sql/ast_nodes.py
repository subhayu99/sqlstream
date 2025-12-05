"""
AST (Abstract Syntax Tree) node definitions for SQL queries

These dataclasses represent the parsed structure of SQL queries.
Start with a minimal subset supporting SELECT, WHERE, and LIMIT.
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class Condition:
    """A single WHERE condition: column operator value"""

    column: str
    operator: str  # '=', '>', '<', '>=', '<=', '!=', 'IN'
    value: Any

    def __repr__(self) -> str:
        return f"{self.column} {self.operator} {self.value}"


@dataclass
class WhereClause:
    """WHERE clause containing multiple conditions"""

    conditions: list[Condition]

    def __repr__(self) -> str:
        return " AND ".join(str(c) for c in self.conditions)


@dataclass
class AggregateFunction:
    """
    Represents an aggregate function in SELECT clause

    Examples:
        COUNT(*), COUNT(id), SUM(amount), AVG(price), MIN(age), MAX(age)
    """

    function: str  # 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX'
    column: str  # Column name, or '*' for COUNT(*)
    alias: str | None = None  # AS alias

    def __repr__(self) -> str:
        result = f"{self.function}({self.column})"
        if self.alias:
            result += f" AS {self.alias}"
        return result


@dataclass
class OrderByColumn:
    """
    Represents a column in ORDER BY clause

    Examples:
        name ASC, age DESC
    """

    column: str
    direction: str = "ASC"  # 'ASC' or 'DESC', default ASC

    def __repr__(self) -> str:
        return f"{self.column} {self.direction}"


@dataclass
class JoinClause:
    """
    Represents a JOIN clause

    Examples:
        INNER JOIN orders ON customers.id = orders.customer_id
        LEFT JOIN products ON orders.product_id = products.id
    """

    right_source: str  # Right table/file name
    join_type: str  # 'INNER', 'LEFT', 'RIGHT'
    on_left: str  # Left column in join condition
    on_right: str  # Right column in join condition

    def __repr__(self) -> str:
        return f"{self.join_type} JOIN {self.right_source} ON {self.on_left} = {self.on_right}"


@dataclass
class SelectStatement:
    """
    Represents a complete SELECT statement

    Examples:
        SELECT * FROM data
        SELECT name, age FROM data WHERE age > 25
        SELECT * FROM data WHERE age > 25 LIMIT 10
        SELECT city, COUNT(*) FROM data GROUP BY city
        SELECT * FROM data ORDER BY age DESC LIMIT 10
        SELECT * FROM customers INNER JOIN orders ON customers.id = orders.customer_id
    """

    columns: list[str]  # ['*'] for all columns, or specific column names
    source: str  # Table/file name (FROM clause)
    where: WhereClause | None = None
    group_by: list[str] | None = None
    order_by: list[OrderByColumn] | None = None
    limit: int | None = None
    aggregates: list[AggregateFunction] | None = None  # Aggregate functions in SELECT
    join: JoinClause | None = None  # JOIN clause

    def __repr__(self) -> str:
        parts = [f"SELECT {', '.join(self.columns)}"]
        parts.append(f"FROM {self.source}")
        if self.where:
            parts.append(f"WHERE {self.where}")
        if self.group_by:
            parts.append(f"GROUP BY {', '.join(self.group_by)}")
        if self.order_by:
            parts.append(f"ORDER BY {', '.join(str(col) for col in self.order_by)}")
        if self.join:
            parts.append(str(self.join))
        if self.limit:
            parts.append(f"LIMIT {self.limit}")
        return " ".join(parts)
