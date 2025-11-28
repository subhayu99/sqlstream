"""
Join Operator

Implements hash-based equi-join for INNER, LEFT, and RIGHT joins.

Hash Join Algorithm:
1. Build Phase: Scan right table and build hash table on join key
2. Probe Phase: Scan left table and probe hash table for matches
3. Output joined rows
"""

from typing import Any, Dict, Iterator, List

from sqlstream.operators.base import Operator


class HashJoinOperator(Operator):
    """
    Hash Join operator for equi-joins

    Supports:
    - INNER JOIN: Only matching rows from both tables
    - LEFT JOIN: All rows from left, matched rows from right (NULL if no match)
    - RIGHT JOIN: All rows from right, matched rows from left (NULL if no match)

    Algorithm:
    1. Build hash table from right table (keyed by join column)
    2. Probe hash table with rows from left table
    3. Output joined rows based on join type

    Note: This operator materializes the right table in memory.
    For very large right tables, consider external hash join or other algorithms.
    """

    def __init__(
        self,
        left: Operator,
        right: Operator,
        join_type: str,
        left_key: str,
        right_key: str,
    ):
        """
        Initialize Hash Join operator

        Args:
            left: Left table operator
            right: Right table operator
            join_type: 'INNER', 'LEFT', or 'RIGHT'
            left_key: Column name in left table for join condition
            right_key: Column name in right table for join condition
        """
        # Store both children (join has two inputs)
        super().__init__(left)
        self.left = left
        self.right = right
        self.join_type = join_type.upper()
        self.left_key = left_key
        self.right_key = right_key

        # Validate join type
        if self.join_type not in ("INNER", "LEFT", "RIGHT"):
            raise ValueError(f"Unsupported join type: {join_type}")

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """
        Execute hash join

        Yields:
            Joined rows with columns from both tables
        """
        if self.join_type == "INNER":
            yield from self._inner_join()
        elif self.join_type == "LEFT":
            yield from self._left_join()
        elif self.join_type == "RIGHT":
            yield from self._right_join()

    def _inner_join(self) -> Iterator[Dict[str, Any]]:
        """
        Execute INNER JOIN

        Returns only rows that have matching join keys in both tables.
        """
        # Build phase: Create hash table from right table
        hash_table = self._build_hash_table()

        # Probe phase: Scan left table and find matches
        for left_row in self.left:
            join_key = left_row.get(self.left_key)

            # Skip rows with NULL join key (standard SQL behavior)
            if join_key is None:
                continue

            # Probe hash table
            if join_key in hash_table:
                # Found match(es) - join with all matching right rows
                for right_row in hash_table[join_key]:
                    yield self._merge_rows(left_row, right_row)

    def _left_join(self) -> Iterator[Dict[str, Any]]:
        """
        Execute LEFT JOIN

        Returns all rows from left table. If there's a match in right table,
        include right columns. If no match, right columns are NULL.
        """
        # Build phase: Create hash table from right table
        hash_table = self._build_hash_table()

        # Probe phase: Scan left table
        for left_row in self.left:
            join_key = left_row.get(self.left_key)

            # Check for match
            if join_key is not None and join_key in hash_table:
                # Found match(es) - join with all matching right rows
                for right_row in hash_table[join_key]:
                    yield self._merge_rows(left_row, right_row)
            else:
                # No match - output left row with NULL for right columns
                yield self._merge_rows(left_row, None)

    def _right_join(self) -> Iterator[Dict[str, Any]]:
        """
        Execute RIGHT JOIN

        Returns all rows from right table. If there's a match in left table,
        include left columns. If no match, left columns are NULL.
        """
        # Build phase: Create hash table from right table
        # Also track which right rows were matched
        hash_table = self._build_hash_table()
        matched_right_rows = set()  # Track (join_key, row_index) tuples

        # Probe phase: Scan left table and output matches
        for left_row in self.left:
            join_key = left_row.get(self.left_key)

            if join_key is not None and join_key in hash_table:
                # Found match(es) - join with all matching right rows
                for idx, right_row in enumerate(hash_table[join_key]):
                    yield self._merge_rows(left_row, right_row)
                    # Mark this right row as matched
                    matched_right_rows.add((join_key, idx))

        # Output unmatched right rows with NULL for left columns
        for join_key, right_rows in hash_table.items():
            for idx, right_row in enumerate(right_rows):
                if (join_key, idx) not in matched_right_rows:
                    yield self._merge_rows(None, right_row)

    def _build_hash_table(self) -> Dict[Any, List[Dict[str, Any]]]:
        """
        Build hash table from right table

        Returns:
            Hash table mapping join key values to lists of matching rows
        """
        hash_table: Dict[Any, List[Dict[str, Any]]] = {}

        for row in self.right:
            join_key = row.get(self.right_key)

            # Skip rows with NULL join key (they can never match)
            if join_key is None:
                continue

            # Handle unhashable types (e.g., lists, dicts)
            if isinstance(join_key, (list, dict)):
                join_key = str(join_key)

            # Add row to hash table
            if join_key not in hash_table:
                hash_table[join_key] = []
            hash_table[join_key].append(row)

        return hash_table

    def _merge_rows(
        self, left_row: Dict[str, Any] | None, right_row: Dict[str, Any] | None
    ) -> Dict[str, Any]:
        """
        Merge left and right rows into a single output row

        Handles column name conflicts by prefixing with table names if needed.

        Args:
            left_row: Row from left table (None for RIGHT JOIN with no match)
            right_row: Row from right table (None for LEFT JOIN with no match)

        Returns:
            Merged row dictionary
        """
        result = {}

        # Add left columns
        if left_row is not None:
            result.update(left_row)
        elif right_row is not None:
            # For RIGHT JOIN with no match, add NULL for all left columns
            # We don't know the left schema, so we just don't add anything
            # The columns will be added on first matched row
            pass

        # Add right columns
        if right_row is not None:
            for key, value in right_row.items():
                # Handle column name conflicts
                if key in result and left_row is not None:
                    # Column exists in both tables - this shouldn't happen with
                    # proper column qualification in SELECT, but handle it
                    # by keeping the left value and prefixing right with "right_"
                    result[f"right_{key}"] = value
                else:
                    result[key] = value

        return result

    def explain(self, indent: int = 0) -> List[str]:
        """Generate execution plan explanation"""
        lines = [
            " " * indent
            + f"HashJoin({self.join_type}, {self.left_key} = {self.right_key})"
        ]
        lines.append(" " * (indent + 2) + "Left:")
        lines.extend(self.left.explain(indent + 4))
        lines.append(" " * (indent + 2) + "Right:")
        lines.extend(self.right.explain(indent + 4))
        return lines
