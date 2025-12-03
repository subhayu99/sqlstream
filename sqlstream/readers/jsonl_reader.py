"""
JSONL Reader for reading line-delimited JSON files
"""

import json
import warnings
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from sqlstream.readers.base import BaseReader
from sqlstream.sql.ast_nodes import Condition
from sqlstream.core.types import Schema


class JSONLReader(BaseReader):
    """
    Reader for JSONL (JSON Lines) files.
    
    Format:
    {"id": 1, "name": "Alice"}
    {"id": 2, "name": "Bob"}
    
    Features:
    - True lazy loading (line-by-line)
    - Handle malformed lines
    - Predicate pushdown
    - Column pruning
    """

    def __init__(self, path: str, encoding: str = "utf-8"):
        """
        Initialize JSONL reader

        Args:
            path: Path to JSONL file
            encoding: File encoding (default: utf-8)
        """
        self.path_str = path
        self.is_s3 = path.startswith("s3://")
        if not self.is_s3:
            self.path = Path(path)
        else:
            self.path = None

        self.encoding = encoding

        # Optimization flags
        self.filter_conditions: List[Condition] = []
        self.required_columns: List[str] = []
        self.limit: Optional[int] = None

        if not self.is_s3 and not self.path.exists():
            raise FileNotFoundError(f"JSONL file not found: {path}")

    def supports_pushdown(self) -> bool:
        return True

    def supports_column_selection(self) -> bool:
        return True

    def supports_limit(self) -> bool:
        return True

    def set_filter(self, conditions: List[Condition]) -> None:
        self.filter_conditions = conditions

    def set_columns(self, columns: List[str]) -> None:
        self.required_columns = columns

    def set_limit(self, limit: int) -> None:
        self.limit = limit

    def _get_file_handle(self):
        """Get file handle for reading (local or S3)."""
        if self.is_s3:
            try:
                import s3fs
                fs = s3fs.S3FileSystem(anon=False)
                return fs.open(self.path_str, mode="r", encoding=self.encoding)
            except ImportError:
                raise ImportError("s3fs is required for S3 support. Install with: pip install sqlstream[s3]")
        else:
            return open(self.path, encoding=self.encoding)

    def read_lazy(self) -> Iterator[Dict[str, Any]]:
        """
        Yield rows from JSONL file line by line
        """
        with self._get_file_handle() as f:
            rows_yielded = 0
            
            for line_num, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    row = json.loads(line)
                    
                    if not isinstance(row, dict):
                        warnings.warn(f"Skipping non-dict row at line {line_num}", UserWarning)
                        continue

                    # Apply filters
                    if self.filter_conditions:
                        if not self._matches_filter(row):
                            continue

                    # Apply column selection
                    if self.required_columns:
                        row = {k: row.get(k) for k in self.required_columns}

                    yield row
                    rows_yielded += 1

                    if self.limit is not None and rows_yielded >= self.limit:
                        break

                except json.JSONDecodeError:
                    warnings.warn(f"Skipping invalid JSON at line {line_num}", UserWarning)
                    continue

    def _matches_filter(self, row: Dict[str, Any]) -> bool:
        """Check if row matches filter conditions"""
        for condition in self.filter_conditions:
            if not self._evaluate_condition(row, condition):
                return False
        return True

    def _evaluate_condition(self, row: Dict[str, Any], condition: Condition) -> bool:
        """Evaluate single condition"""
        if condition.column not in row:
            return False
            
        value = row[condition.column]
        if value is None:
            return False
            
        op = condition.operator
        expected = condition.value
        
        try:
            if op == "=": return value == expected
            elif op == ">": return value > expected
            elif op == "<": return value < expected
            elif op == ">=": return value >= expected
            elif op == "<=": return value <= expected
            elif op == "!=": return value != expected
            else: return True
        except TypeError:
            return False

    def get_schema(self, sample_size: int = 100) -> Optional[Schema]:
        """Infer schema by sampling first N lines"""
        sample_rows = []
        
        try:
            iterator = self.read_lazy()
            for _ in range(sample_size):
                try:
                    sample_rows.append(next(iterator))
                except StopIteration:
                    break
        except Exception:
            pass
            
        if not sample_rows:
            return None
            
        return Schema.from_rows(sample_rows)
