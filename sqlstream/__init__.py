"""
SQLStream - A lightweight SQL query engine for data exploration

This package provides a SQL query interface for various data formats including CSV,
Parquet, and JSON with lazy evaluation and intelligent optimizations.
"""

__version__ = "0.1.0"

# Main API
from sqlstream.core.query import query

__all__ = ["__version__", "query"]
