"""
Output formatters for CLI

Available formatters:
- TableFormatter: Beautiful Rich tables
- JSONFormatter: Machine-readable JSON
- CSVFormatter: Unix-friendly CSV
"""

from sqlstream.cli.formatters.base import BaseFormatter
from sqlstream.cli.formatters.csv import CSVFormatter
from sqlstream.cli.formatters.json import JSONFormatter
from sqlstream.cli.formatters.table import TableFormatter

__all__ = ["BaseFormatter", "TableFormatter", "JSONFormatter", "CSVFormatter"]


def get_formatter(format_name: str) -> BaseFormatter:
    """
    Get formatter by name

    Args:
        format_name: Name of formatter (table, json, csv)

    Returns:
        Formatter instance

    Raises:
        ValueError: If formatter not found
    """
    formatters = {
        "table": TableFormatter,
        "json": JSONFormatter,
        "csv": CSVFormatter,
    }

    if format_name not in formatters:
        available = ", ".join(formatters.keys())
        raise ValueError(
            f"Unknown format: {format_name}. Available formats: {available}"
        )

    return formatters[format_name]()
