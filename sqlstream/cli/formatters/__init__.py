"""
Output formatters for CLI

Available formatters:
- TableFormatter: Beautiful Rich tables
- JSONFormatter: Machine-readable JSON
- CSVFormatter: Unix-friendly CSV
- MarkdownFormatter: GitHub Flavored Markdown tables
"""

from sqlstream.cli.formatters.base import BaseFormatter
from sqlstream.cli.formatters.csv import CSVFormatter
from sqlstream.cli.formatters.json import JSONFormatter
from sqlstream.cli.formatters.markdown import MarkdownFormatter
from sqlstream.cli.formatters.table import TableFormatter

__all__ = ["BaseFormatter", "TableFormatter", "JSONFormatter", "CSVFormatter", "MarkdownFormatter"]


def get_formatter(format_name: str) -> BaseFormatter:
    """
    Get formatter by name

    Args:
        format_name: Name of formatter (table, json, csv, markdown)

    Returns:
        Formatter instance

    Raises:
        ValueError: If formatter not found
    """
    formatters = {
        "table": TableFormatter,
        "json": JSONFormatter,
        "csv": CSVFormatter,
        "markdown": MarkdownFormatter,
    }

    if format_name not in formatters:
        available = ", ".join(formatters.keys())
        raise ValueError(f"Unknown format: {format_name}. Available formats: {available}")

    return formatters[format_name]()
