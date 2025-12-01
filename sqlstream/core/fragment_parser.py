"""
URL Fragment Parser - Parse source#format:table syntax

Provides utilities to parse SQLstream URL fragments for format and table specification.
"""

from typing import Optional, Tuple


class FragmentParseError(Exception):
    """Raised when fragment parsing fails"""
    pass


def parse_source_fragment(source: str) -> Tuple[str, Optional[str], Optional[int]]:
    """
    Parse source URL with optional fragment

    Syntax: source#format:table

    Examples:
        "data.csv" → ("data.csv", None, None)
        "data.html#html" → ("data.html", "html", None)
        "page.html#html:1" → ("page.html", "html", 1)
        "README.md#markdown:-1" → ("README.md", "markdown", -1)
        "data#:2" → ("data", None, 2)
        "https://example.com/data#csv:0" → ("https://example.com/data", "csv", 0)

    Args:
        source: Source path or URL, optionally with #format:table fragment

    Returns:
        Tuple of (source_path, format, table_index)
        - source_path: Path/URL without fragment
        - format: Data format (csv, html, markdown, parquet) or None for auto-detect
        - table_index: Table index (0-based, supports negative) or None for default (0)

    Raises:
        FragmentParseError: If fragment syntax is invalid
    """
    if '#' not in source:
        return (source, None, None)

    # Split on LAST # to handle URLs with # in path
    # (though this is rare, fragment should be last part)
    parts = source.rsplit('#', 1)
    source_path = parts[0]
    fragment = parts[1]

    if not fragment:
        # Empty fragment (#) - just return source
        return (source_path, None, None)

    # Parse fragment: format:table
    if ':' in fragment:
        format_part, table_part = fragment.split(':', 1)

        # Empty format means auto-detect
        format_spec = format_part.strip() if format_part.strip() else None

        # Validate format if specified
        if format_spec and format_spec not in ('csv', 'parquet', 'html', 'markdown', 'json'):
            raise FragmentParseError(
                f"Unknown format '{format_spec}'. "
                f"Supported formats: csv, parquet, html, markdown, json"
            )

        # Parse table index (supports negative)
        if not table_part.strip():
            raise FragmentParseError("Table index cannot be empty after ':'")

        try:
            table_index = int(table_part)
        except ValueError:
            raise FragmentParseError(
                f"Invalid table index: '{table_part}'. Must be an integer."
            )

        return (source_path, format_spec, table_index)
    else:
        # Just format, no table specified
        format_spec = fragment.strip()

        # Validate format
        if format_spec not in ('csv', 'parquet', 'html', 'markdown', 'json'):
            raise FragmentParseError(
                f"Unknown format '{format_spec}'. "
                f"Supported formats: csv, parquet, html, markdown, json"
            )

        return (source_path, format_spec, None)


def build_source_fragment(
    source: str,
    format: Optional[str] = None,
    table: Optional[int] = None
) -> str:
    """
    Build a source string with fragment from components

    Args:
        source: Source path/URL
        format: Optional format specification
        table: Optional table index

    Returns:
        Formatted source string with fragment

    Examples:
        - build_source_fragment("data.html", "html", 1) → "data.html#html:1"
        - build_source_fragment("data.csv", "csv") → "data.csv#csv"
        - build_source_fragment("data.html", table=2) → "data.html#:2"
    """
    if not format and table is None:
        return source

    if format and table is not None:
        return f"{source}#{format}:{table}"
    elif format:
        return f"{source}#{format}"
    else:  # table only
        return f"{source}#:{table}"
