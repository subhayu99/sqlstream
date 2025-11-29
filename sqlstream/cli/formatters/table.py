"""
Rich table formatter for beautiful terminal output
"""

from typing import Any, Dict, List

try:
    from rich.console import Console
    from rich.table import Table

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    Console = None
    Table = None

from sqlstream.cli.formatters.base import BaseFormatter


class TableFormatter(BaseFormatter):
    """Format results as a beautiful Rich table"""

    def format(self, results: List[Dict[str, Any]], **kwargs) -> str:
        """
        Format results as a Rich table

        Args:
            results: List of result dictionaries
            **kwargs: Options like 'no_color', 'show_footer'

        Returns:
            Formatted table string
        """
        if not RICH_AVAILABLE:
            raise ImportError(
                "Table formatter requires rich library. "
                "Install with: pip install sqlstream[cli]"
            )

        if not results:
            return "No results found."

        # Get column names from first row
        columns = list(results[0].keys())

        # Create Rich table
        table = Table(show_header=True, header_style="bold magenta")

        # Add columns
        for col in columns:
            table.add_column(col, style="cyan", no_wrap=False)

        # Add rows
        for row in results:
            # Convert None to "NULL" for display
            values = [str(row[col]) if row[col] is not None else "[dim]NULL[/dim]" for col in columns]
            table.add_row(*values)

        # Render table to string
        console = Console(force_terminal=not kwargs.get("no_color", False))
        with console.capture() as capture:
            console.print(table)

        output = capture.get()

        # Add footer with row count if requested
        if kwargs.get("show_footer", True):
            row_count = len(results)
            footer = f"\n[dim]{row_count} row{'s' if row_count != 1 else ''}[/dim]"
            with console.capture() as capture:
                console.print(footer)
            output += capture.get()

        return output
