"""
Rich table formatter for beautiful terminal output
"""

from typing import Any, Dict, List

try:
    from rich import box
    from rich.console import Console
    from rich.table import Table

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False
    Console = None
    Table = None
    box = None

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

        # Get terminal width and column info
        console = Console(force_terminal=not kwargs.get("no_color", False))
        terminal_width = console.width
        columns = list(results[0].keys())
        num_cols = len(columns)

        # Adaptive column width based on terminal size
        if terminal_width < 80 or num_cols > 8:
            # Narrow terminal or many columns: aggressive truncation
            max_col_width = kwargs.get("max_width", 15)
            table = Table(show_header=True, header_style="bold magenta", box=box.SIMPLE)

            for col in columns:
                table.add_column(
                    col,
                    style="cyan",
                    overflow="ellipsis",
                    max_width=max_col_width,
                    no_wrap=True,
                )
        else:
            # Normal table with moderate truncation
            table = Table(show_header=True, header_style="bold magenta")

            for col in columns:
                table.add_column(
                    col, style="cyan", overflow="ellipsis", max_width=30, no_wrap=False
                )

        # Add rows
        for row in results:
            # Convert None to "NULL" for display
            values = [
                str(row[col]) if row[col] is not None else "[dim]NULL[/dim]"
                for col in columns
            ]
            table.add_row(*values)

        # Render table to string
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
