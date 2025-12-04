"""
Interactive TUI mode with Textual

This module provides an interactive table viewer with scrolling for wide data.
"""

import shutil
import sys
from typing import Any, Dict, List

try:
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.widgets import DataTable, Footer, Header

    TEXTUAL_AVAILABLE = True
except ImportError:
    TEXTUAL_AVAILABLE = False
    App = None
    ComposeResult = None
    Binding = None
    DataTable = None
    Footer = None
    Header = None


def should_use_interactive(
    results: List[Dict[str, Any]],
    force: bool = False,
    no_interactive: bool = False,
    output_file: str = None,
    fmt: str = "table",
) -> bool:
    """
    Determine if interactive mode should be used.

    Args:
        results: Query results to display
        force: Force interactive mode (--interactive flag)
        no_interactive: Disable interactive mode (--no-interactive flag)
        output_file: Output file path (if set, disable interactive)
        fmt: Output format (only use interactive for table format)

    Returns:
        True if interactive mode should be used, False otherwise
    """
    # Force flag overrides everything
    if force:
        return True

    # Never use interactive if:
    if no_interactive or output_file or fmt != "table":
        return False

    # Not a TTY (piped output)
    if not sys.stdout.isatty():
        return False

    # No results = no need for interactive
    if not results:
        return False

    # Auto-detection logic
    terminal_width = shutil.get_terminal_size().columns
    columns = list(results[0].keys())
    num_cols = len(columns)

    # Too many columns
    if num_cols > 10:
        return True

    # Terminal too narrow
    if terminal_width < 80 and num_cols > 5:
        return True

    # Estimate table width (rough: col_name + avg_value_length + borders)
    estimated_width = 0
    for col in columns:
        col_width = len(col)
        # Sample first 5 rows to estimate column width
        for row in results[: min(5, len(results))]:
            val_len = len(str(row[col]))
            col_width = max(col_width, val_len)
        estimated_width += col_width + 3  # +3 for padding/borders

    # Table too wide for terminal
    if estimated_width > terminal_width * 0.9:
        return True

    # Check for very long values
    for row in results[: min(10, len(results))]:  # Sample first 10 rows
        for val in row.values():
            if len(str(val)) > 50:
                return True

    return False


if TEXTUAL_AVAILABLE:

    class TableApp(App):
        """Interactive table viewer with scrolling."""

        CSS = """
        DataTable {
            height: 100%;
        }
        """

        BINDINGS = [
            Binding("q", "quit", "Quit"),
            Binding("escape", "quit", "Quit"),
            ("j", "cursor_down", "Down"),
            ("k", "cursor_up", "Up"),
            ("h", "cursor_left", "Left"),
            ("l", "cursor_right", "Right"),
        ]

        def __init__(self, results: List[Dict[str, Any]], **kwargs):
            super().__init__(**kwargs)
            self.results = results

        def compose(self) -> ComposeResult:
            """Compose the UI."""
            yield Header()
            yield DataTable()
            yield Footer()

        def on_mount(self) -> None:
            """Populate the table on mount."""
            table = self.query_one(DataTable)

            # Get columns from first row
            if not self.results:
                return

            columns = list(self.results[0].keys())

            # Add columns
            for col in columns:
                table.add_column(col, key=col)

            # Add rows
            for i, row in enumerate(self.results):
                values = [str(row[col]) if row[col] is not None else "NULL" for col in columns]
                table.add_row(*values, key=f"row_{i}")

            # Configure table
            table.zebra_stripes = True
            table.cursor_type = "row"
            table.fixed_rows = 1  # Keep header visible

        def action_quit(self) -> None:
            """Quit the app."""
            self.exit()

else:
    # Placeholder when textual not available
    TableApp = None


def launch_interactive(results: List[Dict[str, Any]]) -> None:
    """
    Launch interactive table viewer.

    Args:
        results: Query results to display

    Raises:
        ImportError: If textual library is not installed
    """
    if not TEXTUAL_AVAILABLE:
        raise ImportError("Interactive mode requires textual library. Install `sqlstream[cli]`")

    app = TableApp(results)
    app.run()
