"""
SQLStream Interactive Shell

A full-featured interactive SQL shell using Textual TUI framework.
Allows users to write and execute queries, view results, browse schemas,
and export data - all from a beautiful terminal interface.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import DataTable, Footer, Header, Static, TextArea, Tree

try:
    from sqlstream.core.query import query, parse, QueryInline
    from sqlstream.core.types import Schema
except ImportError:
    # Fallback for development
    from sqlstream import query


class QueryEditor(TextArea):
    """Multi-line SQL query editor with syntax highlighting."""

    BINDINGS = [
        Binding("ctrl+enter", "execute_query", "Execute", priority=True),
        Binding("ctrl+e", "execute_query", "Execute (Alt)", priority=True),
        Binding("ctrl+l", "clear_editor", "Clear", priority=True),
        Binding("ctrl+up", "history_prev", "Prev Query", priority=True),
        Binding("ctrl+down", "history_next", "Next Query", priority=True),
    ]

    def action_execute_query(self) -> None:
        """Execute the current query."""
        self.post_message(self.ExecuteQuery(self.text))

    def action_clear_editor(self) -> None:
        """Clear the query editor."""
        self.clear()
        self.focus()

    def action_history_prev(self) -> None:
        """Show previous query from history."""
        self.app.action_history_prev()

    def action_history_next(self) -> None:
        """Show next query from history."""
        self.app.action_history_next()

    class ExecuteQuery(TextArea.Changed):
        """Message sent when user wants to execute a query."""

        def __init__(self, query_text: str) -> None:
            super().__init__(query_text)
            self.query_text = query_text


class StatusBar(Static):
    """Status bar showing messages and execution info."""

    def __init__(self, **kwargs) -> None:
        super().__init__("", **kwargs)
        self.last_execution_time: Optional[float] = None
        self.row_count: Optional[int] = None

    def update_status(
        self, message: str, execution_time: Optional[float] = None, row_count: Optional[int] = None
    ) -> None:
        """Update status bar with execution info."""
        self.last_execution_time = execution_time
        self.row_count = row_count

        status_parts = [message]
        if row_count is not None:
            status_parts.append(f"{row_count} rows")
        if execution_time is not None:
            status_parts.append(f"{execution_time:.3f}s")

        self.update(" | ".join(status_parts))


class ResultsViewer(DataTable):
    """Interactive results viewer with scrolling."""

    def __init__(self, **kwargs) -> None:
        super().__init__(zebra_stripes=True, cursor_type="row", **kwargs)
        self.border_title = "Results"


class SchemaBrowser(Tree):
    """Side panel for browsing files and schemas."""

    def __init__(self, **kwargs) -> None:
        super().__init__("Data Sources", **kwargs)
        self.border_title = "Schema"
        self.show_root = False

    def show_schemas(self, schemas: Dict[str, Dict[str, str]]) -> None:
        """Update the schema tree with files and columns."""
        self.clear()
        self.root.expand()

        if not schemas:
            self.root.add("No files loaded")
            return

        for filename, schema in schemas.items():
            # Add file node
            file_node = self.root.add(Path(filename).name, expand=True)

            # Add columns
            for col, dtype in schema.items():
                if col == "Error":
                    file_node.add(f"[red]Error: {dtype}[/red]")
                else:
                    file_node.add(f"[green]{col}[/green]: [dim]{dtype}[/dim]")


class SQLShellApp(App):
    """
    SQLStream Interactive Shell Application.

    A full-featured SQL REPL with query editing, results viewing,
    schema browsing, and query history.
    """

    CSS = """
    Screen {
        background: $surface;
    }

    #main-container {
        width: 100%;
        height: 100%;
    }

    #schema-container {
        width: 25%;
        height: 100%;
        border: solid $primary;
        background: $panel;
        display: none;
    }

    #schema-container.visible {
        display: block;
    }

    #right-panel {
        width: 100%;
        height: 100%;
    }

    #query-container {
        height: 30%;
        border: solid $primary;
        background: $panel;
    }

    #query-editor {
        height: 100%;
        border: none;
    }

    #results-container {
        height: 65%;
        border: solid $accent;
        margin-top: 1;
    }

    #results-viewer {
        height: 100%;
    }

    #status-bar {
        height: 1;
        background: $boost;
        color: $text;
        padding: 0 0;
        text-align: center;
    }

    .error {
        background: $error;
        color: $text;
    }

    .success {
        background: $success;
        color: $text;
    }
    """

    BINDINGS = [
        Binding("f1", "show_help", "Help"),
        Binding("f2", "toggle_schema", "Schema"),
        Binding("f3", "toggle_history", "History", show=False),
        Binding("f4", "toggle_explain", "Explain"),
        Binding("ctrl+d", "quit", "Exit"),
        Binding("ctrl+x", "export_results", "Export"),
        Binding("ctrl+f", "filter_results", "Filter"),
        Binding("ctrl+p", "prev_page", "Prev Page", priority=True),
        Binding("ctrl+n", "next_page", "Next Page", priority=True),
        Binding("[", "prev_page", "◀ Prev", show=False, priority=True),
        Binding("]", "next_page", "Next ▶", show=False, priority=True),
    ]

    def __init__(
        self,
        initial_file: Optional[str] = None,
        history_file: Optional[str] = None,
        **kwargs,
    ):
        """
        Initialize the SQL shell.

        Args:
            initial_file: Optional file to load on startup
            history_file: Path to query history file
        """
        super().__init__(**kwargs)
        self.initial_file = initial_file
        self.history_file = history_file or str(Path.home() / ".sqlstream_history")
        self.query_history: List[str] = []
        self.history_index = -1
        self.last_results: List[Dict[str, Any]] = []
        self.filtered_results: List[Dict[str, Any]] = []
        self.query_engine = QueryInline()
        self.loaded_files: List[str] = []
        
        # Pagination state
        self.page_size = 100
        self.current_page = 0
        
        # Filter and sort state
        self.filter_text = ""
        self.sort_column = None
        self.sort_reverse = False
        
        if initial_file:
            self.loaded_files.append(initial_file)

    def compose(self) -> ComposeResult:
        """Compose the application layout."""
        yield Header(show_clock=True)

        with Horizontal(id="main-container"):
            # Schema Browser (Hidden by default)
            with Container(id="schema-container"):
                yield SchemaBrowser(id="schema-browser")

            # Main Content
            with Vertical(id="right-panel"):
                # Query Editor Container
                with Container(id="query-container"):
                    yield QueryEditor(
                        id="query-editor",
                        language="sql",
                        theme="monokai",
                        show_line_numbers=True,
                    )

                # Results Viewer Container
                with Container(id="results-container"):
                    yield ResultsViewer(id="results-viewer")

        # Status Bar
        yield StatusBar(id="status-bar")

        yield Footer()

    def on_mount(self) -> None:
        """Initialize the shell on mount."""
        self.title = "SQLStream Interactive Shell"
        self.sub_title = "Press Ctrl+Enter to execute query, Ctrl+D to exit"

        # Load query history
        self._load_history()

        # Show welcome message
        status_bar = self.query_one(StatusBar)
        status_bar.update_status(
            "Welcome to SQLStream! Type your SQL query and press Ctrl+Enter to execute."
        )

        # Focus the query editor
        self.query_one(QueryEditor).focus()

        # Load initial file if provided
        if self.initial_file:
            self._load_initial_file()
            self._update_schema_browser()

    def on_data_table_header_selected(self, event: DataTable.HeaderSelected) -> None:
        """Handle column header clicks for sorting."""
        # Get the column key - might be ColumnKey object
        column_key = event.column_key
        if hasattr(column_key, 'value'):
            column_key = column_key.value
        else:
            column_key = str(column_key)
        
        if not self.last_results:
            return
        
        # Toggle sort direction if clicking same column
        if self.sort_column == column_key:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = column_key
            self.sort_reverse = False
        
        # Reapply filter and sort
        self.filtered_results = self._apply_filter(self.last_results)
        self.filtered_results = self._apply_sort(self.filtered_results)
        self.current_page = 0
        self._refresh_displayed_results()
        
        direction = "↓" if self.sort_reverse else "↑"
        self._show_status(f"Sorted by {column_key} {direction}")

    def action_history_prev(self) -> None:
        """Navigate to previous query in history."""
        if not self.query_history:
            return

        if self.history_index == -1:
            # Start browsing from end
            self.history_index = len(self.query_history) - 1
        elif self.history_index > 0:
            self.history_index -= 1

        self._set_editor_text(self.query_history[self.history_index])

    def action_history_next(self) -> None:
        """Navigate to next query in history."""
        if not self.query_history or self.history_index == -1:
            return

        if self.history_index < len(self.query_history) - 1:
            self.history_index += 1
            self._set_editor_text(self.query_history[self.history_index])
        else:
            # Reset to empty/current
            self.history_index = -1
            self._set_editor_text("")

    def _set_editor_text(self, text: str) -> None:
        """Set text in query editor."""
        editor = self.query_one(QueryEditor)
        editor.text = text
        if text:
            editor.cursor_location = (len(text.splitlines()) - 1, len(text.splitlines()[-1]))
        else:
            editor.cursor_location = (0, 0)

    def on_query_editor_execute_query(self, message: QueryEditor.ExecuteQuery) -> None:
        """Handle query execution request."""
        query_text = message.query_text.strip()

        if not query_text:
            self._show_status("No query to execute", error=True)
            return

        # Add to history if new
        if not self.query_history or self.query_history[-1] != query_text:
            self.query_history.append(query_text)
            self._save_history()

        # Reset history index
        self.history_index = -1

        # Execute query
        self._execute_query(query_text)

    def _execute_query(self, query_text: str) -> None:
        """Execute a SQL query and display results."""
        status_bar = self.query_one(StatusBar)
        results_viewer = self.query_one(ResultsViewer)

        try:
            # Clear previous results
            results_viewer.clear(columns=True)

            # Show loading status
            status_bar.update_status("Executing query...")

            # Execute query
            start_time = datetime.now()
            result = self.query_engine.sql(query_text)

            # Update loaded files
            _parsed = parse(query_text)
            self.loaded_files.append(_parsed.source)
            if _parsed.join and _parsed.join.right_source:
                self.loaded_files.append(_parsed.join.right_source)

            # Update schema browser
            self._update_schema_browser()

            # Get results
            results = result.to_list()
            execution_time = (datetime.now() - start_time).total_seconds()

            # Store results
            self.last_results = results

            # Display results
            if results:
                self._display_results(results, execution_time)
            else:
                results_viewer.clear(columns=True)
                status_bar.update_status("Query executed successfully (no results)", execution_time=execution_time, row_count=0)

        except Exception as e:
            self._show_error(str(e))

    def _display_results(self, results: List[Dict[str, Any]], execution_time: float) -> None:
        """Display query results in the DataTable with pagination."""
        results_viewer = self.query_one(ResultsViewer)
        status_bar = self.query_one(StatusBar)

        # Get column names from first row
        if not results:
            return

        # Apply filter if set
        self.filtered_results = self._apply_filter(results)
        
        # Apply sort if set
        if self.sort_column:
            self.filtered_results = self._apply_sort(self.filtered_results)
        
        # Reset to first page
        self.current_page = 0
        
        # Display current page
        self._refresh_displayed_results(execution_time)

    def _apply_filter(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply filter text to results."""
        if not self.filter_text:
            return results
        
        filtered = []
        filter_lower = self.filter_text.lower()
        for row in results:
            # Check if filter text appears in any column value
            if any(filter_lower in str(v).lower() for v in row.values()):
                filtered.append(row)
        return filtered

    def _apply_sort(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Sort results by column."""
        if not self.sort_column or not results:
            return results
        
        try:
            return sorted(
                results,
                key=lambda x: x.get(self.sort_column, ""),
                reverse=self.sort_reverse
            )
        except Exception:
            return results

    def _refresh_displayed_results(self, execution_time: Optional[float] = None) -> None:
        """Refresh the displayed results with current page."""
        results_viewer = self.query_one(ResultsViewer)
        status_bar = self.query_one(StatusBar)
        
        # Clear existing
        results_viewer.clear(columns=True)
        
        if not self.filtered_results:
            status_bar.update_status("No results to display")
            return

        # Calculate pagination
        total_rows = len(self.filtered_results)
        start_idx = self.current_page * self.page_size
        end_idx = min(start_idx + self.page_size, total_rows)
        page_results = self.filtered_results[start_idx:end_idx]
        
        columns = list(self.filtered_results[0].keys())

        # Add columns
        for col in columns:
            results_viewer.add_column(col, key=col)

        # Add rows (current page only)
        for row in page_results:
            values = [str(row.get(col, "NULL")) for col in columns]
            results_viewer.add_row(*values)

        # Update status with pagination info
        total_pages = (total_rows + self.page_size - 1) // self.page_size
        page_info = f"Page {self.current_page + 1}/{total_pages}"
        filter_info = f" (filtered from {len(self.last_results)})" if self.filter_text else ""
        
        message = f"Showing {start_idx + 1}-{end_idx} of {total_rows} rows{filter_info} | {page_info}"
        status_bar.update_status(
            message,
            execution_time=execution_time,
            row_count=total_rows
        )
        status_bar.remove_class("error")
        status_bar.add_class("success")


    def _show_error(self, error_message: str) -> None:
        """Show an error message."""
        status_bar = self.query_one(StatusBar)
        status_bar.update_status(f"Error: {error_message}")
        status_bar.remove_class("success")
        status_bar.add_class("error")

    def _show_status(self, message: str, error: bool = False) -> None:
        """Show a status message."""
        status_bar = self.query_one(StatusBar)
        status_bar.update_status(message)
        if error:
            status_bar.remove_class("success")
            status_bar.add_class("error")
        else:
            status_bar.remove_class("error")

    def _load_initial_file(self) -> None:
        """Load the initial file if provided."""
        if not self.initial_file:
            return

        try:
            # Pre-populate query editor
            editor = self.query_one(QueryEditor)
            editor.text = f"SELECT * FROM '{self.initial_file}' LIMIT 10"

            self._show_status(f"Loaded {self.initial_file}. Press Ctrl+Enter to execute.")
        except Exception as e:
            self._show_error(f"Could not load {self.initial_file}: {e}")

    def _load_history(self) -> None:
        """Load query history from file."""
        history_path = Path(self.history_file)
        if history_path.exists():
            try:
                self.query_history = history_path.read_text().splitlines()
            except Exception:
                pass  # Silently ignore history loading errors

    def _save_history(self) -> None:
        """Save query history to file."""
        try:
            history_path = Path(self.history_file)
            history_path.parent.mkdir(parents=True, exist_ok=True)
            # Keep last 100 queries
            history_to_save = self.query_history[-100:]
            history_path.write_text("\n".join(history_to_save))
        except Exception:
            pass  # Silently ignore history saving errors

    @work(thread=True)
    def _update_schema_browser(self) -> None:
        """Update the schema browser with loaded files."""
        schemas = {}
        for file in self.loaded_files:
            try:
                # Use query() to get schema
                q = query(file)
                schemas[file] = q.schema().to_dict()
            except Exception as e:
                schemas[file] = {"Error": str(e)}

        self.call_from_thread(self.query_one(SchemaBrowser).show_schemas, schemas)

    def action_show_help(self) -> None:
        """Show help dialog."""
        self._show_status("Ctrl+Enter=Execute | Ctrl+L=Clear | F2=Schema | Ctrl+X=Export | Ctrl+P/N or [/]=Page | Click headers to sort")

    def action_toggle_schema(self) -> None:
        """Toggle schema browser panel."""
        container = self.query_one("#schema-container")
        if container.has_class("visible"):
            container.remove_class("visible")
            self._show_status("Schema browser hidden")
        else:
            container.add_class("visible")
            self._show_status("Schema browser visible")

    def action_toggle_history(self) -> None:
        """Toggle query history panel."""
        self._show_status("Query history - Coming soon!")

    def action_toggle_explain(self) -> None:
        """Toggle explain mode - shows query execution plan."""
        if not self.last_results:
            self._show_status("Execute a query first to see explain plan", error=True)
            return
        
        # Show placeholder for now
        self._show_status("Explain Mode: Query plan analysis - Coming soon!")

    def action_prev_page(self) -> None:
        """Go to previous page of results."""
        if not self.filtered_results:
            return
        
        if self.current_page > 0:
            self.current_page -= 1
            self._refresh_displayed_results()
        else:
            self._show_status("Already on first page")

    def action_next_page(self) -> None:
        """Go to next page of results."""
        if not self.filtered_results:
            return
        
        total_pages = (len(self.filtered_results) + self.page_size - 1) // self.page_size
        if self.current_page < total_pages - 1:
            self.current_page += 1
            self._refresh_displayed_results()
        else:
            self._show_status("Already on last page")

    def action_filter_results(self) -> None:
        """Toggle filter on current results."""
        if not self.last_results:
            self._show_status("No results to filter", error=True)
            return
        
        # Simple toggle filter for demo
        # In a real app, you'd show an input dialog
        # For now, let's use a simple prompt-style approach
        if not self.filter_text:
            # Enable filter mode - user can type filter
            self._show_status("Filter mode: Type filter text and press Enter (or leave empty to show all)")
            # For MVP, just show a message
            # A full implementation would add an Input widget
        else:
            # Clear filter
            self.filter_text = ""
            self.filtered_results = self._apply_filter(self.last_results)
            self.current_page = 0
            self._refresh_displayed_results()
            self._show_status("Filter cleared")

    def action_export_results(self) -> None:
        """Export current results in multiple formats."""
        if not self.last_results:
            self._show_status("No results to export", error=True)
            return

        # Cycle through formats: CSV -> JSON -> Parquet
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Export format selection (we'll cycle through them)
        # For a full implementation, you'd show a selection dialog
        # For now, let's export to all three formats
        
        formats_exported = []
        
        # Export CSV
        try:
            import csv
            csv_file = f"results_{timestamp}.csv"
            with open(csv_file, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=self.last_results[0].keys())
                writer.writeheader()
                writer.writerows(self.last_results)
            formats_exported.append(("CSV", csv_file))
        except Exception as e:
            self._show_status(f"CSV export failed: {e}", error=True)
            return

        # Export JSON
        try:
            import json
            json_file = f"results_{timestamp}.json"
            with open(json_file, "w") as f:
                json.dump(self.last_results, f, indent=2, default=str)
            formats_exported.append(("JSON", json_file))
        except Exception as e:
            pass  # Optional format

        # Export Parquet (if pyarrow available)
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            
            parquet_file = f"results_{timestamp}.parquet"
            table = pa.Table.from_pylist(self.last_results)
            pq.write_table(table, parquet_file)
            formats_exported.append(("Parquet", parquet_file))
        except Exception:
            pass  # Optional format

        # Show success message
        if formats_exported:
            formats_str = ", ".join([f"{fmt} ({path})" for fmt, path in formats_exported])
            self._show_status(f"Exported to: {formats_str}")


def launch_shell(initial_file: Optional[str] = None, history_file: Optional[str] = None) -> None:
    """
    Launch the interactive SQL shell.

    Args:
        initial_file: Optional file to load on startup
        history_file: Path to query history file
    """
    app = SQLShellApp(initial_file=initial_file, history_file=history_file)
    app.run()


if __name__ == "__main__":
    launch_shell()
