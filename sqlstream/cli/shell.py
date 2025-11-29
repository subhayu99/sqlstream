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
        height: 40%;
        border: solid $primary;
        background: $panel;
    }

    #query-editor {
        height: 100%;
        border: none;
    }

    #results-container {
        height: 55%;
        border: solid $accent;
        margin-top: 1;
    }

    #results-viewer {
        height: 100%;
    }

    #status-bar {
        height: 3;
        background: $boost;
        color: $text;
        padding: 1;
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
        Binding("f4", "toggle_explain", "Explain", show=False),
        Binding("ctrl+d", "quit", "Exit"),
        Binding("ctrl+x", "export_results", "Export", show=False),
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
        self.query_engine = QueryInline()
        self.loaded_files: List[str] = []
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
        """Display query results in the DataTable."""
        results_viewer = self.query_one(ResultsViewer)
        status_bar = self.query_one(StatusBar)

        # Get column names from first row
        if not results:
            return

        columns = list(results[0].keys())

        # Add columns
        for col in columns:
            results_viewer.add_column(col, key=col)

        # Add rows
        for row in results:
            values = [str(row.get(col, "NULL")) for col in columns]
            results_viewer.add_row(*values)

        # Update status
        status_bar.update_status("Query executed successfully", execution_time=execution_time, row_count=len(results))
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
        self._show_status("Help: Ctrl+Enter=Execute | Ctrl+L=Clear | Ctrl+D=Exit | F2=Schema | F3=History | Ctrl+X=Export")

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
        """Toggle explain mode."""
        self._show_status("Explain mode - Coming soon!")

    def action_export_results(self) -> None:
        """Export current results."""
        if not self.last_results:
            self._show_status("No results to export", error=True)
            return

        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"results_{timestamp}.csv"

        try:
            import csv
            with open(filename, "w", newline="") as f:
                if self.last_results:
                    writer = csv.DictWriter(f, fieldnames=self.last_results[0].keys())
                    writer.writeheader()
                    writer.writerows(self.last_results)

            self._show_status(f"Results exported to {filename}")
        except Exception as e:
            self._show_status(f"Export failed: {e}", error=True)


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
