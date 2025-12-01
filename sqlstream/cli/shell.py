"""
SQLStream Interactive Shell

A full-featured interactive SQL shell using Textual TUI framework.
Allows users to write and execute queries, view results, browse schemas,
and export data - all from a beautiful terminal interface.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import json

from textual import work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.widgets import Button, DataTable, DirectoryTree, Footer, Header, Input, Label, Static, TextArea, Tree, OptionList, TabbedContent, TabPane, ContentSwitcher
from textual.geometry import Offset
from textual.events import Key

try:
    from sqlstream.core.query import query, parse, Query
    from sqlstream.core.types import Schema
except ImportError:
    # Fallback for development
    from sqlstream import query


class SQLAutoComplete(OptionList):
    """A popup widget that shows autocomplete suggestions."""
    def __init__(self, suggestions: list[str], **kwargs):
        super().__init__(*suggestions, **kwargs)
        self.add_class("autocomplete-popup")

class QueryEditor(TextArea):
    """Multi-line SQL query editor with syntax highlighting and auto-completion."""

    BINDINGS = [
        Binding("ctrl+enter", "execute_query", "Execute", priority=True),
        Binding("ctrl+e", "execute_query", "Execute (Alt)", priority=True),
        Binding("ctrl+l", "clear_editor", "Clear", priority=True),
        Binding("ctrl+up", "history_prev", "Prev Query", priority=True),
        Binding("ctrl+down", "history_next", "Next Query", priority=True),
        Binding("ctrl+d", "app.quit", "Exit", priority=True),
        # Word deletion shortcuts
        Binding("ctrl+delete", "delete_word_forward", "Delete Word →", show=False),
        Binding("ctrl+backspace", "delete_word_backward", "Delete Word ←", show=False),
    ]

    # SQL Keywords to suggest
    KEYWORDS = [
        "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT",
        "JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "AND", "OR",
        "NOT", "NULL", "IS", "IN", "VALUES", "INSERT", "UPDATE",
        "DELETE", "CREATE", "TABLE", "DROP", "ALTER", "HAVING", "AS"
    ]

    autocomplete_popup: SQLAutoComplete | None = None

    def _get_current_word(self) -> str:
        """Get the word under the cursor."""
        # Get current line text
        line = self.document.get_line(self.cursor_location[0])
        col = self.cursor_location[1]

        # Find start of word
        start = col
        while start > 0 and (line[start-1].isalnum() or line[start-1] == "_"):
            start -= 1

        return line[start:col]

    def _show_suggestions(self, word: str):
        """Show the autocomplete popup if matches found."""
        matches = [k for k in self.KEYWORDS if k.startswith(word.upper())]

        # Remove existing popup if it exists
        if self.autocomplete_popup:
            self.autocomplete_popup.remove()
            self.autocomplete_popup = None

        if not matches or not word:
            return

        # Create and mount the popup
        self.autocomplete_popup = SQLAutoComplete(matches)
        self.screen.mount(self.autocomplete_popup)

        # Position the popup near the cursor
        x, y = self.cursor_screen_offset

        # FIXED LINE: Use x, y directly as they are already screen coordinates
        popup_offset = Offset(x, y + 1)

        self.autocomplete_popup.styles.offset = (popup_offset.x, popup_offset.y)
        self.autocomplete_popup.styles.width = 20
        self.autocomplete_popup.styles.height = min(len(matches) + 2, 10)

    def on_text_area_changed(self) -> None:
        """Called when text changes."""
        word = self._get_current_word()
        self._show_suggestions(word)

    def on_key(self, event: Key) -> None:
        """Handle key presses for selecting suggestions."""
        if self.autocomplete_popup:
            if event.key == "down":
                self.autocomplete_popup.action_cursor_down()
                event.prevent_default()
                return
            elif event.key == "up":
                self.autocomplete_popup.action_cursor_up()
                event.prevent_default()
                return
            elif event.key in ("enter", "tab"):
                # Complete the word
                selected = self.autocomplete_popup.get_option_at_index(self.autocomplete_popup.highlighted).prompt
                self._insert_completion(str(selected))
                self._close_popup()
                event.prevent_default()
                return
            elif event.key == "escape":
                self._close_popup()
                event.prevent_default()
                return

    def _insert_completion(self, completion: str):
        """Replace the current partial word with the completion."""
        current_word = self._get_current_word()
        # Delete the partial word
        self.delete(
            start=(self.cursor_location[0], self.cursor_location[1] - len(current_word)),
            end=self.cursor_location
        )
        # Insert the full keyword
        self.insert(completion)

    def _close_popup(self):
        if self.autocomplete_popup:
            self.autocomplete_popup.remove()
            self.autocomplete_popup = None

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

    def action_delete_word_backward(self) -> None:
        """Delete word to the left of cursor (Ctrl+Backspace)."""
        row, col = self.cursor_location
        
        if col == 0:
            # At start of line, behave like normal backspace (join lines)
            self.action_delete_left()
            return

        line = self.document.get_line(row)
        
        # Scan backwards
        i = col - 1
        
        # 1. Consume whitespace if any
        while i >= 0 and line[i].isspace():
            i -= 1
            
        # 2. Consume word characters OR symbols (but not mixed)
        if i >= 0:
            if line[i].isalnum() or line[i] == '_':
                # Word characters
                while i >= 0 and (line[i].isalnum() or line[i] == '_'):
                    i -= 1
            else:
                # Symbols
                while i >= 0 and not (line[i].isalnum() or line[i] == '_' or line[i].isspace()):
                    i -= 1
                    
        target_col = i + 1
        self.delete(start=(row, target_col), end=(row, col))

    def action_delete_word_forward(self) -> None:
        """Delete word to the right of cursor (Ctrl+Delete)."""
        row, col = self.cursor_location
        line = self.document.get_line(row)
        
        if col >= len(line):
            # At end of line, behave like normal delete (join next line)
            self.action_delete_right()
            return
            
        # Scan forwards
        i = col
        
        # 1. Consume whitespace if any
        while i < len(line) and line[i].isspace():
            i += 1
            
        # 2. Consume word characters OR symbols
        if i < len(line):
            if line[i].isalnum() or line[i] == '_':
                # Word characters
                while i < len(line) and (line[i].isalnum() or line[i] == '_'):
                    i += 1
            else:
                # Symbols
                while i < len(line) and not (line[i].isalnum() or line[i] == '_' or line[i].isspace()):
                    i += 1
                    
        target_col = i
        self.delete(start=(row, col), end=(row, target_col))

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


class FilterDialog(ModalScreen[str]):
    """Modal dialog for entering filter text."""

    def compose(self) -> ComposeResult:
        with Container(id="filter-dialog"):
            yield Label("Filter Results (case-insensitive)")
            yield Input(placeholder="Enter search text...", id="filter-input")
            with Horizontal(id="dialog-buttons"):
                yield Button("Filter", variant="primary", id="filter-btn")
                yield Button("Clear", variant="default", id="clear-btn")
                yield Button("Cancel", variant="default", id="cancel-btn")

    def on_mount(self) -> None:
        self.query_one("#filter-input", Input).focus()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "filter-btn":
            filter_text = self.query_one("#filter-input", Input).value
            self.dismiss(filter_text)
        elif event.button.id == "clear-btn":
            self.dismiss("")
        else:
            self.dismiss(None)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.dismiss(event.value)


class ExplainDialog(ModalScreen):
    """Modal dialog for showing query execution plan."""

    def __init__(self, plan: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.plan = plan

    def compose(self) -> ComposeResult:
        with Container(id="explain-dialog"):
            yield Label("Query Execution Plan", id="explain-title")
            with VerticalScroll(id="explain-content"):
                yield Static(self.plan, id="explain-text")
            yield Button("Close", variant="primary", id="close-btn")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss()


class SaveFileDialog(ModalScreen[str]):
    """Modal dialog for saving files."""

    def __init__(self, default_name: str = "", **kwargs) -> None:
        super().__init__(**kwargs)
        self.default_name = default_name

    def compose(self) -> ComposeResult:
        with Container(id="save-dialog"):
            yield Label("Save Results")
            yield Input(value=self.default_name, placeholder="Enter filename...", id="filename-input")
            yield Label("(Formats: .csv, .json, .parquet)", id="format-hint")
            with Horizontal(id="dialog-buttons"):
                yield Button("Save", variant="primary", id="save-btn")
                yield Button("Cancel", variant="default", id="cancel-btn")

    def on_mount(self) -> None:
        input_widget = self.query_one("#filename-input", Input)
        input_widget.focus()
        # Select the filename part (before extension)
        if "." in self.default_name:
            input_widget.cursor_position = self.default_name.rindex(".")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "save-btn":
            filename = self.query_one("#filename-input", Input).value
            if filename:
                self.dismiss(filename)
        else:
            self.dismiss(None)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.value:
            self.dismiss(event.value)


class FileBrowser(DirectoryTree):
    """Side panel for browsing files."""

    def __init__(self, path: str, **kwargs) -> None:
        super().__init__(path, **kwargs)
        self.border_title = "Files"



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

    #sidebar-container {
        width: 25%;
        height: 100%;
        border: solid $primary;
        background: $panel;
        display: none;
    }

    #sidebar-container.visible {
        display: block;
    }

    #right-panel {
        width: 1fr;  /* Take remaining space (was 100%, causing overflow) */
        height: 100%;
        layout: vertical;
    }

    #query-container {
        height: 12;
        border: solid $primary;
        background: $panel;
    }

    #query-editor {
        height: 100%;
        border: none;
    }

    #results-container {
        height: 1fr;
        border: solid $accent;
        margin-top: 1;
    }

    #results-viewer {
        height: 100%;
    }

    #status-bar {
        height: auto;
        min-height: 3;
        background: $boost;
        color: $text;
        content-align: center middle;
        border: solid $primary;
    }

    .error {
        background: $error;
        color: $text;
    }

    .success {
        background: $success;
        color: $text;
    }

    /* Dialog styles */
    #filter-dialog, #save-dialog, #open-dialog, #explain-dialog {
        align: center middle;
        width: 60;
        height: auto;
        background: $panel;
        border: thick $primary;
        padding: 1 2;
    }

    #filter-dialog Label, #save-dialog Label, #open-dialog Label {
        width: 100%;
        text-align: center;
        margin-bottom: 1;
    }

    #filter-input, #filename-input {
        width: 100%;
        margin-bottom: 1;
    }

    #format-hint {
        color: $text-muted;
        text-align: center;
        margin-bottom: 1;
    }

    #dialog-buttons {
        width: 100%;
        height: auto;
        align: center middle;
    }

    #dialog-buttons Button {
        margin: 0 1;
    }

    #explain-dialog {
        width: 80;
        height: 30;
    }

    #explain-title {
        text-style: bold;
        text-align: center;
        margin-bottom: 1;
    }

    #explain-content {
        height: 1fr;
        border: solid $accent;
        margin-bottom: 1;
    }

    #explain-text {
        padding: 1;
    }

    #file-browser {
        height: 1fr;
        border: none;
    }

    .autocomplete-popup {
        layer: overlay;
        background: $panel;
        border: $accent;
        display: block;
        position: absolute; /* Crucial for floating */
    }
    """

    BINDINGS = [
        Binding("f1", "show_help", "Help"),
        Binding("f2", "toggle_sidebar", "Sidebar"),
        Binding("f3", "toggle_history", "History", show=False),
        Binding("f4", "toggle_explain", "Explain"),
        Binding("ctrl+o", "open_file", "Files"),
        Binding("ctrl+q", "quit", "Exit", priority=True),
        Binding("ctrl+d", "quit", "Exit", priority=True),
        Binding("ctrl+s", "save_state", "Save State"),
        Binding("ctrl+x", "export_results", "Export"),
        Binding("ctrl+f", "filter_results", "Filter"),
        # NOTE: The ctrl+p overrides the default palette binding
        # Binding("ctrl+p", "prev_page", "Prev Page", priority=True),
        # Binding("ctrl+n", "next_page", "Next Page", priority=True),
        Binding("ctrl+t", "new_tab", "New Tab"),
        Binding("ctrl+w", "close_tab", "Close Tab"),
        Binding("[", "prev_page", "◀ Prev", show=True, priority=True),
        Binding("]", "next_page", "Next ▶", show=True, priority=True),
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
        self.last_query = ""  # Store last executed query for explain
        self.query_engine = Query()
        self.loaded_files: List[str] = []

        # Pagination state
        self.page_size = 100
        self.current_page = 0

        # Filter and sort state
        self.filter_text = ""
        self.sort_column = None
        self.sort_reverse = False

        self.state_file = str(Path.home() / ".sqlstream_state")
        self.tab_counter = 0

        if initial_file:
            self.loaded_files.append(initial_file)

    def compose(self) -> ComposeResult:
        """Compose the application layout."""
        yield Header(show_clock=True)

        with Horizontal(id="main-container"):
            # Sidebar (Hidden by default)
            with Container(id="sidebar-container"):
                with TabbedContent(id="sidebar-tabs"):
                    with TabPane("Schema", id="tab-schema"):
                        yield SchemaBrowser(id="schema-browser")
                    with TabPane("Files", id="tab-files"):
                        yield FileBrowser("./", id="file-browser")

            # Main Content
            with Vertical(id="right-panel"):
                # Query Editor Container
                with Container(id="query-container"):
                    with TabbedContent(id="query-tabs"):
                        # Tabs will be loaded dynamically
                        pass

                # Results Viewer Container
                with Container(id="results-container"):
                    yield ResultsViewer(id="results-viewer")

                # Status Bar
                yield StatusBar(id="status-bar")

        yield Footer()

    async def on_mount(self) -> None:
        """Initialize the shell on mount."""
        self.title = "SQLStream Interactive Shell"
        self.sub_title = "Press Ctrl+Enter to execute query, Ctrl+D to exit"

        # Load state (tabs)
        await self._load_state()

        # Show welcome message
        status_bar = self.query_one(StatusBar)
        status_bar.update_status(
            "Welcome to SQLStream! Type your SQL query and press Ctrl+Enter to execute."
        )

        # Focus the active query editor
        self._get_active_editor().focus()

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

    def _get_active_editor(self) -> QueryEditor:
        """Get the currently active query editor."""
        tabs = self.query_one("#query-tabs", TabbedContent)
        if not tabs.active:
            return self.query_one("#query-editor-1", QueryEditor)

        active_pane = self.query_one(f"#{tabs.active}", TabPane)
        return active_pane.query_one(QueryEditor)

    def _set_editor_text(self, text: str) -> None:
        """Set text in active query editor."""
        editor = self._get_active_editor()
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
            _sources = result._discover_sources()
            self.loaded_files.extend([f for f in _sources.values() if f and f not in self.loaded_files])

            # Update schema browser
            self._update_schema_browser()

            # Get results
            results = result.to_list()
            execution_time = (datetime.now() - start_time).total_seconds()

            # Store results and query
            self.last_results = results
            self.last_query = query_text  # Store for explain mode

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

    def _format_value(self, value: Any) -> str:
        """Format a value for display, handling scientific notation."""
        if value is None:
            return "NULL"
        elif isinstance(value, float):
            # Format floats nicely - avoid scientific notation for small numbers
            if abs(value) < 1e-10 and value != 0:
                return "0.0"
            elif abs(value) < 0.01 or abs(value) > 1e6:
                # Use scientific notation for very small or very large
                return f"{value:.6g}"
            else:
                # Regular decimal notation
                return f"{value:.6f}".rstrip('0').rstrip('.')
        else:
            return str(value)

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
            values = [self._format_value(row.get(col)) for col in columns]
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
            editor = self._get_active_editor()
            editor.text = f"SELECT * FROM '{self.initial_file}' LIMIT 10"

            self._show_status(f"Loaded {self.initial_file}. Press Ctrl+Enter to execute.")
        except Exception as e:
            self._show_error(f"Could not load {self.initial_file}: {e}")

    def _load_history(self) -> None:
        """Load query history from file."""
        history_path = Path(self.history_file)
        if history_path.exists():
            try:
                content = history_path.read_text()
                # Use special delimiter to separate queries (supports multiline)
                if content:
                    self.query_history = content.split("\n===\n")
                    self.query_history = sorted(set(self.query_history), key=self.query_history.index, reverse=True)
                else:
                    self.query_history = []
            except Exception:
                pass  # Silently ignore history loading errors

    def _save_history(self) -> None:
        """Save query history to file."""
        try:
            history_path = Path(self.history_file)
            history_path.parent.mkdir(parents=True, exist_ok=True)
            # Keep last 100 queries
            history_to_save = self.query_history[-100:]
            # Use special delimiter to separate queries (preserves multiline)
            history_path.write_text("\n===\n".join(history_to_save))
        except Exception:
            pass  # Silently ignore history saving errors

    @work(thread=True)
    def _update_schema_browser(self) -> None:
        """Update the schema browser with loaded files."""
        schemas = {}
        for file in self.loaded_files:
            if not file:
                continue
            try:
                # Use query() to get schema
                q = query(file)
                schemas[file] = q.schema().to_dict()
            except Exception as e:
                schemas[file] = {"Error": str(e)}

        self.call_from_thread(self.query_one(SchemaBrowser).show_schemas, schemas)

    def action_show_help(self) -> None:
        """Show help dialog."""
        self._show_status("Tab=Switch| Ctrl+E=Execute | Ctrl+L=Clear | F2=Schema | Ctrl+X=Export | [=Prev Page | ]=Next Page | Click headers to sort")

    def action_toggle_sidebar(self) -> None:
        """Toggle sidebar panel."""
        container = self.query_one("#sidebar-container")
        if container.has_class("visible"):
            container.remove_class("visible")
            self._show_status("Sidebar hidden")
        else:
            container.add_class("visible")
            self._show_status("Sidebar visible")
        
        # Force layout refresh to prevent text overflow
        self.refresh(layout=True)
        # Also refresh the active editor to reflow text
        try:
            editor = self._get_active_editor()
            editor.refresh()
        except Exception:
            pass  # Editor might not be ready yet

    def action_toggle_history(self) -> None:
        """Toggle query history panel."""
        self._show_status("Query history - Coming soon!")

    @work
    async def action_toggle_explain(self) -> None:
        """Toggle explain mode - shows query execution plan."""
        if not self.last_query:
            self._show_status("Execute a query first to see explain plan", error=True)
            return

        # Generate query plan
        try:
            parsed = parse(self.last_query)

            # Build explain plan text
            plan_lines = []
            plan_lines.append("=" * 60)
            plan_lines.append("QUERY EXECUTION PLAN")
            plan_lines.append("=" * 60)
            plan_lines.append("")
            plan_lines.append(f"Query: {self.last_query}")
            plan_lines.append("")
            plan_lines.append("--- PLAN STEPS ---")
            plan_lines.append("")

            step = 1
            # Source scan
            plan_lines.append(f"{step}. TABLE SCAN")
            plan_lines.append(f"   Source: {parsed.source}")
            step += 1

            # JOIN if present
            if parsed.join and parsed.join.right_source:
                plan_lines.append("")
                plan_lines.append(f"{step}. JOIN")
                plan_lines.append(f"   Type: {parsed.join.join_type.upper()}")
                plan_lines.append(f"   Right Source: {parsed.join.right_source}")
                plan_lines.append(f"   Condition: {parsed.join.on_left} = {parsed.join.on_right}")
                step += 1

            # WHERE clause
            if parsed.where:
                plan_lines.append("")
                plan_lines.append(f"{step}. FILTER")
                plan_lines.append(f"   Condition: {parsed.where}")
                step += 1

            # GROUP BY
            if parsed.group_by:
                plan_lines.append("")
                plan_lines.append(f"{step}. GROUP BY")
                plan_lines.append(f"   Columns: {', '.join(parsed.group_by)}")
                step += 1

            # ORDER BY
            if parsed.order_by:
                plan_lines.append("")
                plan_lines.append(f"{step}. SORT")
                order_strs = []
                for col, direction in parsed.order_by:
                    order_strs.append(f"{col} {'DESC' if direction else 'ASC'}")
                plan_lines.append(f"   Order: {', '.join(order_strs)}")
                step += 1

            # LIMIT
            if parsed.limit is not None:
                plan_lines.append("")
                plan_lines.append(f"{step}. LIMIT")
                plan_lines.append(f"   Rows: {parsed.limit}")
                step += 1

            # Projection
            plan_lines.append("")
            plan_lines.append(f"{step}. PROJECTION")
            if parsed.columns:
                plan_lines.append(f"   Columns: {', '.join(parsed.columns)}")
            else:
                plan_lines.append("   Columns: * (all)")

            plan_lines.append("")
            plan_lines.append("=" * 60)
            plan_lines.append(f"Estimated rows returned: {len(self.last_results)}")
            plan_lines.append("=" * 60)

            plan_text = "\n".join(plan_lines)

            # Show explain dialog
            await self.push_screen_wait(ExplainDialog(plan_text))

        except Exception as e:
            self._show_status(f"Could not generate explain plan: {e}", error=True)

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

    @work
    async def action_filter_results(self) -> None:
        """Show filter dialog for current results."""
        if not self.last_results:
            self._show_status("No results to filter", error=True)
            return

        # Show filter dialog
        filter_text = await self.push_screen_wait(FilterDialog())

        if filter_text is None:
            # Cancelled
            return
        elif filter_text == "":
            # Clear filter
            self.filter_text = ""
            self.filtered_results = self.last_results.copy()
            self.current_page = 0
            self._refresh_displayed_results()
            self._show_status("Filter cleared")
        else:
            # Apply filter
            self.filter_text = filter_text
            self.filtered_results = self._apply_filter(self.last_results)
            self.current_page = 0
            self._refresh_displayed_results()
            self._show_status(f"Filtered to {len(self.filtered_results)} rows")

    @work
    async def action_export_results(self) -> None:
        """Export current results with file dialog."""
        if not self.last_results:
            self._show_status("No results to export", error=True)
            return

        # Show save dialog with default filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_name = f"results_{timestamp}.csv"
        filename = await self.push_screen_wait(SaveFileDialog(default_name))

        if filename is None:
            return  # Cancelled

        # Determine format from extension
        filename = filename.strip()
        if not filename:
            self._show_status("No filename provided", error=True)
            return

        # Get file extension
        file_lower = filename.lower()

        try:
            # Export based on extension
            if file_lower.endswith('.csv'):
                import csv
                with open(filename, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=self.last_results[0].keys())
                    writer.writeheader()
                    # Use formatted values for export
                    formatted_rows = []
                    for row in self.last_results:
                        formatted_row = {k: self._format_value(v) for k, v in row.items()}
                        formatted_rows.append(formatted_row)
                    writer.writerows(formatted_rows)
                self._show_status(f"Exported to CSV: {filename}")

            elif file_lower.endswith('.json'):
                import json
                # Format values for JSON export
                formatted_rows = []
                for row in self.last_results:
                    formatted_row = {k: self._format_value(v) for k, v in row.items()}
                    formatted_rows.append(formatted_row)
                with open(filename, "w") as f:
                    json.dump(formatted_rows, f, indent=2)
                self._show_status(f"Exported to JSON: {filename}")

            elif file_lower.endswith('.parquet'):
                try:
                    import pyarrow as pa
                    import pyarrow.parquet as pq

                    table = pa.Table.from_pylist(self.last_results)
                    pq.write_table(table, filename)
                    self._show_status(f"Exported to Parquet: {filename}")
                except ImportError:
                    self._show_status("pyarrow not installed. Install with: pip install pyarrow", error=True)

            else:
                # Default to CSV if no recognized extension
                if not file_lower.endswith(('.csv', '.json', '.parquet')):
                    filename = filename + '.csv'
                import csv
                with open(filename, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=self.last_results[0].keys())
                    writer.writeheader()
                    formatted_rows = []
                    for row in self.last_results:
                        formatted_row = {k: self._format_value(v) for k, v in row.items()}
                        formatted_rows.append(formatted_row)
                    writer.writerows(formatted_rows)
                self._show_status(f"Exported to CSV: {filename}")

        except Exception as e:
            self._show_status(f"Export failed: {e}", error=True)

    def action_open_file(self) -> None:
        """Switch to file browser tab and show sidebar."""
        # Show sidebar if hidden
        container = self.query_one("#sidebar-container")
        if not container.has_class("visible"):
            container.add_class("visible")

        # Switch to Files tab
        self.query_one("#sidebar-tabs", TabbedContent).active = "tab-files"

        # Focus file browser
        self.query_one("#file-browser", FileBrowser).focus()
        self._show_status("Select a file from the sidebar")

    def on_directory_tree_file_selected(self, event: DirectoryTree.FileSelected) -> None:
        """Handle file selection from sidebar."""
        file_path = str(event.path)

        # Get active editor
        editor = self._get_active_editor()
        current_text = editor.text.strip()

        # Build query text with selected file
        if not current_text:
            # Empty editor - create simple SELECT query
            new_text = f"SELECT * FROM '{file_path}'"
        elif "FROM" in current_text.upper():
            # Already has FROM clause - add the current text to history and replace with simple select statement
            # Add to history if new
            if not self.query_history or self.query_history[-1] != current_text:
                self.query_history.append(current_text)
                self._save_history()

            new_text = f"SELECT * FROM '{file_path}'"
            self._show_status("Last query stored in history")
        else:
            # Append FROM clause
            new_text = f"{current_text}\nFROM '{file_path}'"

        # Update editor
        editor.text = new_text
        # Move cursor to end
        lines = new_text.splitlines()
        editor.cursor_location = (len(lines) - 1, len(lines[-1]))

        # Focus editor
        editor.focus()

        self._show_status(f"Added file to query: {file_path}")


    async def action_new_tab(self, content: str = "", title: str = None) -> None:
        """Create a new query tab."""
        self.tab_counter += 1
        if not title:
            title = f"Query {self.tab_counter}"

        tab_id = f"tab-query-{self.tab_counter}"
        editor_id = f"query-editor-{self.tab_counter}"

        pane = TabPane(title, id=tab_id)
        editor = QueryEditor(
            id=editor_id,
            language="sql",
            theme="dracula",
            show_line_numbers=True,
            text=content
        )

        tabs = self.query_one("#query-tabs", TabbedContent)
        await tabs.add_pane(pane)
        await pane.mount(editor)
        tabs.active = tab_id
        editor.focus()

    async def action_close_tab(self) -> None:
        """Close the current query tab."""
        tabs = self.query_one("#query-tabs", TabbedContent)
        active_tab = tabs.active
        if not active_tab:
            return

        await tabs.remove_pane(active_tab)

        # If no tabs left, create a new one
        if not tabs.query(TabPane):
             await self.action_new_tab()

    def action_quit(self) -> None:
        """Save state and exit."""
        self._save_state()
        self.exit()

    def action_save_state(self) -> None:
        """Manual save state action."""
        self._save_state()
        self._show_status("State saved!")

    def _save_state(self) -> None:
        """Save editor state to file."""
        try:
            tabs = self.query_one("#query-tabs", TabbedContent)
            state = []

            # Strategy 1: ContentSwitcher children
            try:
                switcher = tabs.query_one(ContentSwitcher)

                for child in switcher.children:
                    if isinstance(child, TabPane):
                        editors = list(child.query(QueryEditor))

                        if editors:
                            editor = editors[0]
                            state.append({
                                "title": str(child._title),
                                "content": editor.text
                            })
            except Exception:
                pass

            # Strategy 2: Direct query if Strategy 1 found nothing
            if not state:
                for pane in tabs.query(TabPane):
                    editors = list(pane.query(QueryEditor))
                    if editors:
                        state.append({
                            "title": str(pane._title),
                            "content": editors[0].text
                        })

            # Write to file
            path = Path(self.state_file)
            path.write_text(json.dumps(state))
            self.notify(f"Saved {len(state)} tabs", timeout=3)

        except Exception as e:
            self.notify(f"Failed to save state: {e}", severity="error")

    async def _load_state(self) -> None:
        """Load editor state from file."""
        # Load history first
        self._load_history()

        state_path = Path(self.state_file)
        loaded = False

        if state_path.exists():
            try:
                state = json.loads(state_path.read_text())
                if state and isinstance(state, list):
                    for tab_data in state:
                        await self.action_new_tab(
                            content=tab_data.get("content", ""),
                            title=tab_data.get("title")
                        )
                    self.notify(f"Loaded {len(state)} tabs", timeout=3)
                    loaded = True
            except Exception as e:
                self.notify(f"Failed to load state: {e}", severity="error")

        if not loaded:
            # Create default tab
            await self.action_new_tab()

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
