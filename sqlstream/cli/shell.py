"""
SQLStream Interactive Shell

A full-featured interactive SQL shell using Textual TUI framework.
Allows users to write and execute queries, view results, browse schemas,
and export data - all from a beautiful terminal interface.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from textual import work
from textual._text_area_theme import _BUILTIN_THEMES as _TEXT_AREA_BUILTIN_THEMES
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical, VerticalScroll
from textual.events import Key
from textual.geometry import Offset
from textual.screen import ModalScreen
from textual.theme import BUILTIN_THEMES as _BUILTIN_THEMES
from textual.widgets import (
    Button,
    ContentSwitcher,
    DataTable,
    DirectoryTree,
    Footer,
    Header,
    Input,
    Label,
    OptionList,
    Select,
    Static,
    Switch,
    TabbedContent,
    TabPane,
    TextArea,
    Tree,
)
from textual.widgets.text_area import Selection

try:
    from sqlstream.core.query import Query, parse, query
    from sqlstream.core.types import Schema
except ImportError:
    # Fallback for development
    from sqlstream import query


APP_THEMES = [(' '.join(y.title() for y in x.split('-')), x) for x in _BUILTIN_THEMES.keys()]
TEXT_AREA_THEMES = [(' '.join(y.title() for y in x.split('-')), x) for x in _TEXT_AREA_BUILTIN_THEMES.keys()]


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
        Binding("ctrl+d", "add_selection_to_next_find", "Select Next", show=False),
        # Word deletion shortcuts
        Binding("ctrl+delete", "delete_word_forward", "Delete Word →", show=False),
        Binding("ctrl+backspace", "delete_word_backward", "Delete Word ←", show=False),
        # VSCode-like keybindings
        Binding("ctrl+slash", "toggle_comment", "Comment", show=False),
        Binding("ctrl+shift+k", "delete_line", "Delete Line", show=False),
        Binding("ctrl+right_square_bracket", "indent_line", "Indent", show=False),
        Binding("ctrl+left_square_bracket", "outdent_line", "Outdent", show=False),
        Binding("ctrl+shift+d", "duplicate_line", "Duplicate", show=False),
        Binding("ctrl+a", "select_all", "Select All", show=False),
        # Selection with Shift+Navigation
        Binding("shift+home", "select_to_line_start", "Select to Line Start", show=False, priority=True),
        Binding("shift+end", "select_to_line_end", "Select to Line End", show=False, priority=True),
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

    def _get_schema_suggestions(self) -> list[str]:
        """Get column names and table names from the app's schema."""
        suggestions = []
        try:
            # Access parent app's schema
            if hasattr(self.app, 'schemas') and self.app.schemas:
                for source_name, schema in self.app.schemas.items():
                    # Add table/source name
                    suggestions.append(source_name)
                    # Add column names
                    for col in schema.columns:
                        suggestions.append(col.name)
        except Exception:
            pass  # Silently fail if schema not available
        return suggestions

    def _show_suggestions(self, word: str):
        """Show the autocomplete popup if matches found."""
        # Combine keywords with schema suggestions
        all_suggestions = self.KEYWORDS + self._get_schema_suggestions()
        matches = [s for s in all_suggestions if s.upper().startswith(word.upper())]

        # Remove duplicates while preserving order
        seen = set()
        unique_matches = []
        for m in matches:
            if m.upper() not in seen:
                seen.add(m.upper())
                unique_matches.append(m)

        # Remove existing popup if it exists
        if self.autocomplete_popup:
            self.autocomplete_popup.remove()
            self.autocomplete_popup = None

        if not unique_matches or not word:
            return

        # Create and mount the popup
        self.autocomplete_popup = SQLAutoComplete(unique_matches[:10])  # Limit to 10 suggestions
        self.screen.mount(self.autocomplete_popup)

        # Position the popup near the cursor
        x, y = self.cursor_screen_offset

        # FIXED LINE: Use x, y directly as they are already screen coordinates
        popup_offset = Offset(x, y + 1)

        self.autocomplete_popup.styles.offset = (popup_offset.x, popup_offset.y)
        self.autocomplete_popup.styles.width = 25  # Increased width for column names
        self.autocomplete_popup.styles.height = min(len(unique_matches) + 2, 10)

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

    def action_toggle_comment(self) -> None:
        """Toggle SQL comment on current line(s) or selection."""
        # Check if there's a selection
        if self.selection.end != self.selection.start:
            # Get selection range
            start_row = min(self.selection.start[0], self.selection.end[0])
            end_row = max(self.selection.start[0], self.selection.end[0])

            # Toggle comment for each line in selection
            for row in range(start_row, end_row + 1):
                line = self.document.get_line(row)
                stripped = line.lstrip()

                if stripped.startswith("--"):
                    # Uncomment
                    new_line = line.replace("-- ", "", 1)
                    if new_line == line:
                        new_line = line.replace("--", "", 1)
                else:
                    # Comment
                    indent = len(line) - len(stripped)
                    new_line = line[:indent] + "-- " + stripped

                self.delete((row, 0), (row, len(line)))
                self.insert(new_line, (row, 0))
        else:
            # No selection, toggle current line only
            cursor_row, cursor_col = self.cursor_location
            line = self.document.get_line(cursor_row)

            stripped = line.lstrip()
            if stripped.startswith("--"):
                # Uncomment
                new_line = line.replace("-- ", "", 1)
                if new_line == line:
                    new_line = line.replace("--", "", 1)
                self.delete((cursor_row, 0), (cursor_row, len(line)))
                self.insert(new_line, (cursor_row, 0))
            else:
                # Comment
                indent = len(line) - len(stripped)
                new_line = line[:indent] + "-- " + stripped
                self.delete((cursor_row, 0), (cursor_row, len(line)))
                self.insert(new_line, (cursor_row, 0))

    def action_delete_line(self) -> None:
        """Delete the current line."""
        cursor_row, _ = self.cursor_location
        line = self.document.get_line(cursor_row)
        # Delete the line content
        self.delete((cursor_row, 0), (cursor_row, len(line)))
        # Also delete the newline if not last line
        if cursor_row < self.document.line_count - 1:
            self.delete((cursor_row, 0), (cursor_row + 1, 0))

    def action_indent_line(self) -> None:
        """Indent current line(s) or selection."""
        if self.selection.end != self.selection.start:
            # Get selection range
            start_row = min(self.selection.start[0], self.selection.end[0])
            end_row = max(self.selection.start[0], self.selection.end[0])

            # Indent each line in selection
            for row in range(start_row, end_row + 1):
                line = self.document.get_line(row)
                new_line = "    " + line  # 4 spaces
                self.delete((row, 0), (row, len(line)))
                self.insert(new_line, (row, 0))
        else:
            # Single line indent
            cursor_row, cursor_col = self.cursor_location
            line = self.document.get_line(cursor_row)
            new_line = "    " + line  # 4 spaces
            self.delete((cursor_row, 0), (cursor_row, len(line)))
            self.insert(new_line, (cursor_row, 0))
            # Move cursor accordingly
            self.cursor_location = (cursor_row, cursor_col + 4)

    def action_outdent_line(self) -> None:
        """Outdent current line(s) or selection."""
        if self.selection.end != self.selection.start:
            # Get selection range
            start_row = min(self.selection.start[0], self.selection.end[0])
            end_row = max(self.selection.start[0], self.selection.end[0])

            # Outdent each line in selection
            for row in range(start_row, end_row + 1):
                line = self.document.get_line(row)

                # Remove up to 4 leading spaces
                spaces_to_remove = 0
                for char in line[:4]:
                    if char == ' ':
                        spaces_to_remove += 1
                    else:
                        break

                if spaces_to_remove > 0:
                    new_line = line[spaces_to_remove:]
                    self.delete((row, 0), (row, len(line)))
                    self.insert(new_line, (row, 0))
        else:
            # Single line outdent
            cursor_row, cursor_col = self.cursor_location
            line = self.document.get_line(cursor_row)

            # Remove up to 4 leading spaces
            spaces_to_remove = 0
            for char in line[:4]:
                if char == ' ':
                    spaces_to_remove += 1
                else:
                    break

            if spaces_to_remove > 0:
                new_line = line[spaces_to_remove:]
                self.delete((cursor_row, 0), (cursor_row, len(line)))
                self.insert(new_line, (cursor_row, 0))
                # Move cursor accordingly
                new_col = max(0, cursor_col - spaces_to_remove)
                self.cursor_location = (cursor_row, new_col)

    def action_duplicate_line(self) -> None:
        """Duplicate the current line."""
        cursor_row, cursor_col = self.cursor_location
        line = self.document.get_line(cursor_row)
        # Insert a newline and the duplicated line
        self.insert("\n" + line, (cursor_row, len(line)))
        # Move cursor to the duplicated line
        self.cursor_location = (cursor_row + 1, cursor_col)

    def action_select_all(self) -> None:
        """Select all text in the editor."""
        self.select_all()

    def action_add_selection_to_next_find(self) -> None:
        """Add selection to next occurrence (VSCode Ctrl+D behavior)."""
        # Get current selection or word under cursor
        if self.selection.end != self.selection.start:
            # Use existing selection
            selected_text = self.get_text_range(self.selection.start, self.selection.end)
        else:
            # Select word under cursor
            row, col = self.cursor_location
            line = self.document.get_line(row)

            # Find word boundaries
            start = col
            while start > 0 and (line[start-1].isalnum() or line[start-1] == '_'):
                start -= 1

            end = col
            while end < len(line) and (line[end].isalnum() or line[end] == '_'):
                end += 1

            if start < end:
                selected_text = line[start:end]
                self.selection = Selection((row, start), (row, end))
            else:
                return  # No word to select

        # Find next occurrence after current selection end
        current_end_row, current_end_col = self.selection.end

        # Search line by line starting from current position
        found = False
        search_row = current_end_row
        search_col = current_end_col

        while search_row < self.document.line_count:
            line = self.document.get_line(search_row)
            # For first line, search from current column
            start_col = search_col if search_row == current_end_row else 0
            search_line = line[start_col:]

            pos = search_line.find(selected_text)
            if pos != -1:
                # Found it! Set selection directly
                actual_col = start_col + pos
                self.selection = Selection(
                    (search_row, actual_col),
                    (search_row, actual_col + len(selected_text))
                )
                found = True
                break

            search_row += 1

        if not found:
            # Wrap around to beginning
            for row in range(0, current_end_row + 1):
                line = self.document.get_line(row)
                # Don't search past current position on the current line
                search_line = line if row < current_end_row else line[:current_end_col]
                pos = search_line.find(selected_text)
                if pos != -1:
                    # Set selection directly
                    self.selection = Selection(
                        (row, pos),
                        (row, pos + len(selected_text))
                    )
                    break

    def action_select_to_line_start(self) -> None:
        """Select from cursor to start of line (Shift+Home)."""
        row, col = self.cursor_location
        # Select from line start to cursor
        self.selection = Selection((row, 0), (row, col))

    def action_select_to_line_end(self) -> None:
        """Select from cursor to end of line (Shift+End)."""
        row, col = self.cursor_location
        line = self.document.get_line(row)
        # Select from cursor to line end
        self.selection = Selection((row, col), (row, len(line)))

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
        self,
        message: str,
        execution_time: Optional[float] = None,
        row_count: Optional[int] = None,
        filter_info: str = "",
        backend_info: str = ""
    ) -> None:
        """Update status bar with execution info."""
        if execution_time is not None:
            self.last_execution_time = execution_time
        if row_count is not None:
            self.row_count = row_count

        status_parts = [message]
        if filter_info:
             status_parts.append(f"🔍 {filter_info}")
        if backend_info:
            status_parts.append(f"⚙️ {backend_info}")
        if self.row_count is not None:
            status_parts.append(f"{self.row_count} rows")
        if self.last_execution_time is not None:
            status_parts.append(f"{self.last_execution_time:.3f}s")

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


class FilterSidebar(Container):
    """Right sidebar for context-aware filtering."""

    def compose(self) -> ComposeResult:
        yield Label("Filter Data", classes="filter-label")

        # 1. Column Selector
        yield Label("Column:")
        yield Select([], prompt="Select Column", id="fs-column")

        # 2. Operator Selector (Populated dynamically)
        yield Label("Operator:")
        yield Select([], prompt="Select Operator", id="fs-operator", disabled=True)

        # 3. Value Inputs (Swapped dynamically)
        yield Label("Value:")
        with Container(id="fs-input-container"):
            yield Input(placeholder="Select a column first...", id="fs-value-1", disabled=True)
            # Secondary input for "Between" operations (hidden by default)
            yield Input(placeholder="And...", id="fs-value-2", classes="hidden")

        # 4. Actions
        with Horizontal(id="filter-actions"):
            yield Button("Clear", variant="error", id="fs-clear")
            yield Button("Apply", variant="primary", id="fs-apply")

    def update_columns(self, columns: List[str]) -> None:
        """Called by App when results change."""
        select = self.query_one("#fs-column", Select)
        # Preserve "Global Search" as first option
        options = [("Global Search (All)", "global")] + [(c, c) for c in columns]
        select.set_options(options)
        select.value = "global"

    def on_select_changed(self, event: Select.Changed) -> None:
        """Handle column or operator changes."""
        if event.select.id == "fs-column":
            self._on_column_changed(event.value)
        elif event.select.id == "fs-operator":
            self._on_operator_changed(event.value)

    def _on_column_changed(self, column: str) -> None:
        """Update operators based on column type."""
        op_select = self.query_one("#fs-operator", Select)
        val_input = self.query_one("#fs-value-1", Input)
        val_input_2 = self.query_one("#fs-value-2", Input)

        # Reset inputs
        val_input.value = ""
        val_input_2.value = ""
        val_input_2.add_class("hidden")
        val_input.disabled = False
        op_select.disabled = False

        if column == "global":
            op_select.set_options([("Contains", "contains")])
            op_select.value = "contains"
            op_select.disabled = True
            return

        # Infer Data Type from App's last results
        # We peek at the first row of data
        col_type = str
        if self.app.last_results:
            first_val = self.app.last_results[0].get(column)
            if isinstance(first_val, (int, float)):
                col_type = float
            elif isinstance(first_val, bool):
                col_type = bool
            # Simple date detection could go here if data is Python datetime objects

        # Populate Operators based on Type
        if col_type == float: # Numbers
            ops = [
                ("Equals (=)", "eq"),
                ("Greater Than (>)", "gt"),
                ("Less Than (<)", "lt"),
                ("Between (Range)", "between")
            ]
            op_select.set_options(ops)
            op_select.value = "eq"

        elif col_type == bool: # Booleans
            ops = [("Is", "is")]
            op_select.set_options(ops)
            op_select.value = "is"

        else: # Strings (Default)
            ops = [
                ("Contains", "contains"),
                ("Equals", "eq"),
                ("Starts With", "startswith"),
                ("Ends With", "endswith"),
                ("Regex", "regex")
            ]
            op_select.set_options(ops)
            op_select.value = "contains"

    def _on_operator_changed(self, operator: str) -> None:
        """Show/Hide secondary input for 'between' operator."""
        val_2 = self.query_one("#fs-value-2", Input)
        if operator == "between":
            val_2.remove_class("hidden")
        else:
            val_2.add_class("hidden")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "fs-apply":
            self._trigger_filter()
        elif event.button.id == "fs-clear":
            self.app.action_clear_filter()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self._trigger_filter()

    def _trigger_filter(self) -> None:
        """Gather values and tell App to filter."""
        col = self.query_one("#fs-column", Select).value
        op = self.query_one("#fs-operator", Select).value
        val1 = self.query_one("#fs-value-1", Input).value
        val2 = self.query_one("#fs-value-2", Input).value

        self.app.apply_advanced_filter(col, op, val1, val2)


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


class HelpDialog(ModalScreen):
    """Modal dialog for showing keyboard shortcuts and help."""

    def compose(self) -> ComposeResult:
        help_text = """[bold cyan]SQLStream Interactive Shell - Keyboard Shortcuts[/bold cyan]

[yellow]Query Editing:[white]
[bold]  Ctrl+Enter / Ctrl+E    [not bold]Execute current query
[bold]  Ctrl+L                 [not bold]Clear editor
[bold]  Ctrl+Up / Ctrl+Down    [not bold]Navigate query history
[bold]  Ctrl+A                 [not bold]Select all text
[bold]  Ctrl+/                 [not bold]Toggle comment (line or selection)

[yellow]Multi-Cursor & Selection:[white]
[bold]  Ctrl+D                 [not bold]Select next occurrence
[bold]  Shift+Home             [not bold]Select to line start
[bold]  Shift+End              [not bold]Select to line end

[yellow]Code Formatting:[white]
[bold]  Ctrl+]                 [not bold]Indent line(s)
[bold]  Ctrl+[                 [not bold]Outdent line(s)
[bold]  Ctrl+Shift+D           [not bold]Duplicate line
[bold]  Ctrl+Shift+K           [not bold]Delete line

[yellow]Word Operations:[white]
[bold]  Ctrl+Backspace         [not bold]Delete word backward
[bold]  Ctrl+Delete            [not bold]Delete word forward

[yellow]View & Layout:[white]
[bold]  F6                     [not bold]Cycle layout (Split/Editor/Results)
[bold]  Alt+Up / Alt+Down      [not bold]Resize editor height
[bold]  F2                     [not bold]Toggle sidebar (Schema/Files/Config)
[bold]  Ctrl+F                 [not bold]Toggle filter sidebar
[bold]  Ctrl+X                 [not bold]Toggle export sidebar

[yellow]Data Operations:[white]
[bold]  Ctrl+B / F5            [not bold]Cycle backend (Auto/DuckDB/Pandas/Python)
[bold]  [ / ]                  [not bold]Previous/Next page
[bold]  F4                     [not bold]Show query execution plan

[yellow]File Operations:[white]
[bold]  Ctrl+O                 [not bold]Open file browser
[bold]  Ctrl+T                 [not bold]New query tab
[bold]  Ctrl+W                 [not bold]Close current tab

[yellow]Application:[white]
[bold]  Ctrl+S                 [not bold]Save state
[bold]  Ctrl+Q                 [not bold]Quit
[bold]  F1                     [not bold]Show this help

[dim]Tip: Click column headers to sort results[/dim]"""

        with Container(id="explain-dialog"):  # Reuse explain dialog styles
            yield Label("Keyboard Shortcuts & Help", id="explain-title")
            with VerticalScroll(id="explain-content"):
                yield Static(help_text, id="explain-text")
            yield Button("Close", variant="primary", id="close-btn")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss()


class FileBrowser(DirectoryTree):
    """Side panel for browsing files."""

    def __init__(self, path: str, **kwargs) -> None:
        super().__init__(path, **kwargs)
        self.border_title = "Files"


class OverwriteConfirmDialog(ModalScreen[bool]):
    """Modal to confirm file overwrite."""

    def __init__(self, filename: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.filename = filename

    def compose(self) -> ComposeResult:
        with Container(classes="confirm-dialog"):
            yield Label("⚠️ File Already Exists", classes="confirm-header")

            with Container(classes="confirm-body"):
                yield Label("The file below already exists:")
                yield Label(self.filename, classes="confirm-filename")
                yield Label("\nDo you want to overwrite it?")

            with Horizontal(classes="confirm-footer"):
                yield Button("Cancel", variant="default", id="no-btn")
                yield Button("Overwrite", variant="error", id="yes-btn")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "yes-btn":
            self.dismiss(True)
        else:
            self.dismiss(False)


class ExportSidebar(Container):
    """Right sidebar for exporting data."""

    def compose(self) -> ComposeResult:
        with Vertical(classes="sidebar-pane"):
            yield Label("Export Data", classes="filter-label")
            yield Label("Ready to export 0 rows", id="export-info")

            # 1. Format Selection
            yield Label("Format:")
            format_options = [
                ("CSV", "csv"),
                ("JSON", "json"),
                ("Parquet", "parquet")
            ]
            yield Select(format_options, allow_blank=False, value="csv", id="ex-format")

            # 2. Directory Browser
            yield Label("Save Location:")
            yield DirectoryTree("./", id="export-tree")

            # 3. Filename Input
            yield Label("Filename:")
            yield Input(placeholder="filename.csv", id="ex-filename")

            # 4. Actions
            with Horizontal(id="export-actions"):
                yield Button("Export", variant="primary", id="ex-btn")

    def on_mount(self) -> None:
        # Generate default filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Use configured default format
        fmt = self.app.default_export_fmt
        self.query_one("#ex-format", Select).value = fmt
        self.query_one("#ex-filename").value = f"results_{timestamp}.{fmt}"

    def update_info(self, row_count: int) -> None:
        """Update the row count label."""
        self.query_one("#export-info").update(f"Ready to export {row_count:,} rows")

    def on_directory_tree_file_selected(self, event: DirectoryTree.FileSelected) -> None:
        """When user clicks a file, adopt its name but keep our format extension."""
        selected_path = Path(event.path)

        # Get current selected format
        fmt = self.query_one("#ex-format", Select).value

        # Get the stem (filename without extension)
        stem = selected_path.stem

        # Set the input value
        self.query_one("#ex-filename").value = f"{stem}.{fmt}"

    def on_select_changed(self, event: Select.Changed) -> None:
        """Update extension if format changes."""
        if event.select.id == "ex-format":
            inp = self.query_one("#ex-filename", Input)
            current = inp.value
            if current and "." in current:
                stem = current.rsplit(".", 1)[0]
                inp.value = f"{stem}.{event.value}"

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "ex-btn":
            self._initiate_export()

    def _initiate_export(self) -> None:
        """Validate and trigger export process."""
        filename = self.query_one("#ex-filename", Input).value
        fmt = self.query_one("#ex-format", Select).value

        if not filename:
            self.app.notify("Please enter a filename", severity="error")
            return

        # Determine directory:
        # If a node is selected in tree, use its parent (if file) or itself (if dir)
        # Fallback to current working directory of the tree
        tree = self.query_one("#export-tree", DirectoryTree)
        cursor_node = tree.cursor_node

        target_dir = Path(tree.path) # Default to root of tree

        if cursor_node and cursor_node.data:
            node_path = cursor_node.data.path
            if node_path.is_dir():
                target_dir = node_path
            else:
                target_dir = node_path.parent

        full_path = target_dir / filename

        # Check overwrite
        if full_path.exists():
            # PASS filename.name HERE
            self.app.push_screen(
                OverwriteConfirmDialog(filename),
                lambda should_overwrite: self._finish_export(full_path, fmt) if should_overwrite else None
            )
        else:
            self._finish_export(full_path, fmt)

    def _finish_export(self, path: Path, fmt: str) -> None:
        """Call the main app to perform the write."""
        self.app.perform_export(path, fmt)


class ConfigSidebar(Container):
    """Sidebar tab for application configuration."""

    def compose(self) -> ComposeResult:
        with VerticalScroll(id="config-form"):
            yield Label("System Configuration", classes="filter-label")

            # --- SECTION 1: BEHAVIOR (Functional) ---
            with Vertical(classes="config-group"):
                yield Label("Behavior", classes="config-label")

                # Confirm Exit
                with Horizontal(classes="switch-row"):
                    yield Label("Confirm on Exit:")
                    yield Switch(value=False, id="cfg-confirm-exit")

                # History Limit
                yield Label("History Size (Queries):", classes="config-sublabel")
                yield Input(value="100", type="integer", id="cfg-history-size")

                # Default Export Format
                yield Label("Default Export Format:", classes="config-sublabel")
                yield Select(
                    [("CSV", "csv"), ("JSON", "json"), ("Parquet", "parquet")],
                    value="csv",
                    id="cfg-export-fmt",
                    allow_blank=False
                )

            # --- SECTION 2: EXECUTION ---
            with Vertical(classes="config-group"):
                yield Label("Execution", classes="config-label")

                yield Label("Default Backend:", classes="config-sublabel")
                yield Select(
                    [("Auto", "auto"), ("DuckDB", "duckdb"), ("Pandas", "pandas")],
                    value="auto",
                    id="cfg-backend",
                    allow_blank=False
                )

                yield Label("Page Size (Rows):", classes="config-sublabel")
                yield Input(value="100", type="integer", id="cfg-pagesize")

            # --- SECTION 3: APPEARANCE ---
            with Vertical(classes="config-group"):
                yield Label("Appearance", classes="config-label")

                yield Label("UI Theme:", classes="config-sublabel")
                yield Select(APP_THEMES, id="cfg-app-theme", allow_blank=False)

                yield Label("SQL Syntax Theme:", classes="config-sublabel")
                yield Select(TEXT_AREA_THEMES, id="cfg-editor-theme", allow_blank=False)

            # --- SECTION 4: DISPLAY SETTINGS ---
            with Vertical(classes="config-group"):
                yield Label("Display Options", classes="config-label")

                with Horizontal(classes="switch-row"):
                    yield Label("Line Numbers:")
                    yield Switch(value=True, id="cfg-linenums")

                with Horizontal(classes="switch-row"):
                    yield Label("Soft Wrap:")
                    yield Switch(value=False, id="cfg-softwrap")

                with Horizontal(classes="switch-row"):
                    yield Label("Zebra Stripes:")
                    yield Switch(value=True, id="cfg-zebra")

                with Horizontal(classes="switch-row"):
                    yield Label("Compact Results:")
                    yield Switch(value=False, id="cfg-compact")

            yield Button("Save & Apply", variant="primary", id="config-save-btn")

    def on_mount(self) -> None:
        """Load current values from App."""
        app = self.app

        # Behavior
        self.query_one("#cfg-confirm-exit", Switch).value = app.confirm_exit
        self.query_one("#cfg-history-size", Input).value = str(app.max_history)
        self.query_one("#cfg-export-fmt", Select).value = app.default_export_fmt

        # Execution
        self.query_one("#cfg-backend", Select).value = app.backend
        self.query_one("#cfg-pagesize", Input).value = str(app.page_size)

        # Appearance
        self.query_one("#cfg-app-theme", Select).value = app.theme
        self.query_one("#cfg-editor-theme", Select).value = app.editor_theme

        # Display
        self.query_one("#cfg-linenums", Switch).value = app.editor_linenums
        self.query_one("#cfg-softwrap", Switch).value = app.editor_soft_wrap
        self.query_one("#cfg-zebra", Switch).value = app.results_zebra
        self.query_one("#cfg-compact", Switch).value = app.results_compact

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "config-save-btn":
            self._save_settings()

    def _save_settings(self) -> None:
        """Apply settings to App and save to disk."""
        app = self.app

        # 1. Gather Values
        confirm_exit = self.query_one("#cfg-confirm-exit", Switch).value
        try:
            history_size = int(self.query_one("#cfg-history-size", Input).value)
        except ValueError:
            history_size = 100
        export_fmt = self.query_one("#cfg-export-fmt", Select).value

        backend = self.query_one("#cfg-backend", Select).value
        try:
            page_size = int(self.query_one("#cfg-pagesize", Input).value)
        except ValueError:
            page_size = 100

        app_theme = self.query_one("#cfg-app-theme", Select).value
        editor_theme = self.query_one("#cfg-editor-theme", Select).value

        linenums = self.query_one("#cfg-linenums", Switch).value
        softwrap = self.query_one("#cfg-softwrap", Switch).value
        zebra = self.query_one("#cfg-zebra", Switch).value
        compact = self.query_one("#cfg-compact", Switch).value

        # 2. Apply to App State
        app.confirm_exit = confirm_exit
        app.max_history = history_size
        app.default_export_fmt = export_fmt

        app.backend = backend
        app.page_size = page_size
        app.theme = app_theme

        app.editor_theme = editor_theme
        app.editor_linenums = linenums
        app.editor_soft_wrap = softwrap

        app.results_zebra = zebra
        app.results_compact = compact

        # 3. Apply to Active Widgets
        for editor in app.query(QueryEditor):
            editor.theme = editor_theme
            editor.show_line_numbers = linenums
            editor.soft_wrap = softwrap

        results = app.query_one(ResultsViewer)
        results.zebra_stripes = zebra
        if compact:
            results.add_class("compact")
        else:
            results.remove_class("compact")

        if app.last_results:
            app._refresh_displayed_results()

        # 4. Persist
        app.save_config_file()
        app.notify("Configuration Saved & Applied!")


class ConfirmExitDialog(ModalScreen[bool]):
    """Modal to confirm application exit."""

    def compose(self) -> ComposeResult:
        with Container(classes="confirm-dialog"):
            yield Label("Confirm Exit", classes="confirm-header")
            with Container(classes="confirm-body"):
                yield Label("Are you sure you want to quit?")
            with Horizontal(classes="confirm-footer"):
                yield Button("Cancel", variant="default", id="cancel-btn")
                yield Button("Exit", variant="error", id="exit-btn")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "exit-btn":
            self.dismiss(True)
        else:
            self.dismiss(False)


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

    /* --- Main Layout --- */
    #main-container {
        width: 100%;
        height: 100%;
    }

    /* Left Sidebar (Files/Schema) */
    #sidebar-container {
        width: 20%;
        height: 100%;
        border-right: solid $primary;
        background: $panel;
        display: none;
    }

    #sidebar-container.visible {
        display: block;
    }

    /* Center Panel */
    #center-panel {
        width: 1fr;
        height: 100%;
        layout: vertical;
    }

    /* Right Sidebar (Tools) */
    #tools-sidebar {
        width: 20%;
        height: 100%;
        border-left: solid $primary;
        background: $surface;
        display: none;
    }

    #tools-sidebar.visible {
        display: block;
    }

    /* --- Sidebar Internals --- */
    #tools-tabs {
        height: 100%;
    }

    .sidebar-pane {
        padding: 1;
        height: 100%;
    }

    /* Filter Styles */
    .filter-label {
        color: $accent;
        text-style: bold;
        margin-bottom: 1;
        margin-top: 1;
    }

    #filter-actions, #export-actions {
        height: auto;
        margin-top: 2;
        align: center middle;
    }

    #filter-actions Button, #export-actions Button {
        width: 1fr;
        margin: 0 1;
    }

    /* Export Styles */
    #export-tree {
        height: 1fr; /* Takes available space */
        border: solid $primary;
        margin-bottom: 1;
        background: $panel;
    }

    #export-info {
        text-align: center;
        color: $accent;
        margin-bottom: 1;
    }

    .confirm-dialog {
        width: 50;
        height: auto;
        background: $surface;
        border: thick $error;
        padding: 0;
    }

    .confirm-header {
        width: 100%;
        background: $error;
        color: white;
        text-align: center;
        text-style: bold;
        padding: 1;
    }

    .confirm-body {
        width: 100%;
        padding: 2;
        text-align: center;
    }

    .confirm-filename {
        color: $accent;
        text-style: bold;
    }

    .confirm-footer {
        width: 100%;
        padding: 1;
        align: center middle;
        background: $surface-darken-1;
    }

    .confirm-footer Button {
        margin: 0 1;
    }

    #query-container {
        border-bottom: solid $primary;
        background: $surface;
        height: 12; /* Default height */
    }

    #query-editor {
        height: 100%;
        border: none;
        background: $surface;
    }

    #results-container {
        height: 1fr;
        background: $surface;
    }

    #results-viewer {
        height: 100%;
    }

    #file-browser {
        height: 1fr;
        border: none;
    }

    #status-bar {
        height: 1;
        background: $primary;
        color: $text;
        content-align: center middle;
    }

    /* --- Filter Sidebar Specifics --- */
    .filter-group {
        margin-bottom: 2;
        background: $panel;
        padding: 1;
        border: solid $primary;
    }

    .filter-label {
        color: $accent;
        text-style: bold;
        margin-bottom: 1;
    }

    #filter-sidebar Select, #filter-sidebar Input {
        margin-bottom: 1;
    }

    #filter-actions {
        height: auto;
        margin-top: 2;
        align: center middle;
    }

    #filter-actions Button {
        width: 1fr;
        margin: 0 1;
    }

    /* --- Utility Classes --- */
    .hidden { display: none !important; }
    .maximized { height: 1fr !important; }

    /* RESTORED: Status Bar Colors */
    .error {
        background: $error;
        color: $text;
    }

    .success {
        background: $success;
        color: $text;
    }

    /* --- Dialog / Modal Styling (The Overhaul) --- */
    ModalScreen {
        align: center middle;
        background: rgba(0,0,0,0.5); /* Dim background */
    }

    .dialog-container {
        width: 60;
        height: auto;
        background: $surface;
        border: thick $primary;
        padding: 0;
    }

    .dialog-header {
        width: 100%;
        height: 3;
        background: $primary;
        color: $text;
        content-align: center middle;
        text-style: bold;
        border-bottom: solid $surface-lighten-1;
    }

    .dialog-body {
        width: 100%;
        height: auto;
        padding: 1 2;
        layout: vertical;
    }

    .dialog-footer {
        width: 100%;
        height: auto;
        padding: 1 2;
        align: right middle;
        background: $surface-darken-1;
    }

    .dialog-label {
        margin-top: 1;
        margin-bottom: 0;
        color: $text-muted;
    }

    .dialog-info {
        margin: 1 0;
        color: $accent;
        text-align: center;
    }

    /* Input/Select styling within dialogs */
    .dialog-body Input, .dialog-body Select {
        margin-bottom: 1;
    }

    /* Button Styling */
    Button {
        margin-left: 1;
    }

    .btn-primary {
        background: $primary;
        color: $text;
    }

    .btn-default {
        background: $surface-lighten-1;
    }

    /* RESTORED: Explain Dialog Styles (Legacy ID support) */
    #explain-dialog {
        width: 80;
        height: 30;
        background: $surface;
        border: thick $primary;
    }

    #explain-title {
        text-style: bold;
        text-align: center;
        margin-bottom: 1;
        background: $primary;
        color: $text;
        padding: 1;
    }

    #explain-content {
        height: 1fr;
        border: solid $accent;
        margin-bottom: 1;
        margin: 1;
    }

    #explain-text {
        padding: 1;
    }

    /* --- Autocomplete Popup --- */
    .autocomplete-popup {
        layer: overlay;
        background: $surface-lighten-1;
        border: solid $accent;
        display: block;
        position: absolute;
    }

    /* --- Config Sidebar Styles --- */
    #config-form {
        height: 1fr;
        padding: 1;
        scrollbar-gutter: stable; /* Prevent layout shift */
    }

    .config-group {
        height: auto;
        margin-bottom: 2;
        background: $panel;
        padding: 1;
        border: solid $primary;
        layout: vertical; /* CRITICAL: Ensures children stack */
    }

    .config-label {
        color: $accent;
        text-style: bold;
        margin-top: 1;
        margin-bottom: 0; /* Tighten up label to input */
    }

    .config-sublabel {
        color: $text;
        height: 1;
        margin-top: 1;
    }

    .config-description {
        color: $text-muted;
        text-style: italic;
        margin-bottom: 1;
        height: auto;
    }

    /* Ensure inputs/selects have space */
    #config-form Select, #config-form Input {
        margin-bottom: 1;
        height: auto;
    }

    /* Switch rows */
    .switch-row {
        height: auto;
        margin-bottom: 1;
        align: left middle;
    }

    .switch-row Label {
        width: 1fr;
    }

    #config-save-btn {
        width: 100%;
        margin-top: 1;
        margin-bottom: 2;
    }

    /* --- Compact Mode for DataTable --- */
    DataTable.compact .datatable--header {
        height: 1;
        padding: 0 1;
    }

    DataTable.compact .datatable--cursor {
        background: $accent 20%;
    }

    /* Reduce padding in cells for compact mode */
    DataTable.compact > .datatable--header-hover,
    DataTable.compact > .datatable--header {
        padding: 0 1;
    }
    """

    BINDINGS = [
        Binding("f1", "show_help", "Help"),
        Binding("f2", "toggle_sidebar", "Sidebar"),
        Binding("f3", "toggle_history", "History", show=False),
        Binding("f4", "toggle_explain", "Explain", show=False),
        Binding("f5", "cycle_backend", "Backend", show=False),

        # --- NEW BINDINGS ---
        Binding("f6", "cycle_layout", "Layout"),
        Binding("alt+up", "resize_query(-1)", "Shrink Edit", show=False),
        Binding("alt+down", "resize_query(1)", "Grow Edit", show=False),
        # --------------------

        Binding("ctrl+b", "cycle_backend", "Backend", show=True),
        Binding("ctrl+o", "open_file", "Files", priority=True),
        Binding("ctrl+f", "toggle_tools('filter')", "Filter", priority=True),
        Binding("ctrl+x", "toggle_tools('export')", "Export", priority=True),
        Binding("ctrl+q", "quit", "Exit", priority=True),
        Binding("ctrl+s", "save_state", "Save State"),
        Binding("ctrl+t", "new_tab", "New Tab", priority=True),
        Binding("ctrl+w", "close_tab", "Close Tab", priority=True),
        Binding("[", "prev_page", "◀ Prev", show=True, priority=True),
        Binding("]", "next_page", "Next ▶", show=True, priority=True),
    ]

    def __init__(
        self,
        initial_file: Optional[str] = None,
        history_file: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.initial_file = initial_file
        self.history_file = history_file or str(Path.home() / ".sqlstream_history")
        self.query_engine = Query()
        self.backend = "auto"
        self.query_history: List[str] = []
        self.history_index = -1
        self.last_results: List[Dict[str, Any]] = []
        self.last_query = ""
        self.loaded_files: List[str] = []
        self.config_file = str(Path.home() / ".sqlstream_config")

        # Configuration Defaults
        self.confirm_exit = False
        self.max_history = 100
        self.default_export_fmt = "csv"

        self.editor_theme = "dracula"
        self.editor_linenums = True
        self.editor_soft_wrap = False
        self.results_zebra = True
        self.results_compact = False

        # Pagination state
        self.page_size = 100
        self.current_page = 0

        # Filter and sort state
        self.filter_text = ""
        self.filter_column = None
        self.filter_mode = "contains"
        self.filter_active = False
        self.filtered_results: List[Dict[str, Any]] = []
        self.sort_column = None
        self.sort_reverse = False

        self.state_file = str(Path.home() / ".sqlstream_state")
        self.tab_counter = 0

        # --- Layout State ---
        self.layout_mode = 0  # 0: Split, 1: Max Editor, 2: Max Results
        self.query_height = 12 # Default height
        # --------------------

        if initial_file:
            self.loaded_files.append(initial_file)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)

        with Horizontal(id="main-container"):
            # Left Sidebar
            with Container(id="sidebar-container"):
                with TabbedContent(id="sidebar-tabs"):
                    with TabPane("Schema", id="tab-schema"):
                        yield SchemaBrowser(id="schema-browser")
                    with TabPane("Files", id="tab-files"):
                        yield FileBrowser("./", id="file-browser")
                    with TabPane("Config", id="tab-config"):
                        yield ConfigSidebar(id="config-sidebar")

            # Center Panel
            with Vertical(id="center-panel"):
                with Container(id="query-container"):
                    with TabbedContent(id="query-tabs"):
                        pass

                with Container(id="results-container"):
                    yield ResultsViewer(id="results-viewer")

                yield StatusBar(id="status-bar")

            # Right Sidebar (Tools)
            with Container(id="tools-sidebar"):
                with TabbedContent(id="tools-tabs"):
                    with TabPane("Filter", id="tab-filter"):
                        yield FilterSidebar(id="filter-sidebar")
                    with TabPane("Export", id="tab-export"):
                        yield ExportSidebar(id="export-sidebar")

        yield Footer()

    async def on_mount(self) -> None:
        """Initialize the shell on mount."""
        self.title = "SQLStream Interactive Shell"
        self.sub_title = "Ctrl+Enter: Run | F6: Layout | Alt+Up/Down: Resize"

        # Apply initial height
        self.query_one("#query-container").styles.height = self.query_height

        # 1. Load Config
        self.load_config_file()

        # 2. Apply Config to Static Widgets (ResultsViewer)
        results = self.query_one(ResultsViewer)
        results.zebra_stripes = self.results_zebra
        if self.results_compact:
            results.add_class("compact")

        # 3. Load State
        await self._load_state()

        status_bar = self.query_one(StatusBar)
        status_bar.update_status(
            "Welcome! Type SQL and press Ctrl+Enter. Use F6 to toggle layout."
        )

        self._get_active_editor().focus()

        if self.initial_file:
            self._load_initial_file()
            self._update_schema_browser()

    def action_cycle_layout(self) -> None:
        """Cycle between Split, Max Editor, and Max Results views."""
        self.layout_mode = (self.layout_mode + 1) % 3

        query_container = self.query_one("#query-container")
        results_container = self.query_one("#results-container")

        # Reset classes
        query_container.remove_class("hidden", "maximized")
        results_container.remove_class("hidden", "maximized")

        if self.layout_mode == 0:
            # Split View (Default)
            # Restore the fixed height
            query_container.styles.height = self.query_height
            self._show_status("Layout: Split View")

        elif self.layout_mode == 1:
            # Maximize Editor
            # IMPORTANT: Clear the fixed height so the CSS '1fr' takes effect
            query_container.styles.height = None
            query_container.add_class("maximized")
            results_container.add_class("hidden")
            self._show_status("Layout: Editor Fullscreen")

        elif self.layout_mode == 2:
            # Maximize Results
            # Clear height here as well to ensure it hides cleanly
            query_container.styles.height = None
            query_container.add_class("hidden")
            results_container.add_class("maximized")
            self._show_status("Layout: Results Fullscreen")

        # Force a refresh of the active editor to handle resize
        try:
            self._get_active_editor().refresh()
        except Exception:
            pass

    def action_resize_query(self, amount: int) -> None:
        """Resize the query editor height (only in Split View)."""
        if self.layout_mode != 0:
            self._show_status("Cannot resize in fullscreen mode", error=True)
            return

        # Update height
        new_height = self.query_height + amount

        # Enforce limits (min 3 lines, max 80% of screen approx)
        if 3 <= new_height <= 50:
            self.query_height = new_height
            self.query_one("#query-container").styles.height = self.query_height
            self._show_status(f"Editor Height: {self.query_height}")

    # --------------------------

    # ... (Keep all other existing methods: on_data_table_header_selected, action_history_prev, etc.) ...
    # ... (Copy the rest of the methods from your previous code here) ...

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
            # Fallback if no tabs exist yet or logic fails
            try:
                return self.query_one(QueryEditor)
            except Exception:
                pass
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

    def _strip_sql_comments(self, sql: str) -> str:
        """Strip SQL comments (lines starting with --) from the query."""
        lines = sql.split('\n')
        cleaned_lines = []
        for line in lines:
            # Find the position of '--' (not inside strings)
            comment_pos = -1
            in_string = False
            string_char = None

            for i, char in enumerate(line):
                if char in ('"', "'") and (i == 0 or line[i-1] != '\\'):
                    if not in_string:
                        in_string = True
                        string_char = char
                    elif char == string_char:
                        in_string = False
                        string_char = None
                elif char == '-' and i + 1 < len(line) and line[i + 1] == '-' and not in_string:
                    comment_pos = i
                    break

            if comment_pos >= 0:
                # Remove comment part
                line = line[:comment_pos].rstrip()

            if line.strip():  # Only keep non-empty lines
                cleaned_lines.append(line)

        return '\n'.join(cleaned_lines)

    def _execute_query(self, query_text: str) -> None:
        """Execute a SQL query and display results."""
        status_bar = self.query_one(StatusBar)
        results_viewer = self.query_one(ResultsViewer)

        try:
            # Clear previous results
            results_viewer.clear(columns=True)

            # Show loading status
            status_bar.update_status("Executing query...")

            # Strip SQL comments before execution
            cleaned_query = self._strip_sql_comments(query_text)

            # Execute query
            start_time = datetime.now()
            result = self.query_engine.sql(cleaned_query, backend=self.backend)

            # Safe source discovery
            try:
                _sources = result._discover_sources()
                self.loaded_files.extend([f for f in _sources.values() if f and f not in self.loaded_files])
            except Exception:
                pass

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
        """Apply filter text to results with specific modes."""
        if not self.filter_text:
            return results

        filtered = []
        filter_lower = self.filter_text.lower()
        mode = getattr(self, "filter_mode", "contains")

        for row in results:
            # Determine values to check
            if self.filter_column:
                values_to_check = [str(row.get(self.filter_column, ""))]
            else:
                values_to_check = [str(v) for v in row.values()]

            match_found = False
            for val in values_to_check:
                val_str = val.lower()

                if mode == "exact":
                    if val_str == filter_lower:
                        match_found = True
                elif mode == "startswith":
                    if val_str.startswith(filter_lower):
                        match_found = True
                elif mode == "endswith":
                    if val_str.endswith(filter_lower):
                        match_found = True
                else: # contains
                    if filter_lower in val_str:
                        match_found = True

                if match_found:
                    break

            if match_found:
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

    def _infer_column_types(self, results: List[Dict[str, Any]], columns: List[str]) -> Dict[str, str]:
        """Infer column datatypes from result values."""
        from sqlstream.core.types import infer_type

        # Datatype icons for compact display
        TYPE_ICONS = {
            "INTEGER": "#",
            "FLOAT": "~",
            "DECIMAL": "$",
            "STRING": '"',
            "JSON": "{}",
            "BOOLEAN": "?",
            "DATE": "📅",
            "TIME": "⏰",
            "DATETIME": "📆",
            "NULL": "∅"
        }

        column_types = {}

        for col in columns:
            # Collect non-null values for type inference
            values = [row.get(col) for row in results[:100] if row.get(col) is not None]

            if not values:
                column_types[col] = TYPE_ICONS.get("NULL", "∅")
                continue

            # Infer type from first non-null value
            first_val = values[0]
            dtype = infer_type(first_val)
            column_types[col] = TYPE_ICONS.get(dtype.name, dtype.name)

        return column_types

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

    def _prepare_value_for_export(self, value: Any) -> Any:
        """Prepare a value for export, preserving proper data types."""
        if value is None:
            return None
        elif isinstance(value, float):
            # Convert near-zero values to 0.0 to avoid scientific notation in exports
            if abs(value) < 1e-10 and value != 0:
                return 0.0
            return value
        else:
            return value

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

        # Infer column datatypes from the results
        column_types = self._infer_column_types(self.filtered_results, columns)

        # Add columns with datatype icons
        for col in columns:
            icon = column_types.get(col, '"')
            # Format: "column_name icon"
            col_label = f"{col} {icon}"
            results_viewer.add_column(col_label, key=col)

        # Add rows (current page only)
        for row in page_results:
            values = [self._format_value(row.get(col)) for col in columns]
            results_viewer.add_row(*values)

        # Update status with pagination info
        total_pages = (total_rows + self.page_size - 1) // self.page_size
        page_info = f"Page {self.current_page + 1}/{total_pages}"

        # FIX: Check filter_active flag
        filter_info = f" (filtered from {len(self.last_results)})" if self.filter_active else ""

        message = f"Showing {start_idx + 1}-{end_idx} of {total_rows} rows{filter_info} | {page_info}"
        status_bar.update_status(message, execution_time=execution_time, row_count=total_rows)
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

        # Construct filter info if active
        filter_info = ""
        # FIX: Check filter_active flag
        if self.filter_active:
            filter_mode = "equals" if self.filter_mode == "exact" else self.filter_mode
            if self.filter_column:
                filter_info = f"`{self.filter_column}` {filter_mode} "
            filter_info += f"'{self.filter_text}'"

        status_bar.update_status(message, filter_info=filter_info, backend_info=self.backend.upper())

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
        try:
            history_path = Path(self.history_file)
            history_path.parent.mkdir(parents=True, exist_ok=True)
            # Use configurable limit
            history_to_save = self.query_history[-self.max_history:]
            history_path.write_text("\n===\n".join(history_to_save))
        except Exception:
            pass

    def save_config_file(self) -> None:
        config = {
            "confirm_exit": self.confirm_exit,
            "max_history": self.max_history,
            "default_export_fmt": self.default_export_fmt,
            "app_theme": self.theme,
            "backend": self.backend,
            "page_size": self.page_size,
            "editor_theme": self.editor_theme,
            "editor_linenums": self.editor_linenums,
            "editor_soft_wrap": self.editor_soft_wrap,
            "results_zebra": self.results_zebra,
            "results_compact": self.results_compact
        }
        try:
            Path(self.config_file).write_text(json.dumps(config, indent=2))
        except Exception as e:
            self.notify(f"Failed to save config: {e}", severity="error")

    def load_config_file(self) -> None:
        try:
            path = Path(self.config_file)
            if not path.exists():
                return

            config = json.loads(path.read_text())

            # Functional
            self.confirm_exit = config.get("confirm_exit", False)
            self.max_history = int(config.get("max_history", 100))
            self.default_export_fmt = config.get("default_export_fmt", "csv")

            # Appearance & Execution
            if "app_theme" in config:
                self.theme = config["app_theme"]
            self.backend = config.get("backend", "auto")
            self.page_size = int(config.get("page_size", 100))
            self.editor_theme = config.get("editor_theme", "dracula")
            self.editor_linenums = config.get("editor_linenums", True)
            self.editor_soft_wrap = config.get("editor_soft_wrap", False)
            self.results_zebra = config.get("results_zebra", True)
            self.results_compact = config.get("results_compact", False)

        except Exception as e:
            self.notify(f"Failed to load config: {e}", severity="error")

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
        self.push_screen(HelpDialog())

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

    def action_toggle_tools(self, tab: str = "filter") -> None:
        """Toggle the right tools sidebar and select specific tab."""
        sidebar = self.query_one("#tools-sidebar")
        tabs = self.query_one("#tools-tabs", TabbedContent)

        # If sidebar is hidden, show it and select tab
        if not sidebar.has_class("visible"):
            sidebar.add_class("visible")
            tabs.active = f"tab-{tab}"
            self._refresh_tools_data()
        else:
            # If sidebar is visible...
            if tabs.active == f"tab-{tab}":
                # And we clicked the same key, hide it
                sidebar.remove_class("visible")
            else:
                # If we clicked a different key, just switch tab
                tabs.active = f"tab-{tab}"
                self._refresh_tools_data()

    def _refresh_tools_data(self) -> None:
        """Update filter columns and export counts based on current results."""
        if not self.last_results:
            return

        # Update Filter Sidebar
        try:
            cols = list(self.last_results[0].keys())
            self.query_one(FilterSidebar).update_columns(cols)
        except: pass

        # Update Export Sidebar
        try:
            # FIX: Check filter_active flag
            count = len(self.filtered_results) if self.filter_active else len(self.last_results)
            self.query_one(ExportSidebar).update_info(count)
        except: pass

    # Renamed from action_export_results to perform_export (called by sidebar)
    def perform_export(self, path: Path, fmt: str) -> None:
        """Execute the file write."""
        if not self.last_results:
            self._show_status("No results to export", error=True)
            return

        # FIX: Check filter_active flag explicitly
        results_to_export = self.filtered_results if self.filter_active else self.last_results

        try:
            row_count = len(results_to_export)
            filename = str(path)

            if fmt == 'csv':
                import csv
                with open(filename, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=results_to_export[0].keys())
                    writer.writeheader()
                    export_rows = []
                    for row in results_to_export:
                        export_row = {k: self._prepare_value_for_export(v) for k, v in row.items()}
                        export_rows.append(export_row)
                    writer.writerows(export_rows)
                self._show_status(f"✓ Exported {row_count} rows to CSV: {filename}")

            elif fmt == 'json':
                import json
                export_rows = []
                for row in results_to_export:
                    export_row = {k: self._prepare_value_for_export(v) for k, v in row.items()}
                    export_rows.append(export_row)
                with open(filename, "w") as f:
                    json.dump(export_rows, f, indent=2)
                self._show_status(f"✓ Exported {row_count} rows to JSON: {filename}")

            elif fmt == 'parquet':
                try:
                    import pyarrow as pa
                    import pyarrow.parquet as pq
                    table = pa.Table.from_pylist(results_to_export)
                    pq.write_table(table, filename)
                    self._show_status(f"✓ Exported {row_count} rows to Parquet: {filename}")
                except ImportError:
                    self._show_status("pyarrow not installed", error=True)

            # Close sidebar on success
            self.query_one("#tools-sidebar").remove_class("visible")

        except Exception as e:
            self._show_status(f"Export failed: {e}", error=True)

    def action_clear_filter(self) -> None:
        """Clear active filters."""
        self.filter_active = False
        self.filtered_results = self.last_results.copy()
        self.current_page = 0
        self._refresh_displayed_results()
        self._show_status("Filter cleared")

    def action_toggle_history(self) -> None:
        """Toggle query history panel."""
        self._show_status("Query history - Coming soon!")

    def action_cycle_backend(self) -> None:
        """Cycle through available execution backends."""
        backends = ["auto", "duckdb", "pandas", "python"]
        try:
            current_idx = backends.index(self.backend)
            next_idx = (current_idx + 1) % len(backends)
        except ValueError:
            next_idx = 0

        self.backend = backends[next_idx]
        self._show_status(f"Switched backend to: {self.backend.upper()}")

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

    def apply_advanced_filter(self, col: str, op: str, val1: str, val2: str) -> None:
        """Apply filter logic based on sidebar inputs."""
        if not self.last_results:
            return

        self.filtered_results = []

        # Helper to safely cast types
        def safe_cast(val, target_type):
            try:
                if target_type is float:
                    return float(val)
                if target_type is bool:
                    return val.lower() == "true"
                return str(val).lower()
            except Exception:
                return None

        for row in self.last_results:
            # 1. Determine Row Value
            if col == "global":
                # Global search is always string match
                row_vals = [str(v).lower() for v in row.values()]
                if any(val1.lower() in rv for rv in row_vals):
                    self.filtered_results.append(row)
                continue

            # Specific Column Search
            raw_val = row.get(col)

            # Determine type based on the raw value in the row
            target_type = str
            if isinstance(raw_val, (int, float)):
                target_type = float
            elif isinstance(raw_val, bool):
                target_type = bool

            # Cast row value and input value
            row_val = safe_cast(raw_val, target_type)
            input_val = safe_cast(val1, target_type)

            if row_val is None or input_val is None:
                continue # Skip invalid data

            match = False

            # 2. Apply Operator Logic
            if op == "eq":
                match = row_val == input_val
            elif op == "contains":
                match = str(input_val) in str(row_val)
            elif op == "startswith":
                match = str(row_val).startswith(str(input_val))
            elif op == "endswith":
                match = str(row_val).endswith(str(input_val))
            elif op == "gt":
                match = row_val > input_val
            elif op == "lt":
                match = row_val < input_val
            elif op == "between":
                input_val_2 = safe_cast(val2, target_type)
                if input_val_2 is not None:
                    match = input_val <= row_val <= input_val_2
            elif op == "is":
                # For booleans, input_val is already cast to bool
                match = row_val is input_val
            elif op == "regex":
                import re
                try:
                    if re.search(str(input_val), str(row_val), re.IGNORECASE):
                        match = True
                except Exception:
                    pass

            if match:
                self.filtered_results.append(row)

         # Update state so Export and Status Bar know a filter is active
        self.filter_active = True
        self.filter_column = col
        self.filter_mode = op
        self.filter_text = str(val1) # Store value for display/logic
        # ------------------------------------------------

        self.current_page = 0
        self._refresh_displayed_results()
        self._refresh_tools_data() # Update the export sidebar count immediately
        self._show_status(f"Filtered: {col} {op} {val1}")


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
        # FIX: Ignore events from the export tree to prevent overwriting the query
        if event.control.id == "export-tree":
            return

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
            theme=self.editor_theme,
            show_line_numbers=self.editor_linenums,
            soft_wrap=self.editor_soft_wrap,
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
        """Save state and exit, optionally confirming."""
        if self.confirm_exit:
            self.push_screen(ConfirmExitDialog(), self._finish_quit)
        else:
            self._finish_quit(True)

    def _finish_quit(self, should_quit: bool) -> None:
        if should_quit:
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
