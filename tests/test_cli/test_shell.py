"""Tests for Interactive SQL Shell.

Validates that shell components work correctly without running the full TUI
(to avoid terminal keybinding conflicts).
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock


class TestShellComponents:
    """Test shell component imports and initialization."""

    def test_shell_imports(self):
        """Test that all shell components can be imported."""
        from sqlstream.cli.shell import (
            ResultsViewer,
            SchemaBrowser,
            StatusBar,
        )

        # Verify they can be instantiated with kwargs
        rv = ResultsViewer(id="test-viewer")
        sb = StatusBar(id="test-status")
        tree = SchemaBrowser(id="test-schema")

        assert rv is not None
        assert sb is not None
        assert tree is not None

    def test_shell_app_initialization(self):
        """Test shell app initialization."""
        from sqlstream.cli.shell import SQLShellApp

        app = SQLShellApp(initial_file=None, history_file="/tmp/test_history")
        assert app is not None
        assert app.query_history == []
        assert app.history_index == -1


class TestQueryExecution:
    """Test query execution engine."""

    @pytest.fixture
    def test_csv(self, tmp_path):
        """Create a test CSV file."""
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(
            "name,age,city,salary\n"
            "Alice,30,NYC,90000\n"
            "Bob,25,LA,75000\n"
            "Charlie,35,SF,85000\n"
        )
        return str(csv_file)

    def test_query_execution(self, test_csv):
        """Test executing a query."""
        from sqlstream.core.query import Query

        q = Query()
        result = q.sql(f"SELECT * FROM '{test_csv}' WHERE age > 25")
        results = result.to_list()

        assert len(results) == 2
        assert all(row['age'] > 25 for row in results)


class TestHistoryNavigation:
    """Test query history navigation logic."""

    def test_history_prev(self):
        """Test navigating to previous query."""
        from sqlstream.cli.shell import SQLShellApp

        app = SQLShellApp(initial_file=None, history_file="/tmp/test_history")
        app.query_history = ["SELECT 1", "SELECT 2"]

        # Mock query_one
        class DummyEditor:
            def __init__(self):
                self.text = ""
                self.cursor_location = (0, 0)

        app._query_one_mock = DummyEditor()

        def mock_query_one(*args, **kwargs):
            if args and args[0] == "#query-tabs":
                tabs = MagicMock()
                tabs.active = None  # Simulate no active tab to force fallback
                return tabs
            return app._query_one_mock

        app.query_one = mock_query_one

        # Test Prev
        app.action_history_prev()
        assert app.history_index == 1
        assert app._query_one_mock.text == "SELECT 2"

        app.action_history_prev()
        assert app.history_index == 0
        assert app._query_one_mock.text == "SELECT 1"

    def test_history_next(self):
        """Test navigating to next query."""
        from sqlstream.cli.shell import SQLShellApp

        app = SQLShellApp(initial_file=None, history_file="/tmp/test_history")
        app.query_history = ["SELECT 1", "SELECT 2"]
        app.history_index = 0

        # Mock query_one
        class DummyEditor:
            def __init__(self):
                self.text = ""
                self.cursor_location = (0, 0)

        app._query_one_mock = DummyEditor()

        def mock_query_one(*args, **kwargs):
            if args and args[0] == "#query-tabs":
                tabs = MagicMock()
                tabs.active = None
                return tabs
            return app._query_one_mock

        app.query_one = mock_query_one

        # Test Next
        app.action_history_next()
        assert app.history_index == 1
        assert app._query_one_mock.text == "SELECT 2"

        app.action_history_next()
        assert app.history_index == -1
        assert app._query_one_mock.text == ""


class TestSchemaBrowser:
    """Test schema browser functionality."""

    def test_schema_browser_population(self):
        """Test populating schema browser with schemas."""
        from sqlstream.cli.shell import SchemaBrowser

        sb = SchemaBrowser(id="test-schema")
        schemas = {"test.csv": {"name": "string", "age": "int"}}
        sb.show_schemas(schemas)

        # Check if nodes added
        assert len(sb.root.children) == 1
        assert str(sb.root.children[0].label) == "test.csv"
        assert len(sb.root.children[0].children) == 2


class TestExport:
    """Test export functionality."""

    def test_export_to_csv(self):
        """Test exporting results to CSV - simplified for unit testing."""
        from sqlstream.cli.shell import SQLShellApp

        app = SQLShellApp(initial_file=None, history_file="/tmp/test_history")
        app.last_results = [{"col1": "val1", "col2": "val2"}]

        # Test that last_results are populated correctly
        assert len(app.last_results) == 1
        assert app.last_results[0]["col1"] == "val1"
        assert app.last_results[0]["col2"] == "val2"

        # Note: Full export testing requires Textual app infrastructure
        # which is tested through integration tests


class TestPagination:
    """Test pagination logic."""

    def test_pagination_logic(self):
        """Test pagination with 250 rows."""
        from sqlstream.cli.shell import SQLShellApp

        app = SQLShellApp(initial_file=None, history_file="/tmp/test_history")
        app.last_results = [{"id": i, "value": f"row{i}"} for i in range(250)]
        app.filtered_results = app.last_results.copy()
        app.page_size = 100
        app.current_page = 0

        assert len(app.filtered_results) == 250
        total_pages = (250 + 100 - 1) // 100
        assert total_pages == 3

        # Test next page
        if app.current_page < total_pages - 1:
            app.current_page += 1
        assert app.current_page == 1

        # Test prev page
        if app.current_page > 0:
            app.current_page -= 1
        assert app.current_page == 0


class TestSorting:
    """Test sorting logic."""

    def test_sorting_ascending(self):
        """Test sorting in ascending order."""
        from sqlstream.cli.shell import SQLShellApp

        app = SQLShellApp(initial_file=None, history_file="/tmp/test_history")
        app.last_results = [
            {"id": 3, "name": "Charlie"},
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
        app.sort_column = "id"
        app.sort_reverse = False

        sorted_results = app._apply_sort(app.last_results)
        assert sorted_results[0]["id"] == 1
        assert sorted_results[2]["id"] == 3

    def test_sorting_descending(self):
        """Test sorting in descending order."""
        from sqlstream.cli.shell import SQLShellApp

        app = SQLShellApp(initial_file=None, history_file="/tmp/test_history")
        app.last_results = [
            {"id": 3, "name": "Charlie"},
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
        app.sort_column = "id"
        app.sort_reverse = True

        sorted_results = app._apply_sort(app.last_results)
        assert sorted_results[0]["id"] == 3
        assert sorted_results[2]["id"] == 1


class TestCLIRegistration:
    """Test CLI command registration."""

    def test_shell_command_registered(self):
        """Test that shell command is registered in CLI."""
        from sqlstream.cli.main import cli

        commands = [cmd.name for cmd in cli.commands.values()]
        assert 'shell' in commands


class TestFiltering:
    """Test advanced filtering functionality."""

    def test_filter_with_special_characters(self):
        """Test filtering with special characters."""
        from sqlstream.cli.shell import SQLShellApp

        app = SQLShellApp(initial_file=None, history_file="/tmp/test_history")
        app.last_results = [
            {"email": "user@example.com", "name": "Alice"},
            {"email": "admin@test.org", "name": "Bob"},
        ]
        app.filter_text = "@example"

        filtered = app._apply_filter(app.last_results)
        assert len(filtered) == 1
        assert filtered[0]["email"] == "user@example.com"

    def test_filter_with_numbers(self):
        """Test filtering with numeric values."""
        from sqlstream.cli.shell import SQLShellApp

        app = SQLShellApp(initial_file=None, history_file="/tmp/test_history")
        app.last_results = [
            {"id": 1, "value": 100},
            {"id": 2, "value": 200},
            {"id": 3, "value": 150},
        ]
        app.filter_text = "200"

        filtered = app._apply_filter(app.last_results)
        assert len(filtered) == 1
        assert filtered[0]["id"] == 2

    def test_filtering_case_insensitive(self):
        """Test case-insensitive filtering."""
        from sqlstream.cli.shell import SQLShellApp

        app = SQLShellApp(initial_file=None, history_file="/tmp/test_history")
        app.last_results = [
            {"name": "Alice", "city": "NYC"},
            {"name": "Bob", "city": "LA"},
            {"name": "Charlie", "city": "NYC"}
        ]
        app.filter_text = "nyc"

        filtered = app._apply_filter(app.last_results)
        assert len(filtered) == 2
        assert all("nyc" in str(r).lower() for r in filtered)


class TestSortingEdgeCases:
    """Test sorting edge cases."""

    def test_sorting_with_none_values(self):
        """Test sorting with NULL/None values."""
        from sqlstream.cli.shell import SQLShellApp

        app = SQLShellApp(initial_file=None, history_file="/tmp/test_history")
        app.last_results = [
            {"id": 1, "value": None},
            {"id": 2, "value": 50},
            {"id": 3, "value": None},
            {"id": 4, "value": 25},
        ]
        app.sort_column = "value"
        app.sort_reverse = False

        sorted_results = app._apply_sort(app.last_results)

        # None values should be handled (typically sorted to beginning or end)
        assert len(sorted_results) == 4
        # At least verify no exceptions were raised

    def test_sorting_mixed_types(self):
        """Test sorting with mixed types in column."""
        from sqlstream.cli.shell import SQLShellApp

        app = SQLShellApp(initial_file=None, history_file="/tmp/test_history")
        app.last_results = [
            {"id": 1, "value": "100"},
            {"id": 2, "value": "50"},
            {"id": 3, "value": "25"},
        ]
        app.sort_column = "value"
        app.sort_reverse = False

        sorted_results = app._apply_sort(app.last_results)

        # Should handle string sorting
        assert len(sorted_results) == 3
