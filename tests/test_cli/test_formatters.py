"""
Tests for CLI formatters
"""

import json

import pytest

from sqlstream.cli.formatters import CSVFormatter, JSONFormatter, TableFormatter, get_formatter


class TestGetFormatter:
    """Test formatter factory function"""

    def test_get_table_formatter(self):
        """Test getting table formatter"""
        formatter = get_formatter("table")
        assert isinstance(formatter, TableFormatter)

    def test_get_json_formatter(self):
        """Test getting JSON formatter"""
        formatter = get_formatter("json")
        assert isinstance(formatter, JSONFormatter)

    def test_get_csv_formatter(self):
        """Test getting CSV formatter"""
        formatter = get_formatter("csv")
        assert isinstance(formatter, CSVFormatter)

    def test_unknown_formatter(self):
        """Test error for unknown formatter"""
        with pytest.raises(ValueError, match="Unknown format"):
            get_formatter("unknown")


class TestJSONFormatter:
    """Test JSON formatter"""

    def test_format_basic(self):
        """Test basic JSON formatting"""
        formatter = JSONFormatter()
        results = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]

        output = formatter.format(results)
        parsed = json.loads(output)

        assert len(parsed) == 2
        assert parsed[0]["name"] == "Alice"
        assert parsed[0]["age"] == 30

    def test_format_empty(self):
        """Test formatting empty results"""
        formatter = JSONFormatter()
        output = formatter.format([])
        parsed = json.loads(output)

        assert parsed == []

    def test_format_compact(self):
        """Test compact JSON output"""
        formatter = JSONFormatter()
        results = [{"name": "Alice", "age": 30}]

        output = formatter.format(results, compact=True)

        # Compact should have no spaces after separators
        assert ", " not in output

    def test_format_with_null(self):
        """Test formatting with NULL values"""
        formatter = JSONFormatter()
        results = [{"name": "Alice", "age": None}]

        output = formatter.format(results)
        parsed = json.loads(output)

        assert parsed[0]["age"] is None


class TestCSVFormatter:
    """Test CSV formatter"""

    def test_format_basic(self):
        """Test basic CSV formatting"""
        formatter = CSVFormatter()
        results = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]

        output = formatter.format(results)

        # CSV may use \r\n line endings, normalize to \n
        lines = output.replace("\r\n", "\n").strip().split("\n")
        assert lines[0] == "name,age"
        assert lines[1] == "Alice,30"
        assert lines[2] == "Bob,25"

    def test_format_empty(self):
        """Test formatting empty results"""
        formatter = CSVFormatter()
        output = formatter.format([])

        assert output == ""

    def test_format_with_commas(self):
        """Test CSV escaping with commas in values"""
        formatter = CSVFormatter()
        results = [{"name": "Smith, John", "age": 30}]

        output = formatter.format(results)

        # Should quote values with commas
        assert '"Smith, John"' in output

    def test_custom_delimiter(self):
        """Test custom delimiter"""
        formatter = CSVFormatter()
        results = [{"name": "Alice", "age": 30}]

        output = formatter.format(results, delimiter="|")

        assert "name|age" in output
        assert "Alice|30" in output


class TestTableFormatter:
    """Test Rich table formatter"""

    def test_format_basic(self):
        """Test basic table formatting"""
        try:
            formatter = TableFormatter()
            results = [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
            ]

            output = formatter.format(results)

            # Should contain the data
            assert "Alice" in output
            assert "Bob" in output
            assert "30" in output
            assert "25" in output

        except ImportError:
            pytest.skip("Rich not installed")

    def test_format_empty(self):
        """Test formatting empty results"""
        try:
            formatter = TableFormatter()
            output = formatter.format([])

            assert "No results found" in output

        except ImportError:
            pytest.skip("Rich not installed")

    def test_format_with_null(self):
        """Test formatting with NULL values"""
        try:
            formatter = TableFormatter()
            results = [{"name": "Alice", "age": None}]

            output = formatter.format(results)

            # Should display NULL
            assert "NULL" in output

        except ImportError:
            pytest.skip("Rich not installed")

    def test_row_count_footer(self):
        """Test row count footer"""
        try:
            formatter = TableFormatter()
            results = [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
            ]

            output = formatter.format(results, show_footer=True)

            # Should show row count (may have color codes)
            assert " rows" in output or "rows" in output

        except ImportError:
            pytest.skip("Rich not installed")
