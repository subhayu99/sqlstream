"""
Tests for CLI formatters
"""

import json

import pytest

from sqlstream.cli.formatters import (
    CSVFormatter,
    JSONFormatter,
    MarkdownFormatter,
    TableFormatter,
    get_formatter,
)


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


class TestMarkdownFormatter:
    """Test Markdown formatter"""

    def test_format_basic(self):
        """Test basic Markdown table formatting"""
        formatter = MarkdownFormatter()
        results = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]

        output = formatter.format(results)

        # Check header
        assert "| name | age |" in output
        # Check separator (default left alignment)
        assert "| :--- | :--- |" in output
        # Check data
        assert "| Alice | 30 |" in output
        assert "| Bob | 25 |" in output
        # Check footer
        assert "2 rows" in output

    def test_format_empty(self):
        """Test formatting empty results"""
        formatter = MarkdownFormatter()
        output = formatter.format([])

        assert output == "_No results found._"

    def test_format_with_null(self):
        """Test formatting with NULL values"""
        formatter = MarkdownFormatter()
        results = [{"name": "Alice", "age": None}]

        output = formatter.format(results)

        # Should display _NULL_ for None values
        assert "_NULL_" in output
        assert "| Alice | _NULL_ |" in output

    def test_format_pipe_escaping(self):
        """Test escaping pipe characters in cell values"""
        formatter = MarkdownFormatter()
        results = [{"name": "Smith | Jones", "age": 30}]

        output = formatter.format(results)

        # Pipes should be escaped
        assert "Smith \\| Jones" in output

    def test_alignment_left(self):
        """Test left alignment (default)"""
        formatter = MarkdownFormatter()
        results = [{"name": "Alice", "age": 30}]

        output = formatter.format(results, align="left")

        # Left alignment uses :---
        assert "| :--- | :--- |" in output

    def test_alignment_center(self):
        """Test center alignment"""
        formatter = MarkdownFormatter()
        results = [{"name": "Alice", "age": 30}]

        output = formatter.format(results, align="center")

        # Center alignment uses :---:
        assert "| :---: | :---: |" in output

    def test_alignment_right(self):
        """Test right alignment"""
        formatter = MarkdownFormatter()
        results = [{"name": "Alice", "age": 30}]

        output = formatter.format(results, align="right")

        # Right alignment uses ---:
        assert "| ---: | ---: |" in output

    def test_alignment_per_column(self):
        """Test different alignment per column"""
        formatter = MarkdownFormatter()
        results = [{"name": "Alice", "age": 30, "score": 95.5}]

        output = formatter.format(
            results, align={"name": "left", "age": "center", "score": "right"}
        )

        # Check mixed alignments
        assert "| :--- | :---: | ---: |" in output

    def test_no_footer(self):
        """Test disabling footer"""
        formatter = MarkdownFormatter()
        results = [{"name": "Alice", "age": 30}]

        output = formatter.format(results, show_footer=False)

        # Should not contain row count
        assert "1 row" not in output
        assert "rows" not in output

    def test_footer_singular(self):
        """Test footer with singular 'row' for 1 result"""
        formatter = MarkdownFormatter()
        results = [{"name": "Alice", "age": 30}]

        output = formatter.format(results, show_footer=True)

        # Should say "1 row" not "1 rows"
        assert "1 row" in output
        assert "1 rows" not in output

    def test_footer_plural(self):
        """Test footer with plural 'rows' for multiple results"""
        formatter = MarkdownFormatter()
        results = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]

        output = formatter.format(results, show_footer=True)

        # Should say "2 rows"
        assert "2 rows" in output

    def test_format_mixed_types(self):
        """Test formatting with mixed data types"""
        formatter = MarkdownFormatter()
        results = [
            {"name": "Alice", "age": 30, "score": 95.5, "active": True},
        ]

        output = formatter.format(results)

        # All types should be converted to strings
        assert "| Alice | 30 | 95.5 | True |" in output

    def test_format_with_special_characters(self):
        """Test formatting with special markdown characters"""
        formatter = MarkdownFormatter()
        results = [{"name": "**Bold** _Italic_", "value": "< > &"}]

        output = formatter.format(results)

        # Special characters should be preserved (except pipes)
        assert "**Bold** _Italic_" in output
        assert "< > &" in output
