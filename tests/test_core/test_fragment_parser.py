"""
Test URL fragment parsing and integration with readers
"""

import pytest

from sqlstream.core.fragment_parser import (
    FragmentParseError,
    build_source_fragment,
    parse_source_fragment,
)


class TestFragmentParser:
    """Test the URL fragment parser"""

    def test_parse_no_fragment(self):
        """Test parsing source without fragment"""
        source, format_hint, table = parse_source_fragment("data.csv")
        assert source == "data.csv"
        assert format_hint is None
        assert table is None

    def test_parse_format_only(self):
        """Test parsing with format only"""
        source, format_hint, table = parse_source_fragment("data.html#html")
        assert source == "data.html"
        assert format_hint == "html"
        assert table is None

    def test_parse_format_and_table(self):
        """Test parsing with both format and table"""
        source, format_hint, table = parse_source_fragment("page.html#html:1")
        assert source == "page.html"
        assert format_hint == "html"
        assert table == 1

    def test_parse_negative_table_index(self):
        """Test parsing with negative table index"""
        source, format_hint, table = parse_source_fragment("README.md#markdown:-1")
        assert source == "README.md"
        assert format_hint == "markdown"
        assert table == -1

    def test_parse_table_only(self):
        """Test parsing with table only (format omitted)"""
        source, format_hint, table = parse_source_fragment("data.html#:2")
        assert source == "data.html"
        assert format_hint is None
        assert table == 2

    def test_parse_url_with_fragment(self):
        """Test parsing HTTP URL with fragment"""
        source, format_hint, table = parse_source_fragment("https://example.com/data#csv:0")
        assert source == "https://example.com/data"
        assert format_hint == "csv"
        assert table == 0

    def test_parse_invalid_format(self):
        """Test that invalid format raises error"""
        with pytest.raises(FragmentParseError, match="Unknown format"):
            parse_source_fragment("data#invalid")

    def test_parse_string_table_identifier(self):
        """Test that string table identifiers are now allowed (for JSON nested paths)"""
        source, format_spec, table = parse_source_fragment("data.json#json:users.name")
        assert source == "data.json"
        assert format_spec == "json"
        assert table == "users.name"  # String identifier for nested JSON path

    def test_parse_empty_table_index(self):
        """Test that empty table index raises error"""
        with pytest.raises(FragmentParseError, match="Table index cannot be empty"):
            parse_source_fragment("data.html#html:")

    def test_build_fragment_full(self):
        """Test building fragment with both format and table"""
        result = build_source_fragment("data.html", format="html", table=1)
        assert result == "data.html#html:1"

    def test_build_fragment_format_only(self):
        """Test building fragment with format only"""
        result = build_source_fragment("data.csv", format="csv")
        assert result == "data.csv#csv"

    def test_build_fragment_table_only(self):
        """Test building fragment with table only"""
        result = build_source_fragment("data.html", table=2)
        assert result == "data.html#:2"

    def test_build_fragment_neither(self):
        """Test building fragment with neither (returns source as-is)"""
        result = build_source_fragment("data.csv")
        assert result == "data.csv"


class TestFragmentIntegration:
    """Test fragment integration with query engine"""

    def test_query_with_markdown_fragment(self):
        """Test querying markdown with fragment"""
        from sqlstream import query

        # This should create a MarkdownReader with table=0
        q = query("examples/sample_data.md#markdown:0")
        assert q.reader is not None
        assert q.reader.__class__.__name__ == "MarkdownReader"
        assert q.reader.table == 0

    def test_query_with_html_extension_and_table(self):
        """Test HTML file with table selection via fragment"""
        # Create a test HTML file first
        import os
        import tempfile

        html_content = """
        <html><body>
        <table><tr><th>A</th></tr><tr><td>1</td></tr></table>
        <table><tr><th>B</th></tr><tr><td>2</td></tr></table>
        </body></html>
        """

        with tempfile.NamedTemporaryFile(mode="w", suffix=".html", delete=False) as f:
            f.write(html_content)
            temp_path = f.name

        try:
            from sqlstream import query

            # Query second table
            q = query(f"{temp_path}#:1")
            assert q.reader.__class__.__name__ == "HTMLReader"
            assert q.reader.table == 1
        finally:
            os.unlink(temp_path)

    def test_inline_query_with_fragment(self):
        """Test fragment parsing with Query integration"""
        from sqlstream.core.query import Query

        q = Query()

        # Parse and create reader from SQL
        # This verifies fragment parsing works in SQL queries
        result = q.sql('SELECT * FROM "examples/sample_data.md#markdown:0" LIMIT 1')
        assert result is not None

        # Verify sources are discovered correctly
        sources = result._discover_sources()
        assert len(sources) > 0
        # Check that a source with the fragment was discovered
        assert any(
            "markdown" in str(path).lower() or "sample_data" in path for path in sources.values()
        )
