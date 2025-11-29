"""
Tests for CLI main commands
"""

import tempfile
from pathlib import Path

import pytest

try:
    from click.testing import CliRunner

    from sqlstream.cli.main import cli

    CLICK_AVAILABLE = True
except ImportError:
    CLICK_AVAILABLE = False
    CliRunner = None
    cli = None


@pytest.fixture
def sample_csv(tmp_path):
    """Create sample CSV file"""
    csv_file = tmp_path / "data.csv"
    csv_file.write_text(
        "name,age,city\n"
        "Alice,30,NYC\n"
        "Bob,25,LA\n"
        "Charlie,35,SF\n"
    )
    return csv_file


@pytest.mark.skipif(not CLICK_AVAILABLE, reason="Click not installed")
class TestQueryCommand:
    """Test query command"""

    def test_query_basic(self, sample_csv):
        """Test basic query execution"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["query", str(sample_csv), "SELECT * FROM data"]
        )

        assert result.exit_code == 0
        assert "Alice" in result.output
        assert "Bob" in result.output

    def test_query_with_where(self, sample_csv):
        """Test query with WHERE clause"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["query", str(sample_csv), "SELECT * FROM data WHERE age > 25"]
        )

        assert result.exit_code == 0
        assert "Alice" in result.output
        assert "Charlie" in result.output
        assert "Bob" not in result.output  # age=25, not > 25

    def test_query_json_format(self, sample_csv):
        """Test JSON output format"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["query", str(sample_csv), "SELECT name FROM data", "--format", "json"]
        )

        assert result.exit_code == 0
        assert '"name": "Alice"' in result.output or '"name":"Alice"' in result.output

    def test_query_csv_format(self, sample_csv):
        """Test CSV output format"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["query", str(sample_csv), "SELECT name FROM data", "--format", "csv"]
        )

        assert result.exit_code == 0
        assert "name" in result.output
        assert "Alice" in result.output

    def test_query_with_limit(self, sample_csv):
        """Test display limit option"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["query", str(sample_csv), "SELECT * FROM data", "--limit", "2"]
        )

        assert result.exit_code == 0
        # Should show 2 rows in footer
        assert "2 rows" in result.output

    def test_query_with_backend(self, sample_csv):
        """Test backend selection"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["query", str(sample_csv), "SELECT * FROM data", "--backend", "python"]
        )

        assert result.exit_code == 0
        assert "Alice" in result.output

    def test_query_explain(self, sample_csv):
        """Test explain flag"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["query", str(sample_csv), "SELECT * FROM data WHERE age > 25", "--explain"]
        )

        assert result.exit_code == 0
        # Should show plan, not results
        assert "Plan" in result.output or "plan" in result.output

    def test_query_file_not_found(self):
        """Test error handling for missing file"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["query", "/nonexistent/file.csv", "SELECT * FROM data"]
        )

        assert result.exit_code == 1
        assert "Error" in result.output or "error" in result.output

    def test_query_invalid_sql(self, sample_csv):
        """Test error handling for invalid SQL"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["query", str(sample_csv), "INVALID SQL SYNTAX"]
        )

        assert result.exit_code == 1

    def test_query_output_to_file(self, sample_csv, tmp_path):
        """Test writing output to file"""
        output_file = tmp_path / "output.csv"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "query",
                str(sample_csv),
                "SELECT * FROM data",
                "--format",
                "csv",
                "--output",
                str(output_file),
            ],
        )

        assert result.exit_code == 0
        assert output_file.exists()

        # Check file contents
        content = output_file.read_text()
        assert "Alice" in content


@pytest.mark.skipif(not CLICK_AVAILABLE, reason="Click not installed")
class TestInteractiveCommand:
    """Test interactive command"""

    def test_interactive_not_implemented(self, sample_csv):
        """Test interactive command shows coming soon message"""
        runner = CliRunner()
        result = runner.invoke(cli, ["interactive", str(sample_csv)])

        # Should show message about coming soon or missing textual
        assert "Interactive" in result.output or "textual" in result.output


@pytest.mark.skipif(not CLICK_AVAILABLE, reason="Click not installed")
class TestCLIVersion:
    """Test CLI version info"""

    def test_version(self):
        """Test --version flag"""
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])

        assert result.exit_code == 0
        assert "0.1.0" in result.output
