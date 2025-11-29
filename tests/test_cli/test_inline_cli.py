"""
Tests for CLI with inline file path syntax (Phase 7.6)
"""

import pytest

try:
    from click.testing import CliRunner

    from sqlstream.cli.main import cli

    CLICK_AVAILABLE = True
except ImportError:
    CLICK_AVAILABLE = False
    CliRunner = None
    cli = None


@pytest.mark.skipif(not CLICK_AVAILABLE, reason="Click not installed")
class TestInlineCLI:
    """Test CLI with inline file paths"""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create sample CSV file"""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("name,age,city\n" "Alice,30,NYC\n" "Bob,25,LA\n" "Charlie,35,Chicago\n")
        return csv_file

    @pytest.fixture
    def second_csv(self, tmp_path):
        """Create second CSV file for JOINs"""
        csv_file = tmp_path / "orders.csv"
        csv_file.write_text("name,product\n" "Alice,Laptop\n" "Bob,Phone\n")
        return csv_file

    def test_inline_simple_query(self, sample_csv):
        """Test simple query with inline file path"""
        runner = CliRunner()
        result = runner.invoke(cli, ["query", f"SELECT * FROM '{sample_csv}'"])

        assert result.exit_code == 0
        assert "Alice" in result.output
        assert "Bob" in result.output
        assert "Charlie" in result.output

    def test_inline_with_where(self, sample_csv):
        """Test inline query with WHERE clause"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["query", f"SELECT * FROM '{sample_csv}' WHERE age > 25"]
        )

        assert result.exit_code == 0
        assert "Alice" in result.output
        assert "Charlie" in result.output
        assert "Bob" not in result.output  # Bob is 25, not > 25

    def test_inline_with_format(self, sample_csv):
        """Test inline query with different output format"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["query", f"SELECT name FROM '{sample_csv}'", "--format", "json"]
        )

        assert result.exit_code == 0
        assert '"name"' in result.output or '"name":' in result.output

    def test_inline_join_query(self, sample_csv, second_csv):
        """Test inline query with JOIN"""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "query",
                f"SELECT name, age, product FROM '{sample_csv}' JOIN '{second_csv}' ON name = name",
            ],
        )

        assert result.exit_code == 0
        assert "Alice" in result.output
        assert "Laptop" in result.output

    def test_backward_compatible_old_syntax(self, sample_csv):
        """Test backward compatibility with old file + SQL syntax"""
        runner = CliRunner()
        result = runner.invoke(cli, ["query", str(sample_csv), "SELECT * FROM data"])

        assert result.exit_code == 0
        assert "Alice" in result.output
        assert "Bob" in result.output

    def test_inline_with_output_file(self, sample_csv, tmp_path):
        """Test inline query with output file"""
        output_file = tmp_path / "output.csv"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "query",
                f"SELECT * FROM '{sample_csv}'",
                "--format",
                "csv",
                "--output",
                str(output_file),
            ],
        )

        assert result.exit_code == 0
        assert output_file.exists()
        content = output_file.read_text()
        assert "Alice" in content
        assert "Bob" in content
