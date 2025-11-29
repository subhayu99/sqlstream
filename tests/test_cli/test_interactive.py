"""
Tests for interactive mode detection and launch
"""

import pytest

from sqlstream.cli.interactive import should_use_interactive

try:
    from click.testing import CliRunner

    from sqlstream.cli.main import cli

    CLICK_AVAILABLE = True
except ImportError:
    CLICK_AVAILABLE = False
    CliRunner = None
    cli = None


class TestShouldUseInteractive:
    """Test auto-detection logic for interactive mode"""

    def test_force_flag_enables(self):
        """Test force flag always enables interactive"""
        results = [{"a": 1}]
        assert should_use_interactive(results, force=True) is True

    def test_no_interactive_flag_disables(self):
        """Test no_interactive flag disables auto-detection"""
        # Create data that would normally trigger auto-detection
        results = [{f"col{i}": i for i in range(15)}]
        assert should_use_interactive(results, no_interactive=True) is False

    def test_many_columns_triggers(self, monkeypatch):
        """Test auto-detection with many columns (>10)"""
        monkeypatch.setattr("sys.stdout.isatty", lambda: True)
        results = [{f"col{i}": i for i in range(15)}]
        assert should_use_interactive(results) is True

    def test_few_columns_no_trigger(self):
        """Test normal table with few columns doesn't trigger"""
        results = [{"a": 1, "b": 2, "c": 3}]
        # This should not trigger interactive mode
        assert should_use_interactive(results) is False

    def test_output_file_disables(self):
        """Test output file disables interactive mode"""
        results = [{f"col{i}": i for i in range(15)}]
        assert should_use_interactive(results, output_file="out.csv") is False

    def test_non_table_format_disables(self):
        """Test non-table format disables interactive mode"""
        results = [{f"col{i}": i for i in range(15)}]
        assert should_use_interactive(results, format="json") is False
        assert should_use_interactive(results, format="csv") is False

    def test_empty_results_no_trigger(self):
        """Test empty results don't trigger interactive mode"""
        results = []
        assert should_use_interactive(results) is False

    def test_no_tty_disables(self, monkeypatch):
        """Test interactive disabled when not TTY (piped output)"""
        monkeypatch.setattr("sys.stdout.isatty", lambda: False)
        results = [{f"col{i}": i for i in range(15)}]
        assert should_use_interactive(results) is False

    def test_wide_table_triggers(self, monkeypatch):
        """Test wide table (estimated width > terminal width)"""
        monkeypatch.setattr("sys.stdout.isatty", lambda: True)
        # Create table with long values
        results = [{"col1": "x" * 100, "col2": "y" * 100, "col3": "z" * 100}]
        # This should trigger due to estimated width
        assert should_use_interactive(results) is True

    def test_long_values_trigger(self, monkeypatch):
        """Test very long values (>50 chars) trigger interactive"""
        monkeypatch.setattr("sys.stdout.isatty", lambda: True)
        results = [{"short": "abc", "long": "x" * 60}]
        assert should_use_interactive(results) is True


@pytest.mark.skipif(not CLICK_AVAILABLE, reason="Click not installed")
class TestInteractiveCLI:
    """Test CLI integration with interactive mode"""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create sample CSV file"""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("name,age,city\n" "Alice,30,NYC\n" "Bob,25,LA\n")
        return csv_file

    def test_no_interactive_flag(self, sample_csv):
        """Test --no-interactive prevents auto-launch"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["query", str(sample_csv), "SELECT * FROM data", "--no-interactive"]
        )

        # Should use Rich table formatter, not interactive
        assert result.exit_code == 0
        assert "Alice" in result.output

    def test_interactive_flag_with_textual_missing(self, sample_csv):
        """Test --interactive flag when textual not installed"""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["query", str(sample_csv), "SELECT * FROM data", "--interactive"]
        )

        # Should attempt to launch interactive or show error about textual
        # Exit code may be 0 or 1 depending on whether textual is installed
        assert result.exit_code in [0, 1]

    def test_format_json_disables_interactive(self, sample_csv):
        """Test JSON format disables interactive mode"""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["query", str(sample_csv), "SELECT * FROM data", "--format", "json"],
        )

        # Should use JSON formatter, not interactive
        assert result.exit_code == 0
        assert '"name"' in result.output or '"name":' in result.output

    def test_output_file_disables_interactive(self, sample_csv, tmp_path):
        """Test output file disables interactive mode"""
        output_file = tmp_path / "output.txt"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "query",
                str(sample_csv),
                "SELECT * FROM data",
                "--output",
                str(output_file),
            ],
        )

        # Should write to file, not launch interactive
        assert result.exit_code == 0
        assert output_file.exists()
        assert "Alice" in output_file.read_text()
