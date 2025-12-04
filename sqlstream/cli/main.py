"""
SQLStream CLI - Query CSV/Parquet files with SQL

Usage:
    sqlstream query [file] <sql> [options]  # file is optional if SQL contains inline paths
    sqlstream shell [file]  # Launch interactive TUI
"""

import sys
import time
from typing import Optional

try:
    import click

    CLICK_AVAILABLE = True
except ImportError:
    CLICK_AVAILABLE = False
    click = None

from sqlstream import query as query_fn
from sqlstream.cli.formatters import get_formatter


@click.group()
@click.version_option(version="0.1.0", prog_name="sqlstream")
def cli():
    """
    SQLStream - Query CSV/Parquet files with SQL

    A lightweight SQL query engine for data exploration.
    """
    if not CLICK_AVAILABLE:
        print("CLI requires click library. Install with: pip install sqlstream[cli]")
        sys.exit(1)


@cli.command()
@click.argument("file_or_sql", type=str, required=False)
@click.argument("sql", type=str, required=False)
@click.option(
    "--sql-file",
    "-q",
    type=click.Path(exists=True),
    default=None,
    help="Read SQL query from file",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["table", "json", "csv", "markdown"], case_sensitive=False),
    default="table",
    help="Output format (default: table)",
)
@click.option(
    "--backend",
    "-b",
    type=click.Choice(["auto", "pandas", "python", "duckdb"], case_sensitive=False),
    default="auto",
    help="Execution backend (default: auto - smart selection based on query complexity)",
)
@click.option(
    "--limit",
    "-l",
    type=int,
    default=None,
    help="Limit number of rows displayed",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    default=None,
    help="Write output to file instead of stdout",
)
@click.option(
    "--no-color",
    is_flag=True,
    help="Disable colored output",
)
@click.option(
    "--explain",
    is_flag=True,
    help="Show query execution plan instead of results",
)
@click.option(
    "--time",
    "-t",
    "show_time",
    is_flag=True,
    help="Show execution time",
)
@click.option(
    "--interactive",
    "-i",
    is_flag=True,
    help="Force interactive mode (scrollable table viewer)",
)
@click.option(
    "--no-interactive",
    is_flag=True,
    help="Disable auto-detection of interactive mode",
)
def query(
    file_or_sql: Optional[str],
    sql: Optional[str],
    sql_file: Optional[str],
    format: str,
    backend: str,
    limit: Optional[int],
    output: Optional[str],
    no_color: bool,
    explain: bool,
    show_time: bool,
    interactive: bool,
    no_interactive: bool,
):
    """
    Execute SQL query on a data file

    Examples:

        \b
        # OLD SYNTAX: Query local CSV file (backward compatible)
        $ sqlstream query data.csv "SELECT * FROM data WHERE age > 25"

        \b
        # NEW SYNTAX: Inline file paths in SQL
        $ sqlstream query "SELECT * FROM 'data.csv' WHERE age > 25"

        \b
        # Read SQL from file
        $ sqlstream query --sql-file query.sql
        $ sqlstream query -q complex_join.sql

        \b
        # Multi-file JOIN with inline paths
        $ sqlstream query "SELECT x.*, y.name FROM 'data.csv' x JOIN 'other.csv' y ON x.id = y.id"

        \b
        # Query from URL with JSON output
        $ sqlstream query https://example.com/data.csv "SELECT name FROM data" -f json

        \b
        # Use pandas backend for performance
        $ sqlstream query data.parquet "SELECT * FROM data" --backend pandas

        \b
        # Show query plan
        $ sqlstream query data.csv "SELECT * FROM data WHERE age > 25" --explain

        \b
        # Save results to file
        $ sqlstream query data.csv "SELECT * FROM data" -f csv -o results.csv
    """
    fmt = format
    del format
    try:
        start_time = time.time()

        # Read SQL from file if --sql-file is provided
        if sql_file:
            with open(sql_file) as f:
                sql_from_file = f.read().strip()
            # If file_or_sql is provided with --sql-file, treat it as data file
            if file_or_sql:
                file = file_or_sql
                sql_query = sql_from_file
            else:
                # No data file, SQL has inline paths
                file = None
                sql_query = sql_from_file
        # Determine if we're using old syntax (file + sql) or new syntax (just sql)
        elif sql is None:
            # New syntax: file_or_sql is the SQL query with inline file paths
            sql_query = file_or_sql
            file = None
        else:
            # Old syntax: file_or_sql is the file, sql is the SQL query
            file = file_or_sql
            sql_query = sql

        # Create query
        if file:
            # Old syntax: single file provided
            q = query_fn(file)
        else:
            # New syntax: sourceless query (file paths in SQL)
            q = query_fn()

        # Execute query or show explain plan
        if explain:
            result = q.sql(sql_query, backend=backend)
            output_text = result.explain()
            click.echo(output_text)
        else:
            # Execute query
            result = q.sql(sql_query, backend=backend)
            results_list = list(result)

            # Auto-detect format from output file extension if not explicitly specified
            output_format = fmt
            if output and fmt == "table":
                # User provided -o but not -f, infer format from extension
                if output.endswith('.json'):
                    output_format = "json"
                elif output.endswith('.csv'):
                    output_format = "csv"
                elif output.endswith('.md'):
                    output_format = "markdown"
                # Otherwise keep as table format

            # Apply display limit if specified (doesn't affect query LIMIT)
            if limit is not None:
                results_list = results_list[:limit]

            # Check if interactive mode should be used
            from sqlstream.cli.interactive import launch_interactive, should_use_interactive

            if should_use_interactive(
                results_list,
                force=interactive,
                no_interactive=no_interactive,
                output_file=output,
                fmt=output_format,
            ):
                # Launch interactive TUI
                try:
                    launch_interactive(results_list)
                except ImportError as e:
                    click.echo(f"Error: {e}", err=True)
                    click.echo("Install with: pip install sqlstream[cli]", err=True)
                    sys.exit(1)
            else:
                # Use standard formatter
                formatter = get_formatter(output_format)
                output_text = formatter.format(
                    results_list,
                    no_color=no_color or (not sys.stdout.isatty()),
                    show_footer=not output,
                )

                # Add execution time if requested
                if show_time:
                    elapsed = time.time() - start_time
                    time_text = f"Processed {len(results_list)} in {elapsed:.3f}s"
                    if output_format == "table" and not no_color:
                        # Add colored time for table format
                        try:
                            from rich.console import Console

                            console = Console()
                            with console.capture() as capture:
                                console.print(f"[dim]{time_text}[/dim]")
                            output_text += capture.get()
                        except ImportError:
                            output_text += time_text
                    else:
                        output_text += time_text

                # Write output
                if output:
                    with open(output, "w") as f:
                        f.write(output_text)
                    click.echo(f"Results written to {output} ({output_format} format)", err=True)
                else:
                    click.echo(output_text)

    except FileNotFoundError as e:
        click.echo(f"Error: File not found - {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        if "--debug" in sys.argv:
            raise
        sys.exit(1)


@cli.command()
@click.argument("file", type=str, required=False)
@click.option(
    "--history-file",
    type=click.Path(),
    default=None,
    help="Path to query history file (default: ~/.sqlstream_history)",
)
def shell(file: Optional[str], history_file: Optional[str]):
    """
    Launch interactive SQL shell

    A full-featured SQL REPL with query editing, results viewing,
    schema browsing, and query history.

    Examples:

        \b
        # Launch empty shell
        $ sqlstream shell

        \b
        # Launch with initial file loaded
        $ sqlstream shell employees.csv

        \b
        # Use custom history file
        $ sqlstream shell --history-file ~/.my_history
    """
    try:
        from sqlstream.cli.shell import launch_shell

        launch_shell(initial_file=file, history_file=history_file)
    except ImportError as e:
        click.echo(
            f"Error: {e}\n"
            "Interactive shell requires textual library.\n"
            "Install with: pip install sqlstream[cli]",
            err=True,
        )
        sys.exit(1)


if __name__ == "__main__":
    cli()
