"""
SQLStream CLI - Query CSV/Parquet files with SQL

Usage:
    sqlstream query <file> <sql> [options]
    sqlstream interactive <file>  # Future: Textual TUI
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
@click.argument("file_or_sql", type=str)
@click.argument("sql", type=str, required=False)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["table", "json", "csv"], case_sensitive=False),
    default="table",
    help="Output format (default: table)",
)
@click.option(
    "--backend",
    "-b",
    type=click.Choice(["auto", "pandas", "python"], case_sensitive=False),
    default="auto",
    help="Execution backend (default: auto)",
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
    file_or_sql: str,
    sql: Optional[str],
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
    try:
        start_time = time.time()

        # Determine if we're using old syntax (file + sql) or new syntax (just sql)
        if sql is None:
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
            # New syntax: extract file from SQL
            # Use a temporary Query with inline mode
            from sqlstream.core.query import QueryInline
            q = QueryInline()

        # Execute query or show explain plan
        if explain:
            result = q.sql(sql_query, backend=backend)
            output_text = result.explain()
            click.echo(output_text)
        else:
            # Execute query
            result = q.sql(sql_query, backend=backend)
            results_list = list(result)

            # Apply display limit if specified (doesn't affect query LIMIT)
            if limit is not None:
                results_list = results_list[:limit]

            # Check if interactive mode should be used
            from sqlstream.cli.interactive import should_use_interactive, launch_interactive

            if should_use_interactive(
                results_list,
                force=interactive,
                no_interactive=no_interactive,
                output_file=output,
                format=format,
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
                formatter = get_formatter(format)
                output_text = formatter.format(
                    results_list,
                    no_color=no_color or (not sys.stdout.isatty()),
                    show_footer=not output,
                )

                # Add execution time if requested
                if show_time:
                    elapsed = time.time() - start_time
                    time_text = f"\nExecution time: {elapsed:.3f}s"
                    if format == "table" and not no_color:
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
                    click.echo(f"Results written to {output}", err=True)
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
@click.argument("file", type=str)
def interactive(file: str):
    """
    Launch interactive query interface (Coming Soon!)

    This will open a Textual-based TUI for exploring data interactively.

    Install with: pip install sqlstream[interactive]
    """
    try:
        # Try to import textual
        import textual  # noqa: F401

        click.echo("Interactive mode is coming soon!")
        click.echo("This will feature:")
        click.echo("  - Live query editing")
        click.echo("  - Interactive result browsing")
        click.echo("  - Export functionality")
        click.echo("  - Beautiful TUI powered by Textual")
        sys.exit(0)
    except ImportError:
        click.echo(
            "Interactive mode requires textual library.\n"
            "Install with: pip install sqlstream[interactive]",
            err=True,
        )
        sys.exit(1)


if __name__ == "__main__":
    cli()
