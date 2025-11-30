# CLI Overview

The `sqlstream` command-line interface provides an easy way to query data files with SQL.

## Available Commands

### `query` - Execute SQL Queries

Execute SQL queries on data files and display results.

```bash
sqlstream query [FILE] <SQL> [OPTIONS]
```

**Examples:**
```bash
# Query a file
sqlstream query data.csv "SELECT * FROM data WHERE age > 25"

# Inline file path
sqlstream query "SELECT * FROM 'data.csv' WHERE age > 25"

# JSON output
sqlstream query data.csv "SELECT * FROM data" --format json

# S3 files
sqlstream query "SELECT * FROM 's3://bucket/data.parquet' LIMIT 100"
```

See [Query Command](query-command.md) for full documentation.

### `shell` - Interactive SQL Shell

Launch the interactive shell with full TUI (Terminal User Interface).

```bash
sqlstream shell [FILE]
```

**Features:**
- Modal dialogs for filtering, export, file selection
- File browser (`Ctrl+O`) to select files
- Query execution plan visualization (`F4`)
- Multi-format export (`Ctrl+X`)
- Live filtering (`Ctrl+F`)
- Schema browser (`F2`)
- Query history with multiline support

See [Interactive Mode](interactive-mode.md) for full documentation.

## Global Options

- `--version` - Show SQLStream version
- `--help` - Show help message

## Quick Start

```bash
# Simple query
sqlstream query employees.csv "SELECT * FROM employees WHERE salary > 80000"

# With pandas backend for performance
sqlstream query large.csv "SELECT * FROM large" --backend pandas

# Launch interactive shell
sqlstream shell employees.csv
```

## Next Steps

- [Query Command Reference](query-command.md) - Detailed query command options
- [Interactive Shell Guide](interactive-mode.md) - Full interactive shell documentation
- [Output Formats](output-formats.md) - Available output formats
