# CLI Overview

The `sqlstream` command-line interface provides an easy way to query data files.

## Basic Usage

```bash
sqlstream query [FILE] <SQL> [OPTIONS]
```

## Quick Examples

```bash
# Query a file
sqlstream query data.csv "SELECT * FROM data"

# Inline file path
sqlstream query "SELECT * FROM 'data.csv'"

# JSON output
sqlstream query data.csv "SELECT * FROM data" --format json

# Interactive mode
sqlstream query data.csv "SELECT * FROM data" --interactive
```

## Available Commands

- `query` - Execute SQL queries on data files
- `interactive` - Launch interactive query interface (coming soon)

## Global Options

- `--version` - Show version
- `--help` - Show help message
