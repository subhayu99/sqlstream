# Query Command

Execute SQL queries on CSV and Parquet files.

## Syntax

```bash
sqlstream query [FILE_OR_SQL] [SQL] [OPTIONS]
```

## Arguments

- `FILE_OR_SQL` - File path or SQL query (optional if SQL contains inline paths)
- `SQL` - SQL query string (optional if using inline paths)

## Options

### Output Format

- `-f, --format [table|json|csv]` - Output format (default: table)
- `-o, --output FILE` - Write output to file

### Performance

- `-b, --backend [auto|pandas|python]` - Execution backend (default: auto)
- `-l, --limit N` - Limit displayed rows

### Display

- `--no-color` - Disable colored output
- `-i, --interactive` - Force interactive mode
- `--no-interactive` - Disable interactive mode
- `-t, --time` - Show execution time

### Debugging

- `--explain` - Show query execution plan

## Examples

See [Query Examples](../examples/basic-queries.md)
