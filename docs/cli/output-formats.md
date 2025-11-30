# Output Formats

The SQLStream CLI supports multiple output formats to suit different needs, from human-readable tables to machine-parsable JSON.

## Specifying Output Format

Use the `--format` option with the `query` command.

```bash
sqlstream query "SELECT * FROM 'data.csv'" --format <format>
```

## Supported Formats

### Table (Default)

Displays results in a formatted ASCII table using the `rich` library. Best for human inspection.

```bash
sqlstream query "SELECT name, age FROM 'users.csv'"
```

**Output:**
```
┏━━━━━━┳━━━━━┓
┃ name ┃ age ┃
┡━━━━━━╇━━━━━┩
│ Alice│ 30  │
│ Bob  │ 25  │
└──────┴─────┘
```

### CSV

Outputs standard Comma-Separated Values. Useful for piping to other tools or saving to files.

```bash
sqlstream query "SELECT * FROM 'data.csv'" --format csv
```

**Output:**
```csv
name,age
Alice,30
Bob,25
```

### JSON

Outputs a JSON array of objects. Ideal for web applications or processing with `jq`.

```bash
sqlstream query "SELECT * FROM 'data.csv'" --format json
```

**Output:**
```json
[
  {"name": "Alice", "age": 30},
  {"name": "Bob", "age": 25}
]
```

### Markdown

Outputs a Markdown-formatted table. Great for generating documentation.

```bash
sqlstream query "SELECT * FROM 'data.csv'" --format markdown
```

**Output:**
```markdown
| name | age |
|------|-----|
| Alice| 30  |
| Bob  | 25  |
```

## Interactive Shell Export

In the interactive shell (`sqlstream shell`), you can export results using `Ctrl+X`. This automatically exports the current result set to **CSV**, **JSON**, and **Parquet** (if available) simultaneously, saving them with a timestamped filename.
