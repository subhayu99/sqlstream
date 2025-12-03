# Data Sources

SQLStream supports multiple data source types.

## CSV Files

```python
from sqlstream import query

# Local CSV
results = query("data.csv").sql("SELECT * FROM data")

# CSV with custom delimiter
# Auto-detected: comma, tab, pipe, semicolon
```

## Parquet Files

```bash
pip install "sqlstream[parquet]"
```

```python
results = query("data.parquet").sql("SELECT * FROM data")
```

## JSON Files

No additional dependencies required!

```python
# Simple JSON array
results = query("data.json").sql("SELECT * FROM data")

# Nested JSON with path syntax
results = query("api.json#json:result.users").sql("SELECT * FROM users")

# Array flattening
results = query("data.json#json:users[].transactions").sql("SELECT * FROM transactions")
```

## JSONL Files

JSONL (JSON Lines) format - one JSON object per line.

```python
# Auto-detected by .jsonl extension
results = query("logs.jsonl").sql("SELECT * FROM logs WHERE level = 'ERROR'")
```

## HTTP URLs

```bash
pip install "sqlstream[http]"
```

```python
results = query("https://example.com/data.csv").sql("SELECT * FROM data")

# Works with JSON too
results = query("https://api.example.com/users.json").sql("SELECT * FROM users")
```

## Inline Paths

```bash
sqlstream query "SELECT * FROM 'data.csv'"
sqlstream query "SELECT * FROM 'data.parquet'"
sqlstream query "SELECT * FROM 'data.json'"
sqlstream query "SELECT * FROM 'logs.jsonl'"
sqlstream query "SELECT * FROM 'https://example.com/data.csv'"
```
