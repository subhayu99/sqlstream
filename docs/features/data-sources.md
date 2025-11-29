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

## HTTP URLs

```bash
pip install "sqlstream[http]"
```

```python
results = query("https://example.com/data.csv").sql("SELECT * FROM data")
```

## Inline Paths

```bash
sqlstream query "SELECT * FROM 'data.csv'"
sqlstream query "SELECT * FROM 'data.parquet'"
sqlstream query "SELECT * FROM 'https://example.com/data.csv'"
```
