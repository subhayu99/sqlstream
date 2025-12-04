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

## HTML Files

Extract and query tables from HTML documents.

```bash
pip install "sqlstream[html]"
```

```python
# Query first table in HTML file
results = query("report.html").sql("SELECT * FROM report")

# Select specific table by index (0-based)
results = query("report.html#html:0").sql("SELECT * FROM report")

# Query tables from HTML URLs
results = query("https://example.com/data.html#html:1").sql("SELECT * FROM data")

# Match table by text content
results = query("report.html", match="Sales Data").sql("SELECT * FROM report")
```

## Markdown Files

Parse and query tables from Markdown documents.

```python
# Query first table in Markdown file
results = query("README.md").sql("SELECT * FROM readme")

# Select specific table by index (0-based)
results = query("README.md#markdown:1").sql("SELECT * FROM readme")

# Works with GitHub Flavored Markdown tables
results = query("docs/tables.md#markdown:0").sql("SELECT * FROM tables")
```

## XML Files

Extract and query structured data from XML files.

```python
# Query repeating elements (auto-detected)
results = query("data.xml").sql("SELECT * FROM data")

# Specify element path explicitly
results = query("data.xml#xml:record").sql("SELECT * FROM data WHERE age > 25")

# Works with nested elements (uses dot notation for nested paths)
results = query("data.xml#xml:person").sql("SELECT name, address.city FROM data")

# Attributes are prefixed with @ in column names
results = query("data.xml#xml:item").sql("SELECT @id, @status, name FROM data")
```

Example XML:
```xml
<?xml version="1.0"?>
<data>
    <record id="1">
        <name>Alice</name>
        <age>30</age>
    </record>
    <record id="2">
        <name>Bob</name>
        <age>25</age>
    </record>
</data>
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
sqlstream query "SELECT * FROM 'report.html#html:0'"
sqlstream query "SELECT * FROM 'README.md#markdown:1'"
sqlstream query "SELECT * FROM 'https://example.com/data.csv'"
```
