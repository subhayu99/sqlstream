# Advanced Data Formats

SQLStream goes beyond standard CSV and Parquet support to handle semi-structured and document-based data formats.

## HTML Tables

You can query HTML tables directly from files or URLs. SQLStream uses `pandas` to extract tables and makes them queryable.

### Usage

```sql
-- Query a local HTML file
SELECT * FROM "data.html";

-- Query a remote HTML file
SELECT * FROM "https://example.com/data.html";
```

### Multiple Tables

If an HTML file contains multiple tables, you can select which one to query using the [URL Fragment Syntax](url-fragments.md):

```sql
-- Select the second table (index 1)
SELECT * FROM "data.html#:1";

-- Select the last table
SELECT * FROM "data.html#:-1";
```

### Schema Inference

Column names are extracted from the table header (`<th>` tags). Data types are inferred based on the content of the columns (Integer, Float, Boolean, String).

---

## Markdown Tables

SQLStream supports querying GitHub Flavored Markdown (GFM) tables. This is perfect for analyzing data embedded in `README.md` files or documentation.

### Usage

```sql
-- Query a local Markdown file
SELECT * FROM "README.md";

-- Query a remote Markdown file
SELECT * FROM "https://raw.githubusercontent.com/user/repo/main/README.md";
```

### Multiple Tables

Like HTML, you can select specific tables if the file contains more than one:

```sql
-- Select the first table (default)
SELECT * FROM "README.md#:0";

-- Select the second table
SELECT * FROM "README.md#:1";
```

### Features

- **Type Inference**: Automatically detects Integers, Floats, and Booleans (e.g., `true`, `false`, `yes`, `no`).
- **NULL Handling**: Recognizes `null`, `none`, `n/a`, and `-` as NULL values.
- **Escaping**: Handles escaped pipe characters `\|` in cell content.

---

## Supported Formats

### 1. HTML Tables

Query tables directly from HTML files or web pages using pandas' `read_html` functionality.

```python
from sqlstream import query

# Query HTML file
result = query("SELECT * FROM employees.html WHERE Department = 'Engineering'")

# Query specific table (if multiple tables in HTML)
from sqlstream.readers.html_reader import HTMLReader
reader = HTMLReader("data.html", table_index=1)  # Select second table
```

**Features:**
- Automatically extracts all `<table>` elements from HTML
- Select specific tables by index (0-based)
- Match tables by text content using `match` parameter
- Supports both local files and HTTP URLs

**Example:**
```python
from sqlstream.readers.html_reader import HTMLReader

# Get the first table
reader = HTMLReader("report.html", table_index=0)

# Find table containing specific text
reader = HTMLReader("report.html", match="Sales Data")

# List all tables in the HTML
for desc in reader.list_tables():
    print(desc)
```

### 2. Markdown Tables

Parse and query GitHub Flavored Markdown tables directly.

```python
from sqlstream import query

# Query markdown file
result = query("SELECT Product, Sales FROM data.md WHERE Sales > 100")
```

**Supported Markdown Table Format:**
```markdown
| Column1 | Column2 | Column3 |
|:--------|:-------:|--------:|
| Value1  | Value2  | Value3  |
| Value4  | Value5  | Value6  |
```

**Features:**
- Automatic type inference (integers, floats, booleans, strings)
- Support for alignment indicators (`:---`, `:---:`, `---:`)
- Handles escaped pipe characters (`\|`)
- Multiple table support (select by index)
- NULL value recognition (`null`, `NULL`, `N/A`, `-`)

**Example:**
```python
from sqlstream.readers.markdown_reader import MarkdownReader

# Query first table
reader = MarkdownReader("data.md", table_index=0)

# List all tables in markdown
for desc in reader.list_tables():
    print(desc)

# Access data
for row in reader.read_lazy():
    print(row)
```

### 3. Format Specification for URLs

For URLs that don't have file extensions (like pastebin raw URLs), you can explicitly specify the format.

```python
from sqlstream.readers.http_reader import HTTPReader

# Pastebin raw URL without .csv extension
reader = HTTPReader(
    "https://pastebin.com/raw/xxxxxx",
    format="csv"  # Explicitly specify format
)

# Works with all supported formats
reader = HTTPReader(url, format="parquet")
reader = HTTPReader(url, format="html")
reader = HTTPReader(url, format="markdown")
```

**With Pandas Executor:**
```python
from sqlstream.core.pandas_executor import PandasExecutor

executor = PandasExecutor()
df = executor._load_dataframe(
    "https://pastebin.com/raw/xxxxxx",
    format="csv"
)
```

## Format Auto-Detection

SQLstream intelligently detects file formats through multiple methods:

### 1. File Extension Detection
```python
# Automatically detected from extension
query("SELECT * FROM data.csv")      # → CSV
query("SELECT * FROM data.parquet")  # → Parquet  
query("SELECT * FROM data.html")     # → HTML
query("SELECT * FROM data.md")       # → Markdown
```

### 2. Content-Based Detection

For files without extensions or ambiguous URLs, SQLstream peeks at the content:

```python
# File without extension - auto-detects from content
reader = HTTPReader("https://example.com/data")  # Checks content type

# Detection logic:
# - Looks for HTML tags: <html>, <table>, <!doctype>
# - Looks for Markdown table markers: |, ---
# - Checks for Parquet magic number: PAR1
# - Defaults to CSV for others
```

### 3. HTTP Content-Type Headers

For HTTP URLs, the Content-Type header is used when available.

## Complete Usage Examples

### Example 1: HTML Sales Report

```python
from sqlstream import query

# HTML file with sales table
result = query("""
    SELECT Region, SUM(Sales) as total_sales
    FROM quarterly_report.html
    WHERE Quarter = 'Q2'
    GROUP BY Region
    ORDER BY total_sales DESC
""")

print(result)
```

### Example 2: Markdown Documentation

```markdown
# API Metrics

| Endpoint      | Requests | Avg_Latency | Error_Rate |
|:--------------|:--------:|:-----------:|-----------:|
| /api/users    | 1500     | 45.2        | 0.2        |
| /api/products | 3200     | 32.1        | 0.1        |
| /api/orders   | 890      | 120.5       | 1.5        |
```

```python
result = query("""
    SELECT Endpoint, Requests, Error_Rate 
    FROM api_metrics.md
    WHERE Error_Rate > 1.0
""")
```

### Example 3: Pastebin Data

```python
from sqlstream.readers.http_reader import HTTPReader

# Pastebin raw URL (no .csv extension)
reader = HTTPReader(
    "https://pastebin.com/raw/abc123", 
    format="csv"
)

# Now use it in queries
from sqlstream import query_df
import pandas as pd

df = pd.read_csv(reader.local_path)
# Or use with SQLstream directly
```

### Example 4: Multiple Tables

```python
from sqlstream.readers.html_reader import HTMLReader

# HTML with multiple tables
reader = HTMLReader("report.html")

# List all tables
print("Available tables:")
for i, desc in enumerate(reader.list_tables()):
    print(f"{i}: {desc}")

# Query specific table
reader_table2 = HTMLReader("report.html", table_index=2)
for row in reader_table2.read_lazy():
    print(row)
```

## Advanced Options

### HTML Reader Options

```python
from sqlstream.readers.html_reader import HTMLReader

# Pass options to pandas read_html
reader = HTMLReader(
    "data.html",
    table_index=0,
    match="Sales",           # Find table containing "Sales"
    header=0,                # Which row is header
    encoding='utf-8',        # File encoding
    thousands=',',           # Thousands separator
    decimal='.'              # Decimal separator
)
```

### Markdown Reader Options

```python
from sqlstream.readers.markdown_reader import MarkdownReader

# Select which table to read
reader = MarkdownReader(
    "document.md",
    table_index=1  # Second table (0-indexed)
)

# Apply filters and column selection
reader.set_filter([...])
reader.set_columns(['col1', 'col2'])
```

## Requirements

- **HTML Support**: Requires `pandas` and `lxml` or `html5lib`
  ```bash
  pip install sqlstream[pandas]
  pip install lxml  # or html5lib
  ```

- **Markdown Support**: No additional dependencies (built-in parser)

- **HTTP Support**: Requires `httpx`
  ```bash
  pip install sqlstream[http]
  ```

## Performance Tips

1. **HTML tables**: For large HTML files with many tables, use `table_index` or `match` to avoid parsing unnecessary tables.

2. **Markdown tables**: Markdown parsing is done in pure Python. For very large tables, consider converting to CSV first.

3. **URL caching**: HTTPReader automatically caches downloaded files. Use `force_download=True` to refresh.

```python
reader = HTTPReader(url, force_download=True)  # Re-download
```

4. **Clear cache** when done:
```python
HTTPReader.clear_all_cache()
```

## Migration Guide

### Old Way (Limited to .csv/.parquet)
```python
# Only worked with proper extensions
query("SELECT * FROM https://example.com/data.csv")  # ✅
query("SELECT * FROM https://pastebin.com/raw/xxx")  # ❌ Failed!
```

### New Way (Flexible Format Support)
```python
# Explicit format for extension-less URLs
from sqlstream.readers.http_reader import HTTPReader
reader = HTTPReader("https://pastebin.com/raw/xxx", format="csv")  # ✅

# HTML and Markdown support
query("SELECT * FROM report.html")  # ✅
query("SELECT * FROM data.md")      # ✅
```
