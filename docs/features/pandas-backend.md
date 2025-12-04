# Pandas Backend

SQLStream includes a high-performance execution backend powered by [pandas](https://pandas.pydata.org/). This backend is designed for scenarios where performance is critical and the dataset fits into memory.

## Enabling the Pandas Backend

You can specify the backend when executing a query:

### Python API

```python
from sqlstream import query

# Force pandas backend
result = query("data.csv").sql("SELECT * FROM data", backend="pandas")

# Auto mode (default) - uses pandas if available
result = query("data.csv").sql("SELECT * FROM data", backend="auto")
```

### CLI

The CLI automatically attempts to use the pandas backend if pandas is installed.

## Supported File Formats

The pandas backend supports **all file formats** that SQLStream offers:

- ✅ **CSV** - Comma-separated values
- ✅ **Parquet** - Apache Parquet columnar format
- ✅ **JSON** - Standard JSON files with nested path support
- ✅ **JSONL** - JSON Lines format (one JSON object per line)
- ✅ **XML** - XML files with element selection
- ✅ **HTML** - HTML table extraction
- ✅ **Markdown** - Markdown table parsing

All formats work seamlessly with the pandas backend through SQLStream's reader infrastructure. Each reader converts data to pandas DataFrames internally, enabling fast vectorized operations.

### Example with Different Formats

```python
from sqlstream import query

# JSON file with pandas backend
result = query("users.json").sql(
    "SELECT name, email FROM users WHERE age > 25",
    backend="pandas"
)

# XML file with pandas backend
result = query("data.xml#xml:record").sql(
    "SELECT product, price FROM data WHERE price > 100",
    backend="pandas"
)

# JSONL file with pandas backend
result = query("logs.jsonl").sql(
    "SELECT timestamp, level, message FROM logs WHERE level = 'ERROR'",
    backend="pandas"
)
```

## Benefits

1.  **Vectorized Execution**: Operations are performed on entire arrays at once rather than row-by-row, leading to significant speedups.
2.  **Optimized Joins**: Leverages pandas' highly optimized merge algorithms.
3.  **Efficient Aggregations**: Grouping and aggregation are much faster.
4.  **Universal Format Support**: Works with all file formats through SQLStream's reader architecture.

## Fallback Mechanism

When `backend="auto"` is used (the default), SQLStream checks if `pandas` is installed.
- If **installed**: It uses the PandasExecutor.
- If **not installed**: It falls back to the pure Python VolcanoExecutor.

This ensures that SQLStream remains lightweight and functional even without heavy dependencies, while offering performance when they are available.

## Limitations

- **Memory Usage**: The pandas backend loads data into memory (DataFrames). For datasets larger than available RAM, the streaming Python backend might be more appropriate (though slower).
