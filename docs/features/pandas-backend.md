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

## Benefits

1.  **Vectorized Execution**: Operations are performed on entire arrays at once rather than row-by-row, leading to significant speedups.
2.  **Optimized Joins**: Leverages pandas' highly optimized merge algorithms.
3.  **Efficient Aggregations**: Grouping and aggregation are much faster.

## Fallback Mechanism

When `backend="auto"` is used (the default), SQLStream checks if `pandas` is installed.
- If **installed**: It uses the PandasExecutor.
- If **not installed**: It falls back to the pure Python VolcanoExecutor.

This ensures that SQLStream remains lightweight and functional even without heavy dependencies, while offering performance when they are available.

## Limitations

- **Memory Usage**: The pandas backend loads data into memory (DataFrames). For datasets larger than available RAM, the streaming Python backend might be more appropriate (though slower).
