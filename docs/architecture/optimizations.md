# Optimizations

SQLStream implements several optimizations to ensure efficient query execution.

## Predicate Pushdown

Filters (WHERE clauses) are "pushed down" as close to the data source as possible.

- **CSV**: Rows are filtered immediately after reading, before any other processing.
- **Parquet**: Filters are pushed down to the Parquet reader, which can skip entire row groups based on statistics (min/max values), significantly reducing I/O.

## Column Pruning

Only the columns required for the query are read from the disk.

- **Parquet**: The reader only decodes the requested columns.
- **CSV**: While the whole line is read, only relevant fields are parsed and kept.

## Vectorized Execution (Pandas Backend)

When using the Pandas backend, SQLStream leverages:
- **SIMD Instructions**: CPU-level parallelism for array operations.
- **Efficient Memory Layout**: Columnar storage in memory.
- **Optimized Algorithms**: C-optimized implementations of joins and aggregations.

## Lazy Evaluation

The query result is an iterator. Execution only happens when you consume the results. This allows for:
- **Early Termination**: `LIMIT` clauses stop execution as soon as enough rows are found.
- **Streaming**: You can start processing the first results while the rest of the query is still running.
