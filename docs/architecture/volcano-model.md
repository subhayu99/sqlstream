# Volcano Model

The default execution model in SQLStream is based on the **Volcano Iterator Model** (also known as the Open-Next-Close model).

## How it Works

Each operator in the query plan (Scan, Filter, Project, Join) implements a standard interface with three methods:

1.  `open()`: Initialize the operator.
2.  `next()`: Retrieve the next tuple (row).
3.  `close()`: Clean up resources.

## Execution Flow

When a query is executed, the top-level operator calls `next()` on its child, which calls `next()` on its child, and so on, down to the Scan operator which reads from the file.

```python
# Simplified representation
class FilterOperator:
    def next(self):
        while True:
            row = self.child.next()
            if row is None:
                return None
            if self.predicate(row):
                return row
```

## Benefits

- **Low Memory Footprint**: Data is processed one row at a time. The entire dataset does not need to be loaded into memory.
- **Pipelining**: No intermediate results need to be materialized (except for blocking operators like Sort or Aggregate).
- **Simplicity**: Easy to implement and extend.

## Trade-offs

- **CPU Overhead**: Function call overhead for every row can be significant in Python.
- **Performance**: Slower than vectorized execution for large datasets. This is why SQLStream also offers a Pandas backend.
