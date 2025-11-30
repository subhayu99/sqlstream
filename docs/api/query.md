# Query API

The `Query` and `QueryInline` classes are the primary interfaces for executing SQL against data files.

## `sqlstream.query`

A convenience function to create a `Query` object.

```python
def query(source: str) -> Query:
    ...
```

**Arguments:**
- `source`: Path to the data file (local path, URL, or S3 URI).

## `Query` Class

Represents a query builder for a specific data source.

### `__init__(source: str)`
Initializes the query with a source file.

### `sql(query: str, backend: str = "auto") -> QueryResult`
Executes a SQL query on the source.

**Arguments:**
- `query`: The SQL query string.
- `backend`: Execution backend ("auto", "pandas", "python").

**Returns:**
- `QueryResult`: An iterable object containing the results.

### `schema() -> Optional[Schema]`
Returns the inferred schema of the data source.

## `QueryInline` Class

Allows querying files specified directly in the SQL string.

### `sql(query: str, backend: str = "auto") -> QueryResult`
Executes a SQL query. The source files must be specified in the `FROM` clause (e.g., `FROM 'file.csv'`).

## `QueryResult` Class

Represents the result of a query execution. It is lazy and iterable.

### `__iter__()`
Yields result rows as dictionaries.

### `to_list() -> List[Dict[str, Any]]`
Materializes all results into a list.

### `explain() -> str`
Returns the execution plan for the query.
