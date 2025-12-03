# Querying JSON Data

SQLStream allows you to query JSON and JSONL (JSON Lines) files with powerful nested path syntax.

## JSON File Format

JSON files can be:
- **Array of objects**: `[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]`
- **Object with records key**: `{"data": [{"id": 1}, {"id": 2}]}`

## Basic JSON Querying

### Example 1: Simple JSON Array

```python
from sqlstream import query

# data.json: [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
results = query("data.json").sql("SELECT * FROM data WHERE age > 25")

for row in results:
    print(row)
```

### Example 2: JSON with Nested Key

```python
# api_response.json: {"result": {"users": [{"id": 1, "name": "Alice"}]}}

# Use fragment syntax to specify nested path
results = query("api_response.json#json:result.users").sql(
    "SELECT * FROM users"
)
```

## Advanced: Nested Path Syntax

SQLStream supports JSONPath-like syntax for accessing nested data:

### Path Operators

| Syntax | Description | Example |
|--------|-------------|---------|
| `key` | Simple key access | `users` |
| `key.nested` | Nested object access | `result.users` |
| `key[0]` | Array index | `users[0].transactions` |
| `key[]` | Flatten array | `users[].transactions` |
| Combined | Any combination | `data.users[].orders` |

### Example 3: Array Indexing

```python
# Get transactions for the first user only
data = {
    "users": [
        {"name": "Alice", "transactions": [{"id": 1, "amount": 50}]},
        {"name": "Bob", "transactions": [{"id": 2, "amount": 100}]}
    ]
}

# Access first user's transactions
results = query("data.json#json:users[0].transactions").sql(
    "SELECT * FROM transactions"
)
```

### Example 4: Array Flattening

```python
# Merge transactions from ALL users into a single table
results = query("data.json#json:users[].transactions").sql(
    "SELECT * FROM transactions WHERE amount > 50"
)

# This flattens [users] and extracts transactions from each
```

### Example 5: Deeply Nested Data

```python
# Complex nested structure
data = {
    "api_version": "1.0",
    "response": {
        "data": {
            "orders": [
                {"id": 1, "status": "completed"},
                {"id": 2, "status": "pending"}
            ]
        }
    }
}

# Navigate to deeply nested orders
results = query("api.json#json:response.data.orders").sql(
    "SELECT * FROM orders WHERE status = 'completed'"
)
```

## JSONL (JSON Lines) Format

JSONL files contain one JSON object per line - perfect for streaming and large datasets.

### Format

```jsonl
{"id": 1, "name": "Alice", "age": 30}
{"id": 2, "name": "Bob", "age": 25}
{"id": 3, "name": "Charlie", "age": 35}
```

### Querying JSONL

```python
from sqlstream import query

# Query JSONL file (auto-detected by .jsonl extension)
results = query("logs.jsonl").sql(
    "SELECT * FROM logs WHERE age > 25"
)

# Or specify format explicitly
results = query("data#jsonl").sql("SELECT * FROM data")
```

### Why JSONL?

- ✅ **Streaming**: Process line-by-line without loading entire file
- ✅ **Append-friendly**: Easy to add new records
- ✅ **Common format**: Used by APIs, logging systems, data dumps
- ✅ **Large files**: Better for massive datasets than standard JSON

## HTTP/URL Support

Query JSON/JSONL from URLs:

```python
# Remote JSON
url = "https://api.example.com/users.json"
results = query(url).sql("SELECT * FROM users WHERE active = true")

# Remote JSONL
url = "https://example.com/logs.jsonl"
results = query(url).sql("SELECT * FROM logs WHERE level = 'ERROR'")

# With nested path
url = "https://api.example.com/data.json#json:result.items"
results = query(url).sql("SELECT * FROM items")
```

## S3 Support

Query JSON/JSONL directly from S3:

```python
# S3 JSON file
results = query("s3://my-bucket/data.json").sql("SELECT * FROM data")

# S3 JSONL file
results = query("s3://my-bucket/logs.jsonl").sql(
    "SELECT * FROM logs WHERE timestamp > '2024-01-01'"
)
```

## Auto-Detection

SQLStream automatically detects JSON format:

```python
# By extension
query("data.json")    # → JSON Reader
query("logs.jsonl")   # → JSONL Reader

# By fragment
query("file#json")    # → Force JSON Reader
query("file#jsonl")   # → Force JSONL Reader

# By content (HTTP URLs)
query("https://api.example.com/data")  # Detects JSON from content
```

## Installation

JSON/JSONL support is included in the base installation:

```bash
pip install sqlstream
```

No additional dependencies required!

## Real-World Examples

### Example: API Response Processing

```python
# Process GitHub API response
url = "https://api.github.com/repos/owner/repo/issues"

results = query(url).sql("""
    SELECT 
        number,
        title,
        state,
        user.login as author
    FROM issues
    WHERE state = 'open'
    ORDER BY created_at DESC
    LIMIT 10
""")
```

### Example: Log Analysis

```python
# Analyze application logs (JSONL format)
results = query("app.jsonl").sql("""
    SELECT 
        level,
        COUNT(*) as count
    FROM logs
    WHERE timestamp >= '2024-01-01'
    GROUP BY level
    ORDER BY count DESC
""")
```

### Example: Nested E-commerce Data

```python
# Flatten order items from all customers
data = {
    "customers": [
        {
            "id": 1,
            "name": "Alice",
            "orders": [
                {"id": "o1", "total": 100},
                {"id": "o2", "total": 200}
            ]
        },
        {
            "id": 2,
            "name": "Bob",
            "orders": [
                {"id": "o3", "total": 150}
            ]
        }
    ]
}

# Get all orders across all customers
results = query("sales.json#json:customers[].orders").sql("""
    SELECT 
        id,
        total
    FROM orders
    WHERE total > 100
""")
```

## Limitations

- Single `[]` operator per path (e.g., `a[].b[].c` not supported)
- JSON files loaded into memory (use JSONL for large datasets)
- Nested path syntax only for JSON (not JSONL)

## See Also

- [URL Fragment Reference](../reference/url-fragments.md)
- [Advanced Formats](../features/advanced-formats.md)
- [Data Sources](../features/data-sources.md)
