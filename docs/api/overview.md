# Python API Overview

Use SQLStream programmatically in your Python code.

## Basic Usage

```python
from sqlstream import query

# Execute query with explicit source
results = query("data.csv").sql("SELECT * FROM data WHERE age > 25")

# Execute query with inline source (extracted from SQL)
results = query().sql("SELECT * FROM 'data.csv' WHERE age > 25")

# Iterate (lazy)
for row in results:
    print(row)

# Or convert to list (eager)
results_list = query().sql("SELECT * FROM 'data.csv'").to_list()
```

---

## Backend Selection

SQLStream offers three execution backends with different performance characteristics and SQL support:

### Auto Backend (Default)

```python
from sqlstream import query

# Automatically selects: pandas > duckdb > python
results = query("data.csv").sql("SELECT * FROM data")
```

**Backend priority**:
1. **Pandas** (if installed) - Fast execution, basic SQL
2. **DuckDB** (if pandas not available, duckdb installed) - Full SQL, maximum performance
3. **Python** (fallback) - Educational, Volcano model

### Explicit Backend

```python
# Force DuckDB backend (full SQL support)
results = query("data.csv").sql("""
    SELECT 
        department,
        AVG(salary) as avg_salary,
        ROW_NUMBER() OVER (ORDER BY AVG(salary) DESC) as rank
    FROM data
    GROUP BY department
""", backend="duckdb")

# Force Pandas backend (10-100x faster than Python)
results = query("data.csv").sql(
    "SELECT * FROM data WHERE age > 25", 
    backend="pandas"
)

# Force Python backend (educational)
results = query("data.csv").sql(
    "SELECT * FROM data WHERE age > 25", 
    backend="python"
)
```

**When to use each backend**:
- **`backend="duckdb"`** - Complex SQL (CTEs, window functions), production workloads, maximum performance
- **`backend="pandas"`** - Balance of speed and simplicity, basic queries
- **`backend="python"`** - Learning query execution, understanding Volcano model
- **`backend="auto"`** (default) - Let SQLStream choose the best available

---

## Working with Results

### Lazy Iteration (Memory-Efficient)

```python
# Process rows one at a time (doesn't load all into memory)
results = query("large_file.csv").sql("SELECT * FROM large_file")

for row in results:
    # Each row is a dictionary
    print(f"{row['name']}: {row['value']}")
```

### Eager Loading

```python
# Load all results into a list
results_list = query("data.csv").sql("SELECT * FROM data").to_list()

# Now you can access by index, slice, etc.
first_row = results_list[0]
top_10 = results_list[:10]
```

### Integration with Other Libraries

```python
# Convert to pandas DataFrame
import pandas as pd

results = query("data.csv").sql("SELECT * FROM data")
df = pd.DataFrame(results.to_list())

# Convert to JSON
import json

results = query("data.csv").sql("SELECT * FROM data")
json_str = json.dumps(results.to_list(), indent=2)

# Write to CSV
import csv

results = query("data.csv").sql("SELECT * FROM data")
with open("output.csv", "w", newline="") as f:
    if results.to_list():
        writer = csv.DictWriter(f, fieldnames=results.to_list()[0].keys())
        writer.writeheader()
        writer.writerows(results.to_list())
```

---

## Multiple File Queries

### Inline File Paths (Recommended)

```python
# JOIN multiple files
results = query().sql("""
    SELECT 
        e.name, 
        e.salary, 
        d.department_name
    FROM 'employees.csv' e
    JOIN 'departments.csv' d ON e.dept_id = d.id
    WHERE e.salary > 75000
""")
```

### Mixed Sources

```python
# Query from different sources
results = query().sql("""
    SELECT 
        local.*, 
        remote.category
    FROM 'data/local.csv' local
    JOIN 's3://bucket/remote.parquet' remote 
        ON local.id = remote.id
""", backend="duckdb")
```

---

## Advanced Usage

### Query Explain Plan

```python
# View query execution plan
results = query("data.csv").sql(
    "SELECT * FROM data WHERE age > 25", 
    backend="python"
)

# Get explain plan
plan = results.explain()
print(plan)
```

**Example output**:
```
Filter (predicate: age > 25)
└─ Scan (source: data.csv, columns: ['name', 'age', 'salary'])
```

### S3 and HTTP Sources

```python
# Query S3 data
results = query("s3://my-bucket/data.parquet").sql("""
    SELECT * FROM data 
    WHERE date > '2024-01-01'
    LIMIT 100
""", backend="duckdb")

# Query HTTP URL
results = query("https://example.com/data.csv").sql("""
    SELECT category, COUNT(*) as count
    FROM data
    GROUP BY category
""")
```

### Complex Analytics

```python
# Multi-step analytics with DuckDB
results = query().sql("""
    WITH monthly_sales AS (
        SELECT 
            DATE_TRUNC('month', sale_date) as month,
            product_id,
            SUM(amount) as total_sales
        FROM 's3://bucket/sales.parquet'
        GROUP BY 1, 2
    ),
    ranked AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY month 
                ORDER BY total_sales DESC
            ) as rank
        FROM monthly_sales
    )
    SELECT * FROM ranked WHERE rank <= 10
""", backend="duckdb")

for row in results:
    print(f"{row['month']}: {row['product_id']} - ${row['total_sales']:,.2f}")
```

---

## Error Handling

```python
from sqlstream import query

try:
    results = query("data.csv").sql(
        "SELECT * FROM data WHERE invalid_column > 10"
    )
    for row in results:
        print(row)
except FileNotFoundError:
    print("File not found")
except ValueError as e:
    print(f"Query error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

---

## Best Practices

### 1. Use Lazy Iteration for Large Files

```python
# ✅ Good: Memory-efficient
for row in query("huge.csv").sql("SELECT * FROM huge"):
    process(row)

# ❌ Bad: Loads everything into memory
all_rows = query("huge.csv").sql("SELECT * FROM huge").to_list()
```

### 2. Choose the Right Backend

```python
# ✅ Good: DuckDB for complex SQL
results = query().sql("""
    WITH stats AS (SELECT AVG(value) as avg FROM 'data.csv')
    SELECT * FROM 'data.csv' WHERE value > (SELECT avg FROM stats)
""", backend="duckdb")

# ❌ Bad: Python backend doesn't support CTEs
# results = query().sql("WITH ...", backend="python")  # Will fail
```

### 3. Use Inline Paths for Multi-File Queries

```python
# ✅ Good: Clear and simple
results = query().sql("""
    SELECT * FROM 'file1.csv' a 
    JOIN 'file2.csv' b ON a.id = b.id
""")

# ❌ Less convenient: Multiple query objects
# q1 = query("file1.csv")
# q2 = query("file2.csv")
# ...
```

---

## Next Steps

- [Advanced Python Guides](guides/advanced.md) - In-depth Python API usage
- [SQL Support](../features/sql-support.md) - Supported SQL syntax
- [Backend Comparison](../features/duckdb-backend.md) - Choose the right backend
- [Examples](../examples/basic-queries.md) - Real-world code examples

