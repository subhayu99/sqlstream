# Simple Python Query Examples

Learn basic patterns for using SQLStream in Python code.

---

## Basic SELECT

```python
from sqlstream import query

# Query a CSV file
results = query("data.csv").sql("SELECT * FROM data WHERE age > 25")

# Iterate over results
for row in results:
    print(row)
```

---

## Filtering Data

```python
from sqlstream import query

# Filter with WHERE clause
results = query("employees.csv").sql("""
    SELECT name, salary
    FROM employees
    WHERE department = 'Engineering'
    AND salary > 80000
""")

for row in results:
    print(f"{row['name']}: ${row['salary']:,}")
```

---

## Sorting Results

```python
from sqlstream import query

# Order by salary descending
results = query("employees.csv").sql("""
    SELECT name, salary
    FROM employees
    ORDER BY salary DESC
    LIMIT 10
""")

print("Top 10 earners:")
for i, row in enumerate(results, 1):
    print(f"{i}. {row['name']}: ${row['salary']:,}")
```

---

## Converting to List

```python
from sqlstream import query

# Get all results as a list
results = query("data.csv").sql("SELECT * FROM data").to_list()

print(f"Found {len(results)} rows")

# Access by index
first_row = results[0]
print(first_row)
```

---

## Working with Pandas

```python
from sqlstream import query
import pandas as pd

# Query with SQLStream, analyze with pandas
results = query("sales.csv").sql("SELECT * FROM sales WHERE amount > 100")
df = pd.DataFrame(results.to_list())

# Pandas analysis
print(df.describe())
print(df.groupby('category')['amount'].sum())
```

---

## Multiple Queries

```python
from sqlstream import query

# Run multiple queries
total_count = query("data.csv").sql("SELECT COUNT(*) as count FROM data").to_list()[0]['count']
avg_value = query("data.csv").sql("SELECT AVG(value) as avg FROM data").to_list()[0]['avg']

print(f"Total rows: {total_count}")
print(f"Average value: {avg_value:.2f}")
```

---

## Inline File Paths

```python
from sqlstream import query

# Specify file in SQL (no source argument needed)
results = query().sql("""
    SELECT * FROM 'data.csv'
    WHERE date > '2024-01-01'
""")

for row in results:
    print(row)
```

---

## Different File Formats

```python
from sqlstream import query

# CSV
csv_results = query("data.csv").sql("SELECT * FROM data")

# JSON
json_results = query("users.json").sql("SELECT * FROM users WHERE active = true")

# Parquet
parquet_results = query("sales.parquet").sql("SELECT * FROM sales")

# HTML table
html_results = query("report.html#html:0").sql("SELECT * FROM report")
```

---

## Error Handling

```python
from sqlstream import query

try:
    results = query("data.csv").sql("SELECT * FROM data WHERE invalid_column = 1")
    for row in results:
        print(row)
except Exception as e:
    print(f"Query error: {e}")
    # Handle error appropriately
```

---

## Backend Selection

```python
from sqlstream import query

# Use pandas for better performance
results = query("large_file.csv", backend="pandas").sql("""
    SELECT * FROM large_file WHERE amount > 1000
""")

# Use DuckDB for complex SQL
results = query("data.csv", backend="duckdb").sql("""
    WITH ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) as rank
        FROM data
    )
    SELECT * FROM ranked WHERE rank <= 5
""")
```

---

## See Also

- [Python Quickstart](../quickstart.md) - Get started
- [Basic Usage](../basic-usage.md) - Core concepts
- [Advanced Patterns](../advanced-patterns.md) - Advanced usage
