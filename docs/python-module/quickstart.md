# Python Module Quick Start

Get started with SQLStream as a Python library in 5 minutes!

---

## Step 1: Install SQLStream

```bash
pip install "sqlstream[all]"
```

For minimal installation (CSV only):
```bash
pip install sqlstream
```

---

## Step 2: Your First Query

Create a sample CSV file or use an existing one:

```python
# Create sample data
import csv

with open("employees.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(["id", "name", "department", "salary"])
    writer.writerow([1, "Alice", "Engineering", 95000])
    writer.writerow([2, "Bob", "Sales", 75000])
    writer.writerow([3, "Charlie", "Engineering", 105000])
```

Now query it with SQLStream:

```python
from sqlstream import query

# Query with explicit source
results = query("employees.csv").sql("""
    SELECT name, salary
    FROM employees
    WHERE department = 'Engineering'
""")

# Print results
for row in results:
    print(f"{row['name']}: ${row['salary']:,}")
```

Output:
```
Alice: $95,000
Charlie: $105,000
```

### Alternative: Inline File Paths

You can also specify the file path directly in the SQL:

```python
from sqlstream import query

# Query with inline source (extracted from SQL)
results = query().sql("""
    SELECT name, salary
    FROM 'employees.csv'
    WHERE department = 'Engineering'
""")

for row in results:
    print(f"{row['name']}: ${row['salary']:,}")
```

---

## Step 3: Working with Results

### Iterate Over Rows (Lazy Evaluation)

```python
from sqlstream import query

results = query("employees.csv").sql("SELECT * FROM employees WHERE salary > 80000")

# Lazy iteration - rows are processed one at a time
for row in results:
    print(row)  # row is a dict
```

###Convert to List

```python
# Convert all results to a list (eager evaluation)
results_list = query("employees.csv").sql("SELECT * FROM employees").to_list()

print(f"Found {len(results_list)} employees")
```

### Access as DataFrame (with pandas)

```python
import pandas as pd
from sqlstream import query

results = query("employees.csv").sql("SELECT * FROM employees")

# Convert to pandas DataFrame
df = pd.DataFrame(results.to_list())
print(df)
```

---

## Step 4: Advanced Queries

### Aggregations

```python
from sqlstream import query

# Count employees by department
results = query("employees.csv").sql("""
    SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC
""")

for row in results:
    print(f"{row['department']}: {row['count']} employees, avg ${row['avg_salary']:,.0f}")
```

### JOINs Across Files

```python
from sqlstream import query

# Create orders data
import csv
with open("orders.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(["order_id", "employee_id", "amount"])
    writer.writerow([101, 1, 1500])
    writer.writerow([102, 2, 2300])
    writer.writerow([103, 1, 1800])

# Join employees and orders
results = query().sql("""
    SELECT e.name, SUM(o.amount) as total_sales
    FROM 'employees.csv' e
    JOIN 'orders.csv' o ON e.id = o.employee_id
    GROUP BY e.name
    ORDER BY total_sales DESC
""")

for row in results:
    print(f"{row['name']}: ${row['total_sales']:,}")
```

---

## Step 5: Multiple File Formats

### Query JSON Files

```python
from sqlstream import query

# Query a JSON file with nested data
results = query("users.json#json:data.users").sql("""
    SELECT name, email
    FROM users
    WHERE age > 25
""")

for row in results:
    print(row)
```

### Query Parquet Files

```python
from sqlstream import query

# Parquet files are optimized for column-oriented queries
results = query("sales.parquet").sql("""
    SELECT region, SUM(amount) as total
    FROM sales
    GROUP BY region
""")

for row in results:
    print(f"{row['region']}: ${row['total']:,}")
```

### Query HTML Tables

```python
from sqlstream import query

# Extract and query tables from HTML
results = query("report.html#html:0").sql("""
    SELECT * FROM report WHERE revenue > 100000
""")

for row in results:
    print(row)
```

---

## Step 6: Performance Optimization

### Use the Pandas Backend

For large files, use the pandas backend for 10-100x speedup:

```python
from sqlstream import query

# Specify backend explicitly
results = query("large_file.csv", backend="pandas").sql("""
    SELECT * FROM large_file WHERE amount > 1000
""")

for row in results:
    print(row)
```

### Use the DuckDB Backend

For complex SQL queries, use DuckDB for 100x+ speedup:

```python
from sqlstream import query

# DuckDB supports advanced SQL features
results = query("employees.csv", backend="duckdb").sql("""
    WITH ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank
        FROM employees
    )
    SELECT * FROM ranked WHERE rank = 1
""")

for row in results:
    print(row)
```

---

## Step 7: Error Handling

```python
from sqlstream import query

try:
    results = query("data.csv").sql("SELECT * FROM data WHERE invalid_column = 1")
    for row in results:
        print(row)
except Exception as e:
    print(f"Query error: {e}")
```

---

## Common Use Cases

### Data Exploration

```python
from sqlstream import query

# Count rows
count = query("data.csv").sql("SELECT COUNT(*) as count FROM data").to_list()[0]['count']
print(f"Total rows: {count}")

# Get unique values
unique_categories = query("data.csv").sql("SELECT DISTINCT category FROM data")
for row in unique_categories:
    print(row['category'])

# Summary statistics
stats = query("data.csv").sql("""
    SELECT
        MIN(price) as min_price,
        MAX(price) as max_price,
        AVG(price) as avg_price
    FROM data
""").to_list()[0]
print(stats)
```

### Data Cleaning

```python
from sqlstream import query
import csv

# Remove duplicates and filter nulls
results = query("messy_data.csv").sql("""
    SELECT DISTINCT *
    FROM messy_data
    WHERE name IS NOT NULL
      AND age > 0
    ORDER BY id
""")

# Export cleaned data
with open("clean_data.csv", "w") as f:
    rows = results.to_list()
    if rows:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
```

### ETL Pipeline

```python
from sqlstream import query

# Extract and Transform
result = query("customers.csv").sql("""
    SELECT
        c.name,
        c.email,
        COUNT(o.order_id) as total_orders,
        SUM(o.amount) as total_spent
    FROM customers c
    LEFT JOIN 'orders.csv' o ON c.id = o.customer_id
    GROUP BY c.name, c.email
    HAVING total_orders > 0
    ORDER BY total_spent DESC
""")

# Load - send to database, API, etc.
for row in result:
    # Process each customer
    print(f"{row['name']}: {row['total_orders']} orders, ${row['total_spent']:,}")
    # INSERT INTO database, POST to API, etc.
```

---

## What's Next?

- [Basic Usage](basic-usage.md) - Learn core Python API concepts
- [Advanced Patterns](advanced-patterns.md) - Custom readers, streaming, error handling
- [Examples](examples/simple-queries.md) - More real-world Python examples
- [API Reference](basic-usage.md) - Complete API documentation

---

## Need Help?

- 📖 [Full Documentation](../index.md)
- 🐛 [Report Issues](https://github.com/subhayu99/sqlstream/issues)
- 💬 [Discussions](https://github.com/subhayu99/sqlstream/discussions)
