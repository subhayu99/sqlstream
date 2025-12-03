# Quick Start

Get up and running with SQLStream in 5 minutes!

---

## Step 1: Install SQLStream

```bash
pip install "sqlstream[all]"
```

---

## Step 2: Create Sample Data

Create a sample CSV file:

```bash
cat > employees.csv << EOF
id,name,department,salary,hire_date
1,Alice,Engineering,95000,2020-01-15
2,Bob,Sales,75000,2019-06-01
3,Charlie,Engineering,105000,2018-03-20
4,Diana,Marketing,68000,2021-02-14
5,Eve,Sales,82000,2020-09-10
EOF
```

---

## Step 3: Your First Query

### CLI Usage

```bash
# Select all rows
$ sqlstream query employees.csv "SELECT * FROM employees"

# Filter by department
$ sqlstream query employees.csv "SELECT name, salary FROM employees WHERE department = 'Engineering'"

# Sort by salary
$ sqlstream query employees.csv "SELECT * FROM employees ORDER BY salary DESC LIMIT 3"

# Query with inline file path (no source argument needed)
$ sqlstream query "SELECT * FROM 'employees.csv' WHERE salary > 80000"
```

###  Python API

```python
from sqlstream import query

# Simple query with explicit source
results = query("employees.csv").sql("SELECT * FROM employees WHERE salary > 80000")

# Query with inline source (extracted from SQL)
results = query().sql("SELECT * FROM 'employees.csv' WHERE salary > 80000")

# Print results
for row in results:
    print(f"{row['name']}: ${row['salary']:,}")
```

Output:
```
Alice: $95,000
Charlie: $105,000
Eve: $82,000
```

---

## Step 4: Advanced Features

### Aggregations

```bash
# Count employees by department
$ sqlstream query employees.csv "SELECT department, COUNT(*) AS count FROM employees GROUP BY department"
```

Output:
```
┌─────────────┬───────┐
│ department  │ count │
├─────────────┼───────┤
│ Engineering │     2 │
│ Sales       │     2 │
│ Marketing   │     1 │
└─────────────┴───────┘
```

### Joins

Create another file for orders:

```bash
cat > orders.csv << EOF
order_id,employee_id,amount
101,1,1500
102,2,2300
103,1,1800
104,3,2100
EOF
```

Join the two files:

```bash
$ sqlstream query "SELECT e.name, o.amount FROM 'employees.csv' e JOIN 'orders.csv' o ON e.id = o.employee_id"
```

Output:
```
┌─────────┬────────┐
│ name    │ amount │
├─────────┼────────┤
│ Alice   │   1500 │
│ Bob     │   2300 │
│ Alice   │   1800 │
│ Charlie │   2100 │
└─────────┴────────┘
```

### Output Formats

=== "Table (default)"

    ```bash
    $ sqlstream query employees.csv "SELECT * FROM employees LIMIT 2"
    ```

    ```
    ┌────┬───────┬─────────────┬────────┬────────────┐
    │ id │ name  │ department  │ salary │ hire_date  │
    ├────┼───────┼─────────────┼────────┼────────────┤
    │  1 │ Alice │ Engineering │  95000 │ 2020-01-15 │
    │  2 │ Bob   │ Sales       │  75000 │ 2019-06-01 │
    └────┴───────┴─────────────┴────────┴────────────┘
    ```

=== "JSON"

    ```bash
    $ sqlstream query employees.csv "SELECT * FROM employees LIMIT 2" --format json
    ```

    ```json
    [
      {
        "id": 1,
        "name": "Alice",
        "department": "Engineering",
        "salary": 95000,
        "hire_date": "2020-01-15"
      },
      {
        "id": 2,
        "name": "Bob",
        "department": "Sales",
        "salary": 75000,
        "hire_date": "2019-06-01"
      }
    ]
    ```

=== "CSV"

    ```bash
    $ sqlstream query employees.csv "SELECT * FROM employees LIMIT 2" --format csv
    ```

    ```csv
    id,name,department,salary,hire_date
    1,Alice,Engineering,95000,2020-01-15
    2,Bob,Sales,75000,2019-06-01
    ```

---

## Step 5: Performance Boost

For large files, use the pandas backend:

```bash
$ sqlstream query large_file.csv "SELECT * FROM large_file WHERE amount > 1000" --backend pandas
```

Performance comparison:

| Rows | Python Backend | Pandas Backend | Speedup |
|------|---------------|----------------|---------|
| 10K | 0.5s | 0.05s | **10x** |
| 100K | 5.2s | 0.15s | **35x** |
| 1M | 52s | 0.8s | **65x** |

For complex SQL queries, use the DuckDB backend:

```bash
$ sqlstream query "
    WITH ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank
        FROM 'employees.csv'
    )
    SELECT * FROM ranked WHERE rank = 1
" --backend duckdb
```

---

## Step 6: Query Different Data Formats

SQLStream supports multiple file formats beyond CSV:

### JSON Files

```bash
# Query a simple JSON array
$ sqlstream query "users.json" "SELECT name, email FROM users"

# Query nested JSON with path syntax
$ sqlstream query "api.json#json:data.users" "SELECT name FROM users WHERE age > 25"
```

### HTML Tables

Extract and query tables from HTML documents:

```bash
# Query first table in HTML file
$ sqlstream query "report.html" "SELECT * FROM report WHERE revenue > 100000"

# Query specific table by index
$ sqlstream query "report.html#html:1" "SELECT * FROM report"

# Query tables from web pages
$ sqlstream query "https://example.com/data.html" "SELECT * FROM data"
```

### Markdown Tables

Parse tables from Markdown documents:

```bash
# Query a table from README
$ sqlstream query "README.md" "SELECT * FROM readme"

# Query specific table by index
$ sqlstream query "docs/api.md#markdown:2" "SELECT * FROM api"
```

### Parquet Files

High-performance columnar format:

```bash
$ sqlstream query "data.parquet" "SELECT * FROM data WHERE date > '2024-01-01'"
```

---

## Step 7: Interactive Shell

For a full interactive experience, use the shell command:

```bash
$ sqlstream shell employees.csv
```

This launches a powerful TUI (Terminal User Interface) with:

- **Query Editor**: Multi-line editing with syntax highlighting and **multiple tabs** (`Ctrl+T` to add, `Ctrl+W` to close).
- **Word Deletion**: Use `Ctrl+Delete` and `Ctrl+Backspace` for fast editing.
- **Results Viewer**: Scrollable table with pagination.
- **Sidebar**: Toggle between **Schema** and **Files** browser (`F2` to toggle, `Ctrl+O` to open files).
- **Backend Toggle**: Press `F5` or `Ctrl+B` to cycle through backends (auto/duckdb/pandas/python).
- **Save Progress**: Save your work with `Ctrl+S`.
- **Export**: Press `Ctrl+X` to export results to CSV/JSON/Parquet.
- **History**: Use `Ctrl+Up`/`Down` to navigate previous queries.
- **State Persistence**: Your open tabs and queries are saved automatically on exit.

---

## Common Workflows

### Data Exploration

```bash
# Check file structure
$ head -5 data.csv

# Count rows
$ sqlstream query data.csv "SELECT COUNT(*) FROM data"

# Show unique values
$ sqlstream query data.csv "SELECT DISTINCT category FROM data"

# Summary statistics
$ sqlstream query data.csv "SELECT MIN(price), MAX(price), AVG(price) FROM data"
```

### Data Cleaning

```python
from sqlstream import query

# Remove duplicates and filter nulls
results = query("messy_data.csv").sql("""
    SELECT DISTINCT *
    FROM messy_data
    WHERE name IS NOT NULL
      AND age > 0
    ORDER BY id
""")

# Export cleaned data
import csv
with open("clean_data.csv", "w") as f:
    writer = csv.DictWriter(f, fieldnames=results.to_list()[0].keys())
    writer.writeheader()
    writer.writerows(results.to_list())
```

### ETL Pipeline

```python
from sqlstream import query

# Extract
customers = query("customers.csv")
orders = query("orders.csv")

# Transform: Calculate total orders per customer
result = query("customers.csv").sql("""
    SELECT c.name, COUNT(o.order_id) as total_orders
    FROM customers c
    JOIN orders o ON c.id = o.customer_id
    GROUP BY c.name
    ORDER BY total_orders DESC
""")

# Load
for row in result:
    # Send to database, API, etc.
    print(row)
```

---

## What's Next?

Now that you're familiar with the basics, explore:

- [Core Concepts](core-concepts.md) - Understand how SQLStream works
- [SQL Support](../features/sql-support.md) - Learn all supported SQL features
- [CLI Reference](../cli/query-command.md) - Master the command-line interface
- [Python API](../api/overview.md) - Deep dive into the programmatic API
- [Examples](../examples/basic-queries.md) - More real-world examples

---

## Need Help?

- 📖 [Documentation](../index.md)
- 🐛 [Report Issues](https://github.com/subhayu99/sqlstream/issues)
- 💬 [Discussions](https://github.com/subhayu99/sqlstream/discussions)
