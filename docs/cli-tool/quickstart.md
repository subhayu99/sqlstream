# CLI Tool Quick Start

Get started with the `sqlstream` command-line tool in 5 minutes!

---

## Step 1: Install SQLStream

```bash
# Using uv (recommended)
uv tool install "sqlstream[all]"

# Or using pip
pip install "sqlstream[all]"
```

Verify installation:
```bash
sqlstream --version
```

---

## Step 2: Create Sample Data

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

### Basic SELECT

```bash
$ sqlstream query employees.csv "SELECT * FROM employees"
```

Output:
```
┌────┬─────────┬─────────────┬────────┬────────────┐
│ id │ name    │ department  │ salary │ hire_date  │
├────┼─────────┼─────────────┼────────┼────────────┤
│  1 │ Alice   │ Engineering │  95000 │ 2020-01-15 │
│  2 │ Bob     │ Sales       │  75000 │ 2019-06-01 │
│  3 │ Charlie │ Engineering │ 105000 │ 2018-03-20 │
│  4 │ Diana   │ Marketing   │  68000 │ 2021-02-14 │
│  5 │ Eve     │ Sales       │  82000 │ 2020-09-10 │
└────┴─────────┴─────────────┴────────┴────────────┘
```

### Filter with WHERE

```bash
$ sqlstream query employees.csv "SELECT name, salary FROM employees WHERE department = 'Engineering'"
```

### Inline File Paths

You can omit the source file if it's in the SQL:

```bash
$ sqlstream query "SELECT * FROM 'employees.csv' WHERE salary > 80000"
```

---

## Step 4: Output Formats

### JSON Output

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

### CSV Output

```bash
$ sqlstream query employees.csv "SELECT name, department FROM employees" --format csv
```

```csv
name,department
Alice,Engineering
Bob,Sales
Charlie,Engineering
Diana,Marketing
Eve,Sales
```

### Markdown Output

```bash
$ sqlstream query employees.csv "SELECT * FROM employees LIMIT 2" --format markdown
```

```markdown
| id | name  | department  | salary | hire_date  |
|----|-------|-------------|--------|------------|
| 1  | Alice | Engineering | 95000  | 2020-01-15 |
| 2  | Bob   | Sales       | 75000  | 2019-06-01 |
```

---

## Step 5: Aggregations

```bash
# Count by department
$ sqlstream query employees.csv "
  SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
  FROM employees
  GROUP BY department
  ORDER BY avg_salary DESC
"
```

Output:
```
┌─────────────┬───────┬────────────┐
│ department  │ count │ avg_salary │
├─────────────┼───────┼────────────┤
│ Engineering │     2 │   100000.0 │
│ Sales       │     2 │    78500.0 │
│ Marketing   │     1 │    68000.0 │
└─────────────┴───────┴────────────┘
```

---

## Step 6: JOIN Multiple Files

Create an orders file:

```bash
cat > orders.csv << EOF
order_id,employee_id,amount
101,1,1500
102,2,2300
103,1,1800
104,3,2100
EOF
```

Join the files:

```bash
$ sqlstream query "
  SELECT e.name, SUM(o.amount) as total_sales
  FROM 'employees.csv' e
  JOIN 'orders.csv' o ON e.id = o.employee_id
  GROUP BY e.name
  ORDER BY total_sales DESC
"
```

---

## Step 7: Performance Options

### Use Pandas Backend

For large files (>100K rows):

```bash
$ sqlstream query large_file.csv "SELECT * FROM large_file WHERE amount > 1000" --backend pandas
```

### Use DuckDB Backend

For complex SQL (window functions, CTEs):

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

### Show Execution Time

```bash
$ sqlstream query employees.csv "SELECT * FROM employees" --time
```

---

## Step 8: Query Different Formats

### JSON Files

```bash
$ sqlstream query "users.json#json:data.users" "SELECT name, email FROM users WHERE age > 25"
```

### HTML Tables

```bash
$ sqlstream query "report.html#html:0" "SELECT * FROM report WHERE revenue > 100000"
```

### Markdown Tables

```bash
$ sqlstream query "README.md#markdown:1" "SELECT * FROM readme"
```

### Parquet Files

```bash
$ sqlstream query "data.parquet" "SELECT * FROM data WHERE date > '2024-01-01'"
```

### S3 Files

```bash
$ sqlstream query "SELECT * FROM 's3://my-bucket/data.parquet' WHERE date > '2024-01-01'"
```

---

## Common Command-Line Patterns

### Pipe to Other Tools

```bash
# Pipe to jq for JSON processing
$ sqlstream query employees.csv "SELECT * FROM employees" --format json | jq '.[] | select(.salary > 80000)'

# Pipe to grep
$ sqlstream query employees.csv "SELECT * FROM employees" --format csv | grep Engineering

# Pipe to wc for counting
$ sqlstream query employees.csv "SELECT * FROM employees" --format csv | wc -l
```

### Save Results to File

```bash
# Save as CSV
$ sqlstream query employees.csv "SELECT * FROM employees WHERE salary > 80000" --format csv > high_earners.csv

# Save as JSON
$ sqlstream query employees.csv "SELECT * FROM employees" --format json > employees.json
```

### Use in Scripts

```bash
#!/bin/bash

# Query and process results
RESULT=$(sqlstream query employees.csv "SELECT COUNT(*) as count FROM employees WHERE department = 'Engineering'" --format json)
COUNT=$(echo "$RESULT" | jq -r '.[0].count')

if [ "$COUNT" -gt 5 ]; then
    echo "Engineering team is large: $COUNT members"
else
    echo "Engineering team is small: $COUNT members"
fi
```

### Exit Codes

```bash
# Check if query succeeded
if sqlstream query employees.csv "SELECT * FROM employees" > /dev/null 2>&1; then
    echo "Query succeeded"
else
    echo "Query failed"
    exit 1
fi
```

---

## What's Next?

- [Query Command Reference](query-command.md) - All command-line options
- [Scripting Patterns](scripting-patterns.md) - Automation, CI/CD, error handling
- [Output Formats](output-formats.md) - Complete format guide
- [Examples](examples/bash-scripts.md) - Real-world CLI examples

---

## Need Help?

```bash
# Show help
$ sqlstream --help
$ sqlstream query --help

# Check version
$ sqlstream --version
```

- 📖 [Full Documentation](../index.md)
- 🐛 [Report Issues](https://github.com/subhayu99/sqlstream/issues)
- 💬 [Discussions](https://github.com/subhayu99/sqlstream/discussions)
