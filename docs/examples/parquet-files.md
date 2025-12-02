# Querying Parquet Files

Parquet is a column-oriented binary file format that provides high performance and efficient compression. SQLStream supports querying Parquet files directly, often with significant performance benefits due to column pruning and predicate pushdown.

## Basic Usage

You can query a remote Parquet file just like a CSV.

**Data Source**: [sales.parquet](https://github.com/subhayu99/sqlstream/raw/main/examples/sales.parquet)

### Example 1: Basic Select

```bash
sqlstream query "SELECT * FROM 'https://github.com/subhayu99/sqlstream/raw/main/examples/sales.parquet' LIMIT 5"
```

### Example 2: Aggregating Sales by Region

SQLStream (and the underlying engines) can optimize this query by only reading the `region` and `amount` columns.

```python
from sqlstream import query

url = "https://github.com/subhayu99/sqlstream/raw/main/examples/sales.parquet"

results = query().sql(f"""
    SELECT region, SUM(amount) as total_sales
    FROM '{url}'
    GROUP BY region
    ORDER BY total_sales DESC
""")

print(results.to_list())
```

## Mixing Formats (CSV + Parquet)

You can join data across different file formats. Here we join a CSV of employees with a Parquet file of sales transactions.

**Data**:

- Employees (CSV): [employees.csv](https://github.com/subhayu99/sqlstream/raw/main/examples/employees.csv)
- Sales (Parquet): [sales.parquet](https://github.com/subhayu99/sqlstream/raw/main/examples/sales.parquet)

```python
from sqlstream import query

base_url = "https://github.com/subhayu99/sqlstream/raw/main/examples"

# Find top performing employees by sales amount
sql = f"""
    SELECT 
        e.name, 
        e.dept_id, 
        SUM(s.amount) as total_revenue
    FROM '{base_url}/employees.csv' e
    JOIN '{base_url}/sales.parquet' s ON e.id = s.emp_id
    GROUP BY e.name, e.dept_id
    ORDER BY total_revenue DESC
    LIMIT 5
"""

results = query().sql(sql, backend='duckdb')
```
