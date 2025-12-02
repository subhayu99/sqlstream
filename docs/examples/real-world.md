# Real-World Scenario: Sales Analytics

This example demonstrates the power of SQLStream by joining three different file formats located on the web into a single result set.

## The Goal
Generate a "Department Sales Performance" report by combining:
1. **Employees** (CSV) - Who made the sale?
2. **Departments** (CSV) - Which department are they in?
3. **Sales** (Parquet) - Transaction details.

## The Code

```python
from sqlstream import query

# Base URL for raw files
base = "https://github.com/subhayu99/sqlstream/raw/main/examples"

# 1. Join Sales (Parquet) -> Employees (CSV) -> Departments (CSV)
# 2. Aggregate revenue by Department Name
sql = f"""
    SELECT 
        d.dept_name,
        COUNT(s.transaction_id) as sales_count,
        SUM(s.amount) as total_revenue,
        ROUND(AVG(s.amount), 2) as avg_ticket_size
    FROM '{base}/sales.parquet' s
    JOIN '{base}/employees.csv' e ON s.emp_id = e.id
    JOIN '{base}/departments.csv' d ON e.dept_id = d.dept_id
    GROUP BY d.dept_name
    ORDER BY total_revenue DESC
"""

print("Running Federation Query...")
results = query().sql(sql, backend="duckdb")

# Output results
for row in results:
    print(f"{row['dept_name']}: ${row['total_revenue']} ({row['sales_count']} sales)")
```

## Advanced: Adding JSON Product Data

Let's verify inventory levels for items sold in the "North" region.

```python
# Join Sales (Parquet) -> Products (JSON)
sql = f"""
    SELECT 
        p.name as product_name,
        p.stock as current_stock,
        SUM(s.amount) as sales_in_north
    FROM '{base}/sales.parquet' s
    JOIN '{base}/products.json' p ON s.product_id = p.id
    WHERE s.region = 'North'
    GROUP BY p.name, p.stock
    ORDER BY sales_in_north DESC
    LIMIT 3
"""

results = query().sql(sql, backend="duckdb")
```
