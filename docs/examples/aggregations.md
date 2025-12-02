# Aggregations

Compute statistics, totals, and averages using standard SQL aggregate functions.

**Data Source**: [sales.parquet](https://github.com/subhayu99/sqlstream/raw/main/examples/sales.parquet)

## Basic Grouping

Calculate total sales volume by region.

```python
from sqlstream import query

url = "https://github.com/subhayu99/sqlstream/raw/main/examples/sales.parquet"

results = query().sql(f"""
    SELECT 
        region, 
        COUNT(*) as transaction_count, 
        SUM(amount) as total_revenue,
        AVG(amount) as average_sale
    FROM '{url}'
    GROUP BY region
    ORDER BY total_revenue DESC
""")
```

## Filtering Aggregates

Find regions with more than 20 transactions (simulating a `HAVING` clause using a subquery or CTE with DuckDB backend).

```python
# Using DuckDB backend for advanced features like CTEs
results = query().sql(f"""
    WITH RegionalStats AS (
        SELECT region, COUNT(*) as cnt, SUM(amount) as revenue
        FROM '{url}'
        GROUP BY region
    )
    SELECT * FROM RegionalStats WHERE cnt > 20
""", backend="duckdb")
```
