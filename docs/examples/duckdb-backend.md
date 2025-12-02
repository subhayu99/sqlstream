# DuckDB Backend

SQLStream supports DuckDB as an execution engine. This unlocks advanced SQL features like Window Functions, CTEs, and complex joins that simple in-memory processing cannot handle.

**Data Source**: [employees.csv](https://github.com/subhayu99/sqlstream/raw/main/examples/employees.csv)

## Window Functions

Use `ROW_NUMBER()` to find the highest-paid employee in each department.

```python
from sqlstream import query

url = "https://github.com/subhayu99/sqlstream/raw/main/examples/employees.csv"

sql = f"""
    SELECT * FROM (
        SELECT 
            name, 
            dept_id, 
            salary,
            ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rank
        FROM '{url}'
    ) 
    WHERE rank = 1
"""

# Must specify backend="duckdb" or backend="auto"
results = query().sql(sql, backend="duckdb")
```

---

## Common Table Expressions (CTEs)

Simplify complex logic using `WITH` clauses.

```python
sql = f"""
    WITH HighEarners AS (
        SELECT * FROM '{url}' WHERE salary > 100000
    )
    SELECT dept_id, COUNT(*) as high_earner_count
    FROM HighEarners
    GROUP BY dept_id
"""
results = query().sql(sql, backend="duckdb")
```

---

## UNION and Set Operations

Combine data from multiple sources.

```python
from sqlstream import query

# Create contractor data
contractor_data = """
name,role,hourly_rate
Ian,Engineer,125
Jane,Designer,95
Kyle,Engineer,135
"""

with open("contractors_sample.csv", "w") as f:
    f.write(contractor_data)

# UNION example
results = query().sql("""
    -- Full-time employees
    SELECT 
        name,
        department as role_dept,
        salary / 2080 as hourly_equivalent,
        'Full-time' as employment_type
    FROM 'employees_sample.csv'
    WHERE department = 'Engineering'
    
    UNION ALL
    
    -- Contractors
    SELECT 
        name,
        role as role_dept,
        hourly_rate as hourly_equivalent,
        'Contractor' as employment_type
    FROM 'contractors_sample.csv'
    WHERE role = 'Engineer'
    
    ORDER BY hourly_equivalent DESC
""", backend="duckdb")

print("Engineering Workforce (Full-time + Contractors):")
print("-" * 80)
for row in results:
    print(f"{row['name']:10} | {row['role_dept']:12} | "
          f"${row['hourly_equivalent']:>6.2f}/hr | {row['employment_type']}")
```

---

## JOIN Multiple Files

Combine employees, departments, and location data.

```python
from sqlstream import query

# Create additional data files
dept_data = """
dept_id,department,budget
ENG,Engineering,500000
SAL,Sales,300000
MKT,Marketing,200000
"""

location_data = """
department,city,country
Engineering,San Francisco,USA
Sales,New York,USA
Marketing,Austin,USA
"""

with open("departments_sample.csv", "w") as f:
    f.write(dept_data)

with open("locations_sample.csv", "w") as f:
    f.write(location_data)

# Three-way JOIN
results = query().sql("""
    SELECT 
        e.name,
        e.department,
        e.salary,
        d.budget,
        l.city,
        l.country,
        ROUND(100.0 * e.salary / d.budget, 2) as pct_of_budget
    FROM 'employees_sample.csv' e
    JOIN 'departments_sample.csv' d 
        ON UPPER(SUBSTRING(e.department, 1, 3)) = d.dept_id
    JOIN 'locations_sample.csv' l 
        ON e.department = l.department
    ORDER BY e.department, e.salary DESC
""", backend="duckdb")

print("Employee Details with Department and Location:")
print("-" * 100)
for row in results:
    print(f"{row['name']:10} | {row['department']:12} | "
          f"${row['salary']:>7,} | {row['city']:15} | "
          f"{row['pct_of_budget']:>5.2f}% of budget")
```

---

## Best Practices

### 1. Use DuckDB for Complex Analytics

```python
# ✅ Good: DuckDB excels at this
results = query().sql("""
    WITH ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) as rn
        FROM 'sales.csv'
    )
    SELECT * FROM ranked WHERE rn <= 10
""", backend="duckdb")
```

### 2. Leverage Date Functions

```python
# ✅ Good: Extract insights from dates
results = query().sql("""
    SELECT 
        DATE_TRUNC('week', order_date) as week,
        COUNT(*) as orders,
        SUM(amount) as revenue
    FROM 'orders.csv'
    WHERE order_date >= CURRENT_DATE - INTERVAL 90 DAY
    GROUP BY 1
    ORDER BY 1
""", backend="duckdb")
```

### 3. Use Window Functions for Rankings

```python
# ✅ Good: Efficient ranking
results = query().sql("""
    SELECT 
        *,
        DENSE_RANK() OVER (ORDER BY score DESC) as rank
    FROM 'leaderboard.csv'
""", backend="duckdb")
```

---

## See Also

- [DuckDB Backend Guide](../features/duckdb-backend.md) - Complete backend documentation
- [SQL Support](../features/sql-support.md) - Supported SQL syntax
- [Python API](../api/overview.md) - Programmatic usage
- [Performance Tips](../features/pandas-backend.md) - Backend comparison
