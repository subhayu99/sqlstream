# DuckDB Backend Examples

Complete examples demonstrating the DuckDB backend's advanced SQL capabilities.

---

## Example 1: Window Functions

Calculate running totals and rankings for employee salaries.

```python
from sqlstream import query

# Create sample data
sample_data = """
name,department,salary,hire_date
Alice,Engineering,95000,2020-01-15
Bob,Engineering,85000,2021-03-10
Charlie,Engineering,105000,2019-06-20
Diana,Sales,75000,2020-11-05
Eve,Sales,82000,2021-01-12
Frank,Sales,68000,2022-02-18
Grace,Marketing,72000,2020-08-25
Henry,Marketing,78000,2021-07-14
"""

with open("employees_sample.csv", "w") as f:
    f.write(sample_data)

# Query with window functions
results = query().sql("""
    SELECT 
        name,
        department,
        salary,
        ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
        RANK() OVER (ORDER BY salary DESC) as overall_rank,
        AVG(salary) OVER (PARTITION BY department) as dept_avg_salary,
        SUM(salary) OVER (ORDER BY hire_date) as running_total
    FROM 'employees_sample.csv'
    ORDER BY department, dept_rank
""", backend="duckdb")

print("Employee Rankings by Department:")
print("-" * 80)
for row in results:
    print(f"{row['name']:10} | {row['department']:12} | "
          f"${row['salary']:,} | Dept Rank: {row['dept_rank']} | "
          f"Dept Avg: ${row['dept_avg_salary']:,.0f}")
```

**Output**:
```
Employee Rankings by Department:
--------------------------------------------------------------------------------
Charlie    | Engineering  | $105,000 | Dept Rank: 1 | Dept Avg: $95,000
Alice      | Engineering  | $95,000  | Dept Rank: 2 | Dept Avg: $95,000
Bob        | Engineering  | $85,000  | Dept Rank: 3 | Dept Avg: $95,000
Eve        | Sales        | $82,000  | Dept Rank: 1 | Dept Avg: $75,000
Diana      | Sales        | $75,000  | Dept Rank: 2 | Dept Avg: $75,000
Frank      | Sales        | $68,000  | Dept Rank: 3 | Dept Avg: $75,000
Henry      | Marketing    | $78,000  | Dept Rank: 1 | Dept Avg: $75,000
Grace      | Marketing    | $72,000  | Dept Rank: 2 | Dept Avg: $75,000
```

---

## Example 2: Common Table Expressions (CTEs)

Multi-step analytics using CTEs to find top performers.

```python
from sqlstream import query

results = query().sql("""
    WITH dept_stats AS (
        -- Calculate department statistics
        SELECT 
            department,
            AVG(salary) as avg_salary,
            MAX(salary) as max_salary,
            MIN(salary) as min_salary,
            COUNT(*) as employee_count
        FROM 'employees_sample.csv'
        GROUP BY department
    ),
    top_earners AS (
        -- Find employees earning above department average
        SELECT 
            e.name,
            e.department,
            e.salary,
            d.avg_salary
        FROM 'employees_sample.csv' e
        JOIN dept_stats d ON e.department = d.department
        WHERE e.salary > d.avg_salary
    )
    SELECT 
        name,
        department,
        salary,
        avg_salary,
        salary - avg_salary as above_average
    FROM top_earners
    ORDER BY above_average DESC
""", backend="duckdb")

print("Employees Earning Above Department Average:")
print("-" * 70)
for row in results:
    print(f"{row['name']:10} | {row['department']:12} | "
          f"${row['salary']:,} | +${row['above_average']:,.0f}")
```

---

## Example 3: Complex Analytics with S3 Data

Analyze sales data from S3 with window functions and CTEs.

```python
from sqlstream import query

# Simulate S3 sales data (in real use, this would be on S3)
sales_data = """
sale_date,product_id,category,amount,customer_id
2024-01-05,P001,Electronics,1200,C001
2024-01-07,P002,Electronics,850,C002
2024-01-10,P001,Electronics,1200,C003
2024-02-03,P003,Clothing,320,C001
2024-02-05,P002,Electronics,850,C004
2024-02-12,P004,Clothing,280,C002
2024-03-01,P001,Electronics,1200,C005
2024-03-10,P005,Home,540,C003
2024-03-15,P003,Clothing,320,C006
"""

with open("sales_sample.csv", "w") as f:
    f.write(sales_data)

# Complex analytics query
results = query().sql("""
    WITH monthly_sales AS (
        SELECT 
            DATE_TRUNC('month', CAST(sale_date AS DATE)) as month,
            product_id,
            category,
            SUM(amount) as total_sales,
            COUNT(*) as sale_count,
            COUNT(DISTINCT customer_id) as unique_customers
        FROM 'sales_sample.csv'
        GROUP BY 1, 2, 3
    ),
    ranked_products AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY month, category 
                ORDER BY total_sales DESC
            ) as category_rank,
            SUM(total_sales) OVER (
                PARTITION BY month
            ) as monthly_total
        FROM monthly_sales
    )
    SELECT 
        STRFTIME(month, '%Y-%m') as month,
        category,
        product_id,
        total_sales,
        sale_count,
        unique_customers,
        category_rank,
        ROUND(100.0 * total_sales / monthly_total, 2) as pct_of_month
    FROM ranked_products
    WHERE category_rank <= 2
    ORDER BY month, category, category_rank
""", backend="duckdb")

print("Top Products by Category Each Month:")
print("-" * 100)
print(f"{'Month':<8} | {'Category':<12} | {'Product':<8} | "
      f"{'Sales':>8} | {'Count':>5} | {'Customers':>9} | {'% of Month':>10}")
print("-" * 100)

for row in results:
    print(f"{row['month']:<8} | {row['category']:<12} | {row['product_id']:<8} | "
          f"${row['total_sales']:>7,} | {row['sale_count']:>5} | "
          f"{row['unique_customers']:>9} | {row['pct_of_month']:>9.1f}%")
```

---

## Example 4: Subqueries and Correlated Queries

Find employees earning above their department's median salary.

```python
from sqlstream import query

results = query().sql("""
    SELECT 
        e.name,
        e.department,
        e.salary,
        (
            SELECT MEDIAN(salary) 
            FROM 'employees_sample.csv' sub
            WHERE sub.department = e.department
        ) as dept_median,
        e.salary - (
            SELECT MEDIAN(salary) 
            FROM 'employees_sample.csv' sub
            WHERE sub.department = e.department
        ) as above_median
    FROM 'employees_sample.csv' e
    WHERE e.salary > (
        SELECT MEDIAN(salary) 
        FROM 'employees_sample.csv' sub
        WHERE sub.department = e.department
    )
    ORDER BY above_median DESC
""", backend="duckdb")

print("Employees Above Department Median:")
print("-" * 70)
for row in results:
    print(f"{row['name']:10} | {row['department']:12} | "
          f"${row['salary']:,} (Median: ${row['dept_median']:,.0f}, "
          f"+${row['above_median']:,.0f})")
```

---

## Example 5: String and Date Functions

Advanced text processing and date manipulation.

```python
from sqlstream import query

results = query().sql("""
    SELECT 
        name,
        UPPER(SUBSTRING(name, 1, 1)) || LOWER(SUBSTRING(name, 2)) as formatted_name,
        department,
        UPPER(department) as dept_code,
        CONCAT(SUBSTRING(name, 1, 1), SUBSTRING(department, 1, 3)) as employee_code,
        hire_date,
        EXTRACT(YEAR FROM CAST(hire_date AS DATE)) as hire_year,
        EXTRACT(MONTH FROM CAST(hire_date AS DATE)) as hire_month,
        DATE_DIFF('day', CAST(hire_date AS DATE), CURRENT_DATE) as days_employed,
        ROUND(DATE_DIFF('day', CAST(hire_date AS DATE), CURRENT_DATE) / 365.25, 1) as years_employed,
        CASE 
            WHEN DATE_DIFF('year', CAST(hire_date AS DATE), CURRENT_DATE) >= 3 
            THEN 'Senior'
            WHEN DATE_DIFF('year', CAST(hire_date AS DATE), CURRENT_DATE) >= 1 
            THEN 'Intermediate'
            ELSE 'Junior'
        END as seniority_level
    FROM 'employees_sample.csv'
    ORDER BY hire_date
""", backend="duckdb")

print("Employee Details with Computed Fields:")
print("-" * 100)
for row in results:
    print(f"{row['employee_code']:8} | {row['formatted_name']:10} | "
          f"{row['dept_code']:12} | Hired: {row['hire_year']}-{row['hire_month']:02d} | "
          f"{row['years_employed']:4.1f} yrs | {row['seniority_level']}")
```

---

## Example 6: Statistical Functions

Calculate comprehensive statistics for salary by department.

```python
from sqlstream import query

results = query().sql("""
    SELECT 
        department,
        COUNT(*) as employee_count,
        MIN(salary) as min_salary,
        MAX(salary) as max_salary,
        AVG(salary) as mean_salary,
        MEDIAN(salary) as median_salary,
        STDDEV(salary) as salary_stddev,
        VARIANCE(salary) as salary_variance,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary) as Q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary) as Q3,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY salary) as P95
    FROM 'employees_sample.csv'
    GROUP BY department
    ORDER BY mean_salary DESC
""", backend="duckdb")

print("Salary Statistics by Department:")
print("-" * 120)
print(f"{'Department':<12} | {'Count':>5} | {'Min':>8} | {'Max':>8} | "
      f"{'Mean':>8} | {'Median':>8} | {'StdDev':>8} | {'Q1':>8} | {'Q3':>8}")
print("-" * 120)

for row in results:
    print(f"{row['department']:<12} | {row['employee_count']:>5} | "
          f"${row['min_salary']:>7,} | ${row['max_salary']:>7,} | "
          f"${row['mean_salary']:>7,.0f} | ${row['median_salary']:>7,.0f} | "
          f"${row['salary_stddev']:>7,.0f} | ${row['Q1']:>7,.0f} | ${row['Q3']:>7,.0f}")
```

---

## Example 7: UNION and Set Operations

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

## Example 8: JOIN Multiple Files

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
