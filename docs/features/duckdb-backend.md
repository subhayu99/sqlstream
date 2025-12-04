# DuckDB Backend - Full SQL Support

The DuckDB backend provides **complete SQL support** by leveraging DuckDB's powerful SQL engine while maintaining SQLStream's simple interface.

---

## Why DuckDB Backend?

**Choose DuckDB backend when you need**:
- ✅ Full SQL compatibility (window functions, CTEs, subqueries, etc.)
- ✅ Maximum performance (10-1000x faster than Python backend)
- ✅ Complex analytical queries
- ✅ Production-ready SQL engine

**Comparison**:

| Feature | Python Backend | Pandas Backend | **DuckDB Backend** |
|---------|---------------|----------------|-------------------|
| **Speed** | Baseline | 10-100x faster | **10-1000x faster** |
| **Window Functions** | ❌ | ❌ | **✅** |
| **CTEs (WITH clause)** | ❌ | ❌ | **✅** |
| **Subqueries** | ❌ | ❌ | **✅** |
| **HAVING clause** | ❌ | ❌ | **✅** |
| **String Functions** | ❌ Limited | ❌ Limited | **✅ Full** |
| **Date Functions** | ❌ Limited | ❌ Limited | **✅ Full** |
| **Statistical Functions** | ❌ | ❌ | **✅** |
| **Use Case** | Learning | Fast queries | **Production & Complex SQL** |

---

## Installation

```bash
# Install DuckDB support
pip install duckdb

# Or install with all SQLStream features
pip install "sqlstream[all]"
```

---

## Usage

### Python API

```python
from sqlstream import query

# Use DuckDB backend explicitly
result = query("data.csv").sql("""
    SELECT 
        department,
        AVG(salary) as avg_salary,
        ROW_NUMBER() OVER (ORDER BY AVG(salary) DESC) as rank
    FROM 'data.csv'
    GROUP BY department
    HAVING avg_salary > 50000
""", backend="duckdb")

for row in result:
    print(row)
```

### CLI

```bash
# Use DuckDB backend
sqlstream query "SELECT * FROM 'data.csv'" --backend duckdb

# Complex query with window functions
sqlstream query "
    WITH ranked AS (
        SELECT *, 
               ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rank
        FROM 'employees.csv'
    )
    SELECT * FROM ranked WHERE rank <= 3
" --backend duckdb
```

---

## Supported SQL Features

### Window Functions ✅

```sql
SELECT 
    name,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank,
    AVG(salary) OVER (PARTITION BY department) as dept_avg,
    salary - AVG(salary) OVER (PARTITION BY department) as diff_from_avg
FROM 'employees.csv'
```

**Supported window functions**:
- `ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`
- `LAG()`, `LEAD()`  
- `FIRST_VALUE()`, `LAST_VALUE()`, `NTH_VALUE()`
- `SUM`, `AVG`, `MIN`, `MAX`, `COUNT` with `OVER`

---

### Common Table Expressions (CTEs) ✅

```sql
WITH high_earners AS (
    SELECT * FROM 'employees.csv'
    WHERE salary > 100000
),
dept_stats AS (
    SELECT 
        department,
        AVG(salary) as avg_salary
    FROM high_earners
    GROUP BY department
)
SELECT * FROM dept_stats
WHERE avg_salary > 120000
```

---

### Subqueries ✅

```sql
-- Subquery in FROM
SELECT department, avg_salary
FROM (
    SELECT department, AVG(salary) as avg_salary
    FROM 'employees.csv'
    GROUP BY department
) dept_avgs
WHERE avg_salary > 75000

-- Subquery in WHERE
SELECT * FROM 'employees.csv'
WHERE salary > (SELECT AVG(salary) FROM 'employees.csv')

-- Correlated subquery
SELECT e1.*
FROM 'employees.csv' e1
WHERE salary > (
    SELECT AVG(salary) 
    FROM 'employees.csv' e2 
    WHERE e2.department = e1.department
)
```

---

### HAVING Clause ✅

```sql
SELECT 
    department,
    AVG(salary) as avg_salary,
    COUNT(*) as employee_count
FROM 'employees.csv'
GROUP BY department
HAVING COUNT(*) > 10 AND AVG(salary) > 75000
```

---

### String Functions ✅

```sql
SELECT 
    UPPER(name) as name_upper,
    LOWER(department) as dept_lower,
    LENGTH(name) as name_length,
    SUBSTRING(name, 1, 5) as name_prefix,
    CONCAT(first_name, ' ', last_name) as full_name,
    TRIM(email) as email_clean,
    REPLACE(phone, '-', '') as phone_digits
FROM 'employees.csv'
```

**Supported**: `UPPER`, `LOWER`, `LENGTH`, `SUBSTRING`, `CONCAT`, `TRIM`, `REPLACE`, `SPLIT`, `REGEXP_MATCHES`, and many more.

---

### Date/Time Functions ✅

```sql
SELECT 
    hire_date,
    EXTRACT(YEAR FROM hire_date) as hire_year,
    EXTRACT(MONTH FROM hire_date) as hire_month,
    DATE_DIFF('day', hire_date, CURRENT_DATE) as days_since_hire,
    DATE_ADD(hire_date, INTERVAL 1 YEAR) as first_anniversary,
    STRFTIME(hire_date, '%Y-%m') as year_month
FROM 'employees.csv'
```

**Supported**: `EXTRACT`, `DATE_DIFF`, `DATE_ADD`, `DATE_SUB`, `CURRENT_DATE`, `CURRENT_TIME`, `STRFTIME`, and more.

---

### Statistical Functions ✅

```sql
SELECT 
    department,
    AVG(salary) as mean_salary,
    STDDEV(salary) as salary_stddev,
    VARIANCE(salary) as salary_variance,
    MEDIAN(salary) as median_salary,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY salary) as p95_salary
FROM 'employees.csv'
GROUP BY department
```

---

### UNION/INTERSECT/EXCEPT ✅

```sql
-- UNION
SELECT name FROM 'employees.csv' WHERE department = 'Engineering'
UNION ALL
SELECT name FROM 'contractors.csv' WHERE role = 'Engineer'

-- INTERSECT
SELECT email FROM 'employees.csv'
INTERSECT
SELECT email FROM 'active_users.csv'

-- EXCEPT
SELECT email FROM 'all_users.csv'
EXCEPT
SELECT email FROM 'blocked_users.csv'
```

---

## Performance

DuckDB backend provides **exceptional performance**:

### Benchmarks

| Query Type | Python | Pandas | **DuckDB** | Speedup |
|------------|--------|--------|------------|---------|
| Simple SELECT | 1.2s | 0.15s | **0.02s** | **60x** |
| Complex JOIN | 8.5s | 0.8s | **0.05s** | **170x** |
| Window Functions | ❌ | ❌ | **0.12s** | **∞** |
| Aggregations | 2.1s | 0.3s | **0.04s** | **52x** |

_Benchmark on 1M row dataset, Intel i7, 16GB RAM_

---

## File Format Support

DuckDB backend supports **all SQLStream file formats** by leveraging the unified Reader architecture:

### CSV Files
```python
query("data.csv").sql("SELECT * FROM 'data.csv'", backend="duckdb")
```

### Parquet Files
```python
query("data.parquet").sql("SELECT * FROM 'data.parquet'", backend="duckdb")
```

### HTML Tables
```python
# Query tables embedded in HTML files
query("report.html").sql("SELECT * FROM 'report.html#html:0'", backend="duckdb")
```

### Markdown Tables
```python
# Query tables in Markdown documents
query("README.md").sql("SELECT * FROM 'README.md#markdown:0'", backend="duckdb")
```

### S3 Files
```python
# Automatically handles S3 authentication via s3fs
query("s3://bucket/data.parquet").sql(
    "SELECT * FROM 's3://bucket/data.parquet' WHERE date > '2024-01-01'",
    backend="duckdb"
)
```

### HTTP URLs
```python
# Works with CSV, Parquet, HTML, etc. over HTTP
query("https://example.com/data.csv").sql(
    "SELECT * FROM 'https://example.com/data.csv'",
    backend="duckdb"
)
```

---

## Advanced Usage

### Multiple Files

```python
result = query("employees.csv").sql("""
    SELECT 
        e.name,
        e.salary,
        d.department_name,
        l.city
    FROM 'employees.csv' e
    JOIN 'departments.csv' d ON e.dept_id = d.id
    JOIN 'locations.csv' l ON d.location_id = l.id
    WHERE e.salary > 75000
    ORDER BY e.salary DESC
""", backend="duckdb")
```

### Complex Analytics

```python
result = query("sales.parquet").sql("""
    WITH monthly_sales AS (
        SELECT 
            DATE_TRUNC('month', sale_date) as month,
            product_id,
            SUM(amount) as total_sales,
            COUNT(*) as sale_count
        FROM 'sales.parquet'
        WHERE sale_date >= '2024-01-01'
        GROUP BY 1, 2
    ),
    ranked_products AS (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY month ORDER BY total_sales DESC) as rank
        FROM monthly_sales
    )
    SELECT 
        month,
        product_id,
        total_sales,
        sale_count,
        rank
    FROM ranked_products
    WHERE rank <= 10
    ORDER BY month, rank
""", backend="duckdb")
```

---

## Backend Selection

### Auto-Detection (Default)

```python
# Automatically selects: Pandas > DuckDB > Python
result = query("data.csv").sql("SELECT * FROM 'data.csv'")
```

**Priority order**:
1. **Pandas** (if installed) - for backward compatibility
2. **DuckDB** (if installed and pandas not available)
3. **Python** (fallback)

### Explicit Selection

```python
# Force DuckDB (raises error if not installed)
result = query("data.csv").sql(
    "SELECT * FROM 'data.csv' WHERE ...",
    backend="duckdb"
)

# Force Pandas
result = query("data.csv").sql("...", backend="pandas")

# Force Python (educational)
result = query("data.csv").sql("...", backend="python")
```

---

## Limitations

### Known Limitations

1. **Requires DuckDB**: Must install separately (`pip install duckdb`)
2. **Raw SQL Required**: Currently requires storing raw SQL (no AST reconstruction)
3. **Read-Only**: No INSERT/UPDATE/DELETE (same as other backends)

### Not Supported (DuckDB Limitations)

- Write operations (INSERT, UPDATE, DELETE)
- User-defined functions (UDFs) from Python
- Temporary tables across queries

---

## When to Use Each Backend

### Use **Python Backend** when:
- Learning query execution internals
- Understanding the Volcano model
- Educational purposes
- Query fits in SQLStream's limited SQL support

### Use **Pandas Backend** when:
- Need better performance than Python
- Query fits in SQLStream's parser
- Already using pandas in your project
- Want balance of speed and compatibility

### Use **DuckDB Backend** when:
- ✅ **Need complex SQL** (CTEs, window functions, subqueries)
- ✅ **Maximum performance** required
- ✅ **Production workloads**
- ✅ **Full SQL compatibility** needed
- ✅ **Large datasets** (multi-GB files)
- ✅ **Analytical queries**

---

## Troubleshooting

### "DuckDB backend requested but duckdb is not installed"

**Solution**:
```bash
pip install duckdb
# or
pip install "sqlstream[all]"
```

### Query fails with DuckDB but works with other backends

DuckDB has **stricter SQL syntax**. Check:
- Table/column names are case-sensitive
- String literals use single quotes `'...'`
- File paths are quoted: `'file.csv'` not `file.csv`

### S3/HTTP queries fail

**Solution**: DuckDB needs httpfs extension (auto-loaded by SQLStream)
```python
# Should work automatically
query("s3://bucket/data.parquet").sql(
    "SELECT * FROM 's3://bucket/data.parquet'",
    backend="duckdb"
)
```

---

## Examples

See [Examples](../examples/duckdb-backend.md) for complete use cases.

---

## See Also

- [SQL Support (Python/Pandas)](sql-support.md) - Limited SQL for Python/Pandas backends
- [Pandas Backend](pandas-backend.md) - Fast execution for supported SQL
- [FAQ](../guides/faq.md) - Common questions about backends
- [Limitations](../reference/limitations.md) - What's not supported
