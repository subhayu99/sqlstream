# SQL Support

SQLStream supports a practical subset of SQL designed for data exploration and ETL tasks.

---

## Supported Syntax

### SELECT

```sql
-- Select all columns
SELECT * FROM data

-- Select specific columns
SELECT name, age, city FROM data

-- With table alias
SELECT d.name, d.age FROM data d
```

### WHERE

```sql
-- Simple conditions
SELECT * FROM data WHERE age > 25
SELECT * FROM data WHERE name = 'Alice'
SELECT * FROM data WHERE salary >= 80000

-- Multiple conditions with AND
SELECT * FROM data WHERE age > 25 AND city = 'NYC'
SELECT * FROM data WHERE salary > 80000 AND department = 'Engineering'
```

**Supported operators**: `=`, `>`, `<`, `>=`, `<=`, `!=`, `<>`

### GROUP BY

```sql
-- Simple grouping
SELECT city, COUNT(*) FROM data GROUP BY city

-- Multiple columns
SELECT department, city, AVG(salary) FROM data GROUP BY department, city

-- With WHERE
SELECT city, COUNT(*) FROM data WHERE age > 25 GROUP BY city
```

### Aggregate Functions

```sql
SELECT COUNT(*) FROM data
SELECT COUNT(id) FROM data
SELECT SUM(salary) FROM data
SELECT AVG(age) FROM data
SELECT MIN(salary) FROM data
SELECT MAX(salary) FROM data
```

**With aliases**:
```sql
SELECT department, COUNT(*) AS employee_count, AVG(salary) AS avg_salary
FROM data
GROUP BY department
```

### JOIN

```sql
-- INNER JOIN
SELECT * FROM employees e
INNER JOIN departments d ON e.dept_id = d.id

-- LEFT JOIN
SELECT * FROM employees e
LEFT JOIN departments d ON e.dept_id = d.id

-- RIGHT JOIN
SELECT * FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.id
```

### ORDER BY

```sql
-- Ascending (default)
SELECT * FROM data ORDER BY age
SELECT * FROM data ORDER BY age ASC

-- Descending
SELECT * FROM data ORDER BY salary DESC

-- Multiple columns
SELECT * FROM data ORDER BY city ASC, age DESC
```

### LIMIT

```sql
-- Top 10 rows
SELECT * FROM data LIMIT 10

-- With ORDER BY
SELECT * FROM data ORDER BY salary DESC LIMIT 5
```

---

## Complete Example

```sql
SELECT
    department,
    COUNT(*) AS employee_count,
    AVG(salary) AS avg_salary,
    MIN(salary) AS min_salary,
    MAX(salary) AS max_salary
FROM employees
WHERE hire_date > '2020-01-01'
  AND status = 'active'
GROUP BY department
HAVING COUNT(*) > 5
ORDER BY avg_salary DESC
LIMIT 10
```

---

## Inline File Paths (Phase 7.6)

```bash
# Single file
sqlstream query "SELECT * FROM 'data.csv' WHERE age > 25"

# Multiple files with JOIN
sqlstream query "SELECT c.name, o.total FROM 'customers.csv' c JOIN 'orders.csv' o ON c.id = o.customer_id"

# Quoted paths (for spaces)
sqlstream query "SELECT * FROM '/path/with spaces/data.csv'"
```

---

## Not Yet Supported

The following SQL features are planned but not yet implemented:

- ❌ Subqueries
- ❌ UNION/INTERSECT/EXCEPT
- ❌ HAVING clause
- ❌ CASE expressions
- ❌ String functions (UPPER, LOWER, etc.)
- ❌ Date functions
- ❌ Window functions
- ❌ Common Table Expressions (WITH)

---

## Next Steps

- [JOIN Examples](joins.md)
- [Aggregation Examples](aggregations.md)
- [Inline File Paths](inline-paths.md)
