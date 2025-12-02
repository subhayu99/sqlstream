# JOIN Examples

Combine data from multiple files using standard SQL syntax.

## joining CSVs

**Data**:

- [employees.csv](https://github.com/subhayu99/sqlstream/raw/main/examples/employees.csv)
- [departments.csv](https://github.com/subhayu99/sqlstream/raw/main/examples/departments.csv)

### Inner Join

Match employees to their department names.

```python
from sqlstream import query

base = "https://github.com/subhayu99/sqlstream/raw/main/examples"

sql = f"""
    SELECT 
        e.name,
        d.dept_name,
        e.salary
    FROM '{base}/employees.csv' e
    JOIN '{base}/departments.csv' d ON e.dept_id = d.dept_id
    WHERE e.salary > 80000
"""

results = query().sql(sql)
```

### Aggregation across Joins

Calculate total budget usage (sum of salaries) per department.

```python
sql = f"""
    SELECT 
        d.dept_name,
        d.budget,
        SUM(e.salary) as total_salary_expense
    FROM '{base}/departments.csv' d
    JOIN '{base}/employees.csv' e ON d.dept_id = e.dept_id
    GROUP BY d.dept_name, d.budget
"""

results = query().sql(sql, backend="duckdb")
```
