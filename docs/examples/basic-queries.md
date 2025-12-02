# Basic Query Examples

All examples below use real data files hosted in the SQLStream repository. You can copy, paste, and run them immediately!

## Selecting Data

**Data**: [employees.csv](https://github.com/subhayu99/sqlstream/raw/main/examples/employees.csv)

### CLI Example

```bash
sqlstream query "SELECT name, salary FROM 'https://github.com/subhayu99/sqlstream/raw/main/examples/employees.csv' WHERE salary > 100000"
```

### Python Example

```python
from sqlstream import query

url = "https://github.com/subhayu99/sqlstream/raw/main/examples/employees.csv"

# Select employees hired after 2020
results = query().sql(f"""
    SELECT name, hire_date 
    FROM '{url}'
    WHERE hire_date >= '2020-01-01'
""")
```

## Ordering and Limiting

```python
# Top 3 highest paid employees
results = query().sql(f"""
    SELECT name, salary
    FROM '{url}'
    ORDER BY salary DESC
    LIMIT 3
""")
```
