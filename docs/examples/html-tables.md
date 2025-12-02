# Querying HTML Tables

SQLStream is excellent for scraping structured data from HTML files or URLs.

**Data Source**: [simple.html](https://github.com/subhayu99/sqlstream/raw/main/examples/simple.html)

## Extracting Data from Web Reports

The example file contains two tables: **Contractors** (Table 0) and **Project Status** (Table 1).

### Example 1: Basic Extraction

Find contractors charging more than $140/hour.

```python
from sqlstream import query

url = "https://github.com/subhayu99/sqlstream/raw/main/examples/simple.html"

# Target Table 0 (Contractors)
results = query().sql(f"""
    SELECT name, project, rate_hourly 
    FROM '{url}#html:0'
    WHERE rate_hourly > 140
""")
```

### Example 2: Joining Tables within an HTML File

You can join two tables extracted from the same HTML document.

```python
# Join Contractors (Table 0) with Projects (Table 1)
sql = f"""
    SELECT 
        c.name, 
        c.rate_hourly, 
        p.status
    FROM '{url}#html:0' c
    JOIN '{url}#html:1' p ON c.project = p.project
    WHERE p.status = 'Active'
"""

results = query().sql(sql, backend='duckdb')
```
