# Querying Markdown Tables

Markdown files often contain structured data in tables. SQLStream can extract these tables and run SQL queries on them.

**Data Source**: [sample_data.md](https://github.com/subhayu99/sqlstream/raw/main/examples/sample_data.md)

## Selecting Specific Tables

When a file contains multiple tables, SQLStream assigns them indices starting at 0. You can specify the table using the fragment syntax `#markdown:<index>`.

### Example 1: Querying the First Table (Inventory)

The file contains an "Inventory" table first (Index 0).

```bash
sqlstream query "SELECT * FROM 'https://github.com/subhayu99/sqlstream/raw/main/examples/sample_data.md#markdown:0' WHERE quantity < 50"
```

### Example 2: Querying the Second Table (Events)

Query the "Recent Events" table (Index 1) to find high-severity issues.

```python
from sqlstream import query

url = "https://github.com/subhayu99/sqlstream/raw/main/examples/sample_data.md"

# Use #markdown:1 to target the second table
results = query().sql(f"""
    SELECT event_id, description 
    FROM '{url}#markdown:1' 
    WHERE severity = 'High'
""")

for row in results:
    print(f"Alert: {row['description']}")
```
