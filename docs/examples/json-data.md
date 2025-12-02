# Querying JSON Data

SQLStream allows you to treat JSON arrays of objects as tables.

**Data Source**: [products.json](https://github.com/subhayu99/sqlstream/raw/main/examples/products.json)

## Basic JSON Querying

### Example 1: Filtering JSON

```python
from sqlstream import query

url = "https://github.com/subhayu99/sqlstream/raw/main/examples/products.json"

# Select electronics under $100
results = query().sql(f"""
    SELECT name, price, stock
    FROM '{url}'
    WHERE category = 'Electronics' AND price < 100
""")
```

### Example 2: Analytics on JSON Logs

**Data Source**: [api_logs.json](https://github.com/subhayu99/sqlstream/raw/main/examples/api_logs.json)

```python
url = "https://github.com/subhayu99/sqlstream/raw/main/examples/api_logs.json"

# Calculate average latency per endpoint
results = query().sql(f"""
    SELECT 
        endpoint,
        COUNT(*) as requests,
        AVG(latency_ms) as avg_latency
    FROM '{url}'
    GROUP BY endpoint
""")
```
