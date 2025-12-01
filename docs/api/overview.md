# Python API Overview

Use SQLStream programmatically in your Python code.

## Basic Usage

```python
from sqlstream import query

# Execute query with explicit source
results = query("data.csv").sql("SELECT * FROM data WHERE age > 25")

# Execute query with inline source (extracted from SQL)
results = query().sql("SELECT * FROM 'data.csv' WHERE age > 25")

# Iterate (lazy)
for row in results:
    print(row)

# Or convert to list (eager)
results_list = query().sql("SELECT * FROM 'data.csv'").to_list()
```
