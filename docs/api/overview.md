# Python API Overview

Use SQLStream programmatically in your Python code.

## Basic Usage

```python
from sqlstream import query

# Execute query
results = query("data.csv").sql("SELECT * FROM data WHERE age > 25")

# Iterate (lazy)
for row in results:
    print(row)

# Or convert to list (eager)
results_list = query("data.csv").sql("SELECT * FROM data").to_list()
```
