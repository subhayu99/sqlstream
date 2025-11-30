# Query Optimizations

SQLStream implements a pipeline-based optimization framework to ensure efficient query execution.

---

## Optimizer Architecture

The optimizer module uses a **modular pipeline design** where each optimization rule is a separate class:

```python
from sqlstream.optimizers import QueryPlanner

planner = QueryPlanner()
planner.optimize(ast, reader)
print(planner.get_optimization_summary())
```

**Pipeline Order:**
1. Predicate Pushdown (reduce data read)
2. Column Pruning (narrow columns)
3. Limit Pushdown (early termination)
4. Projection Pushdown (transform at source - future)

---

## 1. Predicate Pushdown

Filters (WHERE clauses) are "pushed down" to the data source for early filtering.

**Benefits:**
- Reduces I/O by filtering at the source
- Reduces memory usage
- Especially effective for columnar formats (Parquet)
- Can leverage indexes if available

**Example:**
```sql
SELECT * FROM data.csv WHERE age > 30
```

**Without pushdown:**
```
Read all rows → Filter in memory → Return results
```

**With pushdown:**
```
Filter while reading → Return only matching rows
```

**Implementation by Reader:**

- **CSV**: Rows are filtered immediately after reading, before processing
- **Parquet**: Filters can skip entire row groups based on statistics (min/max values), significantly reducing I/O
- **HTTP**: Filters applied after download but before buffering

**Limitations:**
- Currently only supports simple comparisons (column op value)
- Does not support complex expressions (e.g., `LENGTH(name) > 5`)
- Does not support cross-column comparisons (e.g., `age > salary`)
- Disabled for JOIN queries (needs smarter per-table analysis)

---

## 2. Column Pruning

Only columns required for the query are read from disk.

**Benefits:**
- Massive I/O reduction for wide tables
- Reduces memory usage
- Critical for columnar formats (Parquet, ORC)
- Can read 10x faster if selecting 1 column from 10

**Example:**
```sql
SELECT name, age FROM employees  -- 100 columns total
```

**Without pruning:**
```
Read all 100 columns → Project to 2 columns
```

**With pruning:**
```
Read only 2 columns → Much faster
```

**Implementation by Reader:**

- **Parquet**: Only decodes requested columns from file
- **CSV**: Whole line is read, but only relevant fields are parsed and kept
- **HTTP**: Entire response read, but only needed columns extracted

**Column Analysis:**

The optimizer analyzes which columns are needed from:
- SELECT clause
- WHERE clause
- GROUP BY clause
- ORDER BY clause
- Aggregate functions
- JOIN conditions

**Limitations:**
- Cannot prune with `SELECT *`
- CSV still reads full lines (just parses fewer fields)

---

## 3. Limit Pushdown

LIMIT clauses enable early termination of data reading.

**Benefits:**
- Stop reading after N rows
- Massive speedup for large files
- Reduces memory usage

**Example:**
```sql
SELECT * FROM large_file.csv LIMIT 10
```

**Without pushdown:**
```
Read entire file → Take first 10 rows
```

**With pushdown:**
```
Stop reading after 10 rows → Much faster
```

**Limitations (Current Implementation):**
- Not yet implemented in readers (placeholder for future work)
- Cannot push down with ORDER BY (need all rows to sort)
- Cannot push down with GROUP BY (need all rows to group)
- Cannot push down with aggregates (need all rows)
- Cannot push down with JOINs (complex - may need all rows)

**Status:** ⚠️ Optimizer detects opportunities but readers don't implement yet

---

## 4. Projection Pushdown

Push computed expressions to the data source for evaluation.

**Benefits (when implemented):**
- Evaluate expressions at read time
- Reduce data movement
- Leverage native database/engine functions

**Example (future):**
```sql
SELECT UPPER(name), age * 2 FROM data
```

**With pushdown:**
```
Reader evaluates UPPER() and age*2 → Return transformed data
```

**Status:** ⚠️ Not yet implemented - placeholder for future work

---

## Vectorized Execution (Pandas Backend)

When using the Pandas backend (`backend="pandas"`), SQLStream leverages:

- **SIMD Instructions**: CPU-level parallelism for array operations
- **Efficient Memory Layout**: Columnar storage in memory
- **Optimized Algorithms**: C-optimized implementations of joins and aggregations
- **NumPy**: Highly optimized numerical operations

**When to use:**
```python
# For large datasets or complex aggregations
query("data.csv").sql("SELECT * FROM data", backend="pandas")
```

---

## Lazy Evaluation

Query results are iterators - execution only happens when you consume results.

**Benefits:**
- **Early Termination**: `LIMIT` clauses stop execution as soon as enough rows are found
- **Streaming**: Start processing first results while rest of query runs
- **Memory Efficient**: Don't load entire result set into memory

**Example:**
```python
results = query("data.csv").sql("SELECT * FROM data LIMIT 10")
# No execution yet

for row in results:  # Execution starts here
    print(row)
```

---

## Optimization Summary

You can see which optimizations were applied:

```python
plan = query("data.csv").sql("""
    SELECT name, age
    FROM data
    WHERE age > 30
""", backend="python").explain()

print(plan)
```

**Output:**
```
Query Plan:
  Scan: data.csv
  Filter: age > 30
  Project: name, age

Optimizations applied:
  - Predicate pushdown: 1 condition(s)
  - Column pruning: 3 column(s) selected
```

---

## Custom Optimizers

You can add custom optimization rules:

```python
from sqlstream.optimizers import QueryPlanner, Optimizer

class MyCustomOptimizer(Optimizer):
    def get_name(self) -> str:
        return "My custom rule"

    def can_optimize(self, ast, reader) -> bool:
        # Check if optimization applies
        return True

    def optimize(self, ast, reader) -> None:
        # Apply optimization
        self.applied = True
        self.description = "did something cool"

planner = QueryPlanner()
planner.add_optimizer(MyCustomOptimizer())
```

---

## Performance Tips

1. **Use Parquet for wide tables**: Column pruning is most effective
2. **Use WHERE early**: Predicate pushdown reduces data read
3. **Select specific columns**: Avoid `SELECT *` when possible
4. **Use LIMIT for exploration**: Quick previews of large files
5. **Use Pandas backend for aggregations**: Faster for GROUP BY queries
6. **Check explain plans**: Use `.explain()` to see which optimizations applied

---

## Future Optimizations (Roadmap)

- ✅ Predicate pushdown (implemented)
- ✅ Column pruning (implemented)
- ⏳ Limit pushdown (detected but not implemented in readers)
- ⏳ Projection pushdown (placeholder)
- ⏳ Partition pruning (for partitioned Parquet)
- ⏳ Join reordering (optimize join order)
- ⏳ Aggregate pushdown (push GROUP BY to readers)
- ⏳ Index usage (when indexes available)
- ⏳ Parallel execution (multi-threaded readers)
- ⏳ Adaptive query execution (runtime optimization)
