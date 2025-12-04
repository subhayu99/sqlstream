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
1. Join Reordering (optimize join execution plan)
2. Partition Pruning (skip entire partitions/files)
3. Predicate Pushdown (reduce data read)
4. Column Pruning (narrow columns)
5. Limit Pushdown (early termination)
6. Projection Pushdown (transform at source - future)

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

**Implementation:**
- ✅ Fully implemented in CSVReader and ParquetReader
- Early termination at reader level (stops reading after N rows)
- Works seamlessly with filters (limit applied after filtering)

**Limitations:**
- Cannot push down with ORDER BY (need all rows to sort)
- Cannot push down with GROUP BY (need all rows to group)
- Cannot push down with aggregates (need all rows)
- Cannot push down with JOINs (complex - may need all rows)

**Status:** ✅ Fully implemented and tested

---

## 4. Partition Pruning

Skip entire partitions/files based on filter conditions for Hive-style partitioned datasets.

**Benefits:**
- Massive I/O reduction (can skip 10x-1000x data)
- Critical for data lakes and partitioned datasets
- Zero-cost filtering at partition level
- Works with S3 and cloud storage

**Example:**
```sql
-- Dataset: s3://data/year=2023/month=01/data.parquet
--          s3://data/year=2024/month=01/data.parquet
--          s3://data/year=2024/month=02/data.parquet

SELECT * FROM data WHERE year = 2024 AND month = 1
```

**Without partition pruning:**
```
Read all 3 files → Filter rows → Return results
```

**With partition pruning:**
```
Skip year=2023 files → Read only year=2024/month=1 → Return results
```

**Implementation:**
- ✅ Fully implemented in ParquetReader
- Detects Hive-style partitioning (key=value in path)
- Partition columns added as virtual columns to results
- Filters on partition columns removed from row-level filtering

**How it works:**
1. Parse partition info from file path (e.g., `year=2024/month=01/`)
2. Extract partition column filters from WHERE clause
3. Evaluate filters against partition values
4. Skip reading file if partition doesn't match
5. Add partition columns to output rows

**Status:** ✅ Fully implemented and tested

---

## 5. Join Reordering

Optimize join execution order to minimize intermediate result sizes.

**Benefits:**
- Smaller intermediate results = less memory
- Faster execution (less data to process)
- Better cache utilization

**Strategy:**
- Join smaller tables first
- Apply filters early to reduce row counts
- Future: Use table statistics for cost-based decisions

**Example:**
```sql
-- Tables: A (1M rows), B (100 rows), C (1K rows)
-- Bad order:  A JOIN B JOIN C = huge intermediate result
-- Good order: B JOIN C JOIN A = smaller intermediate result
```

**Status:** ⚠️ Framework implemented, placeholder (not yet active)

**Note:** Join reordering is complex and can break query correctness if done incorrectly. Current implementation is a placeholder that provides the infrastructure but doesn't actually reorder joins yet.

---

## 6. Cost-Based Optimization

Framework for statistics-driven optimization decisions.

**Components:**
- Table statistics (row counts, cardinality, min/max values)
- Cost models for operations (scan, filter, join, sort)
- Selectivity estimation
- Plan cost comparison

**Benefits:**
- Smarter optimization decisions
- Better join ordering
- Adaptive query execution (future)
- Index selection (future)

**Example:**
```python
from sqlstream.optimizers import CostModel, TableStatistics

# Estimate join cost
cost = CostModel.estimate_join_cost(
    left_rows=1000000,
    right_rows=100,
    selectivity=0.1
)

# Estimate filter selectivity
selectivity = CostModel.estimate_selectivity(condition)
```

**Status:** ⚠️ Framework implemented (not yet active in query execution)

**Note:** Full cost-based optimization requires statistics collection (expensive) and plan enumeration. Current implementation provides the cost models and infrastructure for future use.

---

## 7. Parallel Execution

Multi-threaded data reading for improved performance.

**Benefits:**
- Faster data ingestion
- Better CPU utilization
- Overlap I/O with computation

**Implementation:**
- Thread pool for parallel reading
- Queue-based producer-consumer pattern
- Works with any reader (CSV, Parquet, HTTP)

**Example:**
```python
from sqlstream.readers import enable_parallel_reading, CSVReader

reader = CSVReader("large_file.csv")
parallel_reader = enable_parallel_reading(reader, num_threads=4)

for row in parallel_reader:
    process(row)
```

**Status:** ⚠️ Infrastructure implemented (basic functionality)

**Note:** Python's GIL limits true parallelism for CPU-bound tasks. Parallel execution is most effective for I/O-bound operations. Parquet already has native parallel reading via PyArrow.

---

## 8. Projection Pushdown

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

## Implementation Status

### Fully Implemented ✅
- **Predicate pushdown** - Push WHERE filters to readers
- **Column pruning** - Read only required columns
- **Limit pushdown** - Early termination for LIMIT queries
- **Partition pruning** - Skip partitions based on filters (Parquet)

### Framework Available ⚠️
- **Join reordering** - Infrastructure exists, not yet active
- **Cost-based optimization** - Cost models and statistics framework available
- **Parallel execution** - Basic thread pool implementation available

### Future Enhancements 🔮
- ⏳ Projection pushdown (push computed expressions to source)
- ⏳ Aggregate pushdown (push GROUP BY to readers)
- ⏳ Index usage (when indexes available)
- ⏳ Adaptive query execution (runtime optimization)
- ⏳ Query result caching
- ⏳ Materialized views
- ⏳ Advanced statistics collection (histograms, sketches)
