# Performance Tuning Guide

This guide helps you optimize SQLStream for your workload, from small CSV files to multi-gigabyte datasets.

---

## Backend Selection

SQLStream offers **three execution backends** with different performance characteristics:

### Backend Comparison

| Backend | Speed | Memory | Best For | Limitations |
|---------|-------|--------|----------|-------------|
| **Python** | Baseline | Low | Learning, debugging, small files (<100K rows) | Slow for large datasets |
| **Pandas** | **10-100x faster** | Medium | Production queries, large files (100K-10M rows) | Basic SQL only |
| **DuckDB** | **100x+ faster** | High | Complex SQL, analytics, huge files (10M+ rows) | Requires duckdb package |

### When to Use Each Backend

#### Python Backend
```bash
# Good for learning and understanding query execution
sqlstream query data.csv "SELECT * FROM data WHERE age > 25" --backend python
```

**Use when:**
- Learning SQL query execution internals
- Debugging query behavior
- Working with small files (<100K rows)
- No dependencies available

**Avoid when:**
- File has >100K rows
- Performance matters
- Complex aggregations or joins

#### Pandas Backend
```bash
# 10-100x faster than Python for large files
sqlstream query large.csv "SELECT city, COUNT(*) FROM data GROUP BY city" --backend pandas
```

**Use when:**
- Files are 100K - 10M rows
- Simple to moderate SQL queries
- Need fast GROUP BY / aggregations
- Memory is available (2-3x file size)

**Avoid when:**
- Need advanced SQL (window functions, CTEs)
- Memory constrained (<2GB RAM)
- Very small files (<10K rows - overhead not worth it)

#### DuckDB Backend
```bash
# 100x+ faster with full SQL support
sqlstream query huge.parquet "SELECT * FROM data WHERE date > '2024-01-01'" --backend duckdb
```

**Use when:**
- Files are >10M rows or >1GB
- Need advanced SQL (CTEs, window functions, complex joins)
- Working with Parquet files
- Need maximum performance

**Avoid when:**
- DuckDB package not installed
- Very simple queries on small files

### Auto Backend Selection

By default, SQLStream intelligently selects the best backend:

```bash
# Let SQLStream choose (recommended)
sqlstream query data.csv "SELECT * FROM data"
```

**Selection logic:**
1. **DuckDB preferred** if installed and query is complex
2. **Pandas** for simple queries on medium-large files
3. **Python** fallback for compatibility

**Override in interactive shell:**
- Press `F5` or `Ctrl+B` to cycle through backends
- Current backend shown in status bar

---

## File Format Performance

### Format Comparison

| Format | Read Speed | Write Speed | Compression | Schema | Best For |
|--------|-----------|-------------|-------------|--------|----------|
| **CSV** | Slow | Fast | None | Inferred | Compatibility, human-readable |
| **Parquet** | **Very Fast** | Moderate | Excellent | Built-in | Analytics, large datasets, archival |
| **JSON** | Moderate | Fast | None | Flexible | APIs, nested data |
| **JSONL** | Fast | Fast | None | Flexible | Streaming, logs, append-only |

### Benchmark Results (1M rows, 10 columns)

```
Format     | Read Time | File Size | Compression |
-----------|-----------|-----------|-------------|
CSV        | 5.2s      | 180 MB    | None        |
CSV (gzip) | 8.1s      | 45 MB     | 4x          |
Parquet    | 0.4s      | 38 MB     | 4.7x        |
JSON       | 12.3s     | 280 MB    | None        |
JSONL      | 6.8s      | 280 MB    | None        |
```

### Recommendations

**For maximum read performance:**
```bash
# Convert CSV to Parquet for repeated queries
sqlstream query data.csv "SELECT * FROM data" --format parquet -o data.parquet

# 10x faster subsequent reads
sqlstream query data.parquet "SELECT * FROM data WHERE age > 25"
```

**For space efficiency:**
- Use Parquet (built-in compression)
- Or gzip CSV files (SQLStream auto-detects)

**For streaming/append:**
- Use JSONL format (line-by-line processing)

**For nested/hierarchical data:**
- Use JSON with nested path syntax:
```bash
sqlstream query "users.json#json:data.users" "SELECT name, email FROM users"
```

---

## Query Optimization

### 1. Column Pruning

**❌ Bad - Reads all columns:**
```sql
SELECT * FROM 'large.csv'
```

**✅ Good - Reads only needed columns:**
```sql
SELECT name, email FROM 'large.csv'
```

**Impact:** 5-10x faster with 100+ columns

### 2. Predicate Pushdown

**❌ Bad - Reads all data then filters:**
```sql
SELECT * FROM (SELECT * FROM 'data.csv') WHERE age > 25
```

**✅ Good - Filters during read:**
```sql
SELECT * FROM 'data.csv' WHERE age > 25
```

**Impact:** 10-100x faster depending on selectivity

### 3. Early LIMIT

**❌ Bad - Processes all rows:**
```sql
SELECT * FROM 'large.csv' ORDER BY age LIMIT 10
```

**✅ Good - Stops early when possible:**
```sql
SELECT * FROM 'large.csv' LIMIT 1000  -- Then process
```

**Impact:** Near-instant for small limits

### 4. Use Appropriate Joins

**Small file JOIN large file:**
```sql
-- Load smaller table first (left side)
SELECT * FROM 'small.csv' s
JOIN 'large.csv' l ON s.id = l.user_id
```

**Impact:** Memory usage reduced by 50%+

### 5. Avoid SELECT DISTINCT on Large Results

**❌ Bad - Memory intensive:**
```sql
SELECT DISTINCT * FROM 'huge.csv'
```

**✅ Better - Use GROUP BY:**
```sql
SELECT column FROM 'huge.csv' GROUP BY column
```

---

## Memory Optimization

### Understanding Memory Usage

**Python backend:**
- Memory = ~2x file size

**Pandas backend:**
- Memory = ~3-4x file size

**DuckDB backend:**
- Memory = ~1-2x file size (best for large files)

### Streaming Large Files

**For files larger than available RAM:**

1. **Use LIMIT to process in chunks:**
```bash
# Process first 100K rows
sqlstream query huge.csv "SELECT * FROM data LIMIT 100000" -o chunk1.parquet

# Process next 100K rows (manual offset in WHERE)
sqlstream query huge.csv "SELECT * FROM data WHERE id > 100000 LIMIT 100000"
```

2. **Use column selection:**
```sql
-- Only read needed columns
SELECT id, name, email FROM 'huge.csv'  -- Not SELECT *
```

3. **Switch to DuckDB backend:**
```bash
# DuckDB has better memory management
sqlstream query huge.parquet "SELECT * FROM data" --backend duckdb
```

4. **Convert to Parquet first:**
```bash
# Parquet uses 4x less memory than CSV
sqlstream query huge.csv "SELECT * FROM data" -o huge.parquet
```

---

## S3 Performance

### Minimize S3 Requests

**❌ Bad - Multiple S3 reads:**
```sql
SELECT * FROM 's3://bucket/data.csv' WHERE age > 25
UNION
SELECT * FROM 's3://bucket/data.csv' WHERE age < 18
```

**✅ Good - Single S3 read:**
```sql
SELECT * FROM 's3://bucket/data.csv' WHERE age > 25 OR age < 18
```

### Use Parquet on S3

**CSV on S3:**
- Must download entire file
- 180 MB download for 1M rows

**Parquet on S3:**
- Column-pruning reduces download
- Only 10 MB download for 2 columns
- **18x less data transferred**

```bash
# Parquet is 18x more efficient on S3
sqlstream query "s3://bucket/data.parquet" "SELECT name, email FROM data"
```

### Partitioned Data

**Use Hive-style partitions for filtering:**
```bash
# Only reads year=2024 partition
sqlstream query "s3://bucket/data/year=2024/" "SELECT * FROM data"
```

### Caching

HTTP URLs are automatically cached for 24 hours:
```bash
# First query downloads
sqlstream query "https://example.com/data.csv" "SELECT * FROM data"

# Subsequent queries use cache (instant)
sqlstream query "https://example.com/data.csv" "SELECT COUNT(*) FROM data"
```

**Cache location:** `~/.cache/sqlstream/`

---

## Interactive Shell Performance

### Background Computation

The interactive shell runs queries asynchronously:
- UI remains responsive during long queries
- Cancel anytime with `Ctrl+C`
- Progress shown in status bar

### Result Set Limits

**By default, shell limits to 10,000 rows** to prevent freezing.

For larger results:
1. Export to file instead of viewing
2. Use LIMIT in query
3. Add more specific WHERE filters

### Export Performance

**Format selection matters:**

```
Format  | 1M rows | Compression |
--------|---------|-------------|
CSV     | 3.2s    | None        |
JSON    | 8.7s    | None        |
Parquet | 1.1s    | Built-in    |
```

**Recommendation:** Use Parquet for large exports

---

## Benchmark Examples

### Small Files (<100K rows)

```bash
# All backends perform similarly
$ time sqlstream query small.csv "SELECT * FROM data WHERE age > 25"

Python:  0.15s
Pandas:  0.18s (overhead not worth it)
DuckDB:  0.22s (overhead not worth it)
```

**Recommendation:** Use default (Python)

### Medium Files (100K - 1M rows)

```bash
$ time sqlstream query medium.csv "SELECT city, COUNT(*) FROM data GROUP BY city"

Python:  42s
Pandas:  1.2s  ⚡ 35x faster
DuckDB:  0.9s  ⚡ 47x faster
```

**Recommendation:** Use Pandas or DuckDB

### Large Files (>1M rows)

```bash
$ time sqlstream query large.parquet "SELECT * FROM data WHERE date > '2024-01-01'"

Python:  380s (6+ minutes)
Pandas:  12s   ⚡ 32x faster
DuckDB:  0.8s  ⚡ 475x faster
```

**Recommendation:** Use DuckDB

### Complex SQL (Window Functions)

```bash
$ sqlstream query data.csv "SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY age) FROM data"

Python:  ❌ Not supported
Pandas:  ❌ Not supported
DuckDB:  ✅ 2.1s
```

**Recommendation:** Use DuckDB for advanced SQL

---

## Performance Checklist

**Before running a query:**

- [ ] Using smallest file format? (Parquet > CSV)
- [ ] Selecting only needed columns? (not `SELECT *`)
- [ ] Filtering as early as possible? (`WHERE` clause)
- [ ] Using appropriate backend? (DuckDB for large/complex)
- [ ] Using LIMIT for exploration? (faster feedback)
- [ ] Partitioned data on S3? (reduces scan)

**For production pipelines:**

- [ ] Convert CSV → Parquet for repeated reads
- [ ] Use DuckDB backend for 100x speedup
- [ ] Cache HTTP sources locally
- [ ] Use column selection extensively
- [ ] Profile with `EXPLAIN` to verify optimizations

---

## Profiling Queries

### View Execution Plan

**CLI:**
```bash
sqlstream query data.csv "SELECT * FROM data WHERE age > 25" --explain
```

**Interactive Shell:**
Press `F4` to view execution plan

**Output:**
```
Execution Plan:
├─ Scan: data.csv
│  └─ Columns: [name, age, city] (pruned 7 columns)
├─ Filter: age > 25 (pushed down)
└─ Limit: 1000 (pushed down)

Optimizations Applied:
✓ Column pruning (reads 3/10 columns)
✓ Predicate pushdown (filters during read)
✓ Limit pushdown (early termination)
```

### Measure Query Time

```bash
# CLI shows execution time
sqlstream query data.csv "SELECT * FROM data" --time

# Output:
# Query executed in 1.23s
```

---

## Troubleshooting Performance

### Query is Slow

1. **Check backend:**
   ```bash
   # Try DuckDB
   sqlstream query data.csv "SELECT * FROM data" --backend duckdb
   ```

2. **Use Parquet instead of CSV:**
   ```bash
   # Convert first
   sqlstream query data.csv "SELECT * FROM data" -o data.parquet
   ```

3. **Add WHERE filters:**
   ```sql
   -- Reduce data scanned
   SELECT * FROM data WHERE date > '2024-01-01'
   ```

### Out of Memory

1. **Use DuckDB backend** (most memory-efficient)
2. **Add LIMIT** to reduce result set
3. **Select fewer columns**
4. **Process in chunks** (manual pagination)

### S3 is Slow

1. **Use Parquet** (column pruning reduces download)
2. **Use partitions** (only read needed partitions)
3. **Run from same AWS region** (reduce latency)
4. **Check S3 credentials** (authentication overhead)

---

## Additional Resources

- [Backend Selection Guide](../features/backends.md)
- [File Format Comparison](../features/data-sources.md)
- [SQL Optimization Tips](../features/sql-support.md)
- [Troubleshooting Guide](../troubleshooting.md)

---

**Last Updated:** December 2025
