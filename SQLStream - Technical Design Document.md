# SQLStream - Technical Design Document

## Vision & Philosophy

**Core Principle:** "Optimize for the 90% case, make the 10% possible"

Build a tool that:
- Solves the most common data exploration tasks extremely well
- Keeps the codebase small enough for one person to understand completely
- Uses clever algorithms over brute force where it matters
- Degrades gracefully for unsupported features

---

## Key Design Decisions

### 1. Lazy Evaluation vs Eager Execution

**Decision:** Default to lazy evaluation everywhere

**Why:**
- Works with infinite generators
- Memory efficient for large datasets
- User can force materialization when needed (`list()`, `.to_parquet()`)
- Natural pipeline composition

**Tradeoff:**
- ORDER BY requires materialization (but we can warn users)
- Some operations need full dataset (GROUP BY all rows)
- Debugging is harder (no intermediate results visible)

**Solution Pattern:**
```
Stream → Transform → Transform → Materialize-only-when-needed

Examples:
- WHERE: Can filter lazily ✓
- SELECT: Can project lazily ✓
- LIMIT: Can stop early ✓
- ORDER BY: Must materialize ✗
- GROUP BY: Needs smart handling (see below)
```

**GROUP BY Special Case:**
Use incremental aggregation instead of full materialization:
- Keep running aggregates in a dictionary
- Update as rows stream through
- Only materialize final results
- Memory = O(unique groups) not O(total rows)

---

### 2. Zero-Copy vs Data Transformation

**Decision:** Minimize data copying, pass references where possible

**Techniques:**

**A. Generator Chaining**
```
Input → Filter Generator → Map Generator → Output
```
Each operation yields to the next without creating intermediate lists

**B. View-based Projections**
Don't copy data for SELECT, create views:
```
Original: {id: 1, name: "Alice", age: 30, country: "USA", ...}
After SELECT id, name: Return dict view/proxy that only exposes id, name
```

**C. In-place Sorting**
When ORDER BY is required, sort in-place rather than creating sorted copy

---

### 3. Statistics-Driven Optimization

**Philosophy:** "Know your data before touching it"

**Data Sources with Statistics:**

1. **Parquet Files** (Built-in statistics)
   - Row group min/max for each column
   - Null counts
   - Distinct counts (sometimes)

2. **Partitioned Data** (Partition values)
   ```
   /data/year=2024/month=01/file.parquet
   /data/year=2024/month=02/file.parquet
   ```
   Partition columns have implicit min/max

3. **Sampling for Unknown Sources**
   - Take first 1000 rows
   - Infer types and value ranges
   - Use for query planning

**Using Statistics for Skip Logic:**

```
Query: WHERE age > 60 AND country = 'USA'

Row Group Stats:
  Row Group 1: age [18-35], countries ['USA', 'UK', 'Canada']
  Row Group 2: age [36-55], countries ['USA', 'Germany']
  Row Group 3: age [56-90], countries ['USA', 'France']

Decision:
  Skip Row Group 1: max age (35) < 60
  Skip Row Group 2: max age (55) < 60  
  Read Row Group 3: age range overlaps with filter ✓
```

**The Power of Statistics:**
- Skip 66% of data without reading it
- Reduces I/O by factor of 3
- Critical for remote files (S3, HTTP)

---

### 4. Push vs Pull Execution Model

**Decision:** Hybrid - Pull-based with push-down optimizations

**Pull-Based (Volcano Model):**
```
User calls: list(query_result)
↓
Limit pulls from Order By
↓
Order By pulls from Filter  
↓
Filter pulls from Scan
↓
Scan reads data
```

Each operator pulls from child when it needs data.

**Push-Down Optimizations:**
Before execution, push requirements down the tree:
```
Optimizer sees: Filter → Scan
Optimizer transforms: Scan(with_filter)
```

**Why Hybrid?**
- Pull model is simple to implement and reason about
- Push-down gives performance without complexity
- Best of both worlds

---

## Advanced Optimization Techniques

### 1. Predicate Pushdown - Deep Dive

**Concept:** Move filters as close to data source as possible

**Pushdown Levels:**

**Level 1: File Format (Parquet)**
```
Filter pushed into Parquet reader
→ Uses row group statistics
→ Skips entire row groups
→ Huge I/O savings
```

**Level 2: Storage System (S3, HTTP)**
```
Filter pushed to API
→ S3 Select: Server-side filtering
→ Only transfer matching rows over network
→ Bandwidth savings
```

**Level 3: Partition Pruning**
```
Filter pushed to partition discovery
→ Don't even list irrelevant partitions
→ Metadata operation savings
```

**Pushdown Rules:**

Safe to push:
- `column = value`
- `column > value`
- `column IN (values)`
- `AND` combinations of above

NOT safe to push:
- Complex expressions: `LENGTH(name) > 5`
- Cross-column: `age > salary`
- User-defined functions
- OR conditions (sometimes)

**Partial Pushdown Strategy:**
```
Original: WHERE age > 25 AND LENGTH(name) > 5

Push down: age > 25 (simple predicate)
Keep in executor: LENGTH(name) > 5 (complex expression)

Still wins! Reduces dataset before expensive operation.
```

---

### 2. Column Pruning - Implementation Strategies

**Goal:** Read minimum columns needed

**Column Dependency Analysis:**

```
Query: SELECT name, age * 2 as double_age 
       FROM users 
       WHERE country = 'USA' 
       ORDER BY salary

Needed columns:
  - name (in SELECT)
  - age (in SELECT expression)
  - country (in WHERE)
  - salary (in ORDER BY)

DON'T read:
  - email, address, phone, etc.
```

**Columnar Format Advantage (Parquet):**
```
Row format: Must read full row
Columnar: Read only needed columns

File size: 100 columns × 1M rows = 100M values
Reading 4 columns: 4M values (96% reduction!)
```

**Implementation Decision:**

Use Parquet's native column selection:
```python
# PyArrow does the heavy lifting
table = parquet_file.read(columns=['name', 'age', 'country', 'salary'])
```

Don't implement your own columnar format - use Parquet.

---

### 3. Partition Pruning - Advanced Patterns

**Hive-Style Partitioning:**
```
/data/year=2024/month=01/country=USA/data.parquet
/data/year=2024/month=01/country=UK/data.parquet
/data/year=2024/month=02/country=USA/data.parquet
```

**Query:** `WHERE year = 2024 AND country = 'USA'`

**Pruning Algorithm:**
1. Parse partition structure from filesystem
2. Extract partition predicates from WHERE
3. Match partition values against predicates
4. Only scan matching partitions

**Partition Discovery:**
```
Fast approach:
1. List top-level directories (year=*)
2. Filter by year predicate
3. Recursively descend only matching paths
4. Stop early when partition doesn't match

vs Naive:
List ALL files recursively, then filter
(Much slower for large partition structures)
```

**Multi-Level Pruning:**
```
Query: WHERE year = 2024 AND month IN (1, 2)

Pruning tree:
/data/
  year=2023/     [SKIP - year filter]
  year=2024/     [KEEP]
    month=01/    [KEEP - month filter]
    month=02/    [KEEP - month filter]
    month=03/    [SKIP - month filter]
```

---

### 4. Join Optimization Strategies

**Problem:** Joins are expensive - O(N×M) naive

**Hash Join (Primary Strategy):**
```
1. Build phase: Create hash table from smaller table
   Key: join column value → Value: full row

2. Probe phase: Stream through larger table
   For each row, lookup in hash table by join key
   
Complexity: O(N + M) instead of O(N×M)
Memory: O(size of smaller table)
```

**Size Estimation:**
```
Before reading data, estimate sizes:
- Parquet: Use metadata (num_rows, file_size)
- CSV: Sample first 1000 rows, extrapolate
- URL: Use Content-Length header

Choose smaller table for hash table build
```

**Bloom Filter Optimization:**
```
Problem: Large table has many non-matching rows
Solution: Create bloom filter from smaller table

1. Build bloom filter from join keys in small table
2. Filter large table using bloom filter
3. Reduce large table by ~90% before hash join
4. Do hash join on filtered data

Bloom filter size: ~10 bits per element = tiny
False positive rate: ~1% (acceptable)
```

**When Bloom Filter Helps:**
- Large table has many non-matches
- Join selectivity is low (<10%)
- Network/disk I/O is bottleneck

---

## HTTP Streaming - Deep Dive

### Why This is Hard

**Problem 1: Parquet Footer is at the End**
```
[Row Group 1 Data][Row Group 2 Data]...[Metadata Footer]
↑                                       ↑
Start of file                          End of file

To know what's in the file, must read footer first!
```

**Problem 2: Row Groups are Scattered**
```
Byte 0      Byte 1000    Byte 5000    Byte 9000
[RG1 Data]  [RG2 Data]   [RG3 Data]   [Footer]

To read RG2, need byte range 1000-5000
Can't stream sequentially!
```

### Clever HTTP Range Request Strategy

**Phase 1: Minimal Metadata**
```
1. Request: Last 8 bytes (contains footer size)
   GET /file.parquet
   Range: bytes=-8

2. Parse footer size (e.g., 1024 bytes)

3. Request: Footer metadata
   Range: bytes=-1032--8
   (Get 1024 bytes before the last 8 bytes)
```

**Phase 2: Row Group Selection**
```
4. Parse footer:
   - Row Group 1: offset=0, size=1000, stats={age: [18-30]}
   - Row Group 2: offset=1000, size=4000, stats={age: [31-45]}
   - Row Group 3: offset=5000, size=4000, stats={age: [46-90]}

5. Apply filter (WHERE age > 60):
   - Skip RG1: max_age=30
   - Skip RG2: max_age=45
   - Read RG3: overlaps with filter
```

**Phase 3: Selective Read**
```
6. Request only needed row group:
   Range: bytes=5000-9000
   
Total downloaded: 8 + 1024 + 4000 = ~5KB
vs Full file: 9000 bytes

Savings: 44% (and scales better with more RGs!)
```

**HTTP Range Request Gotchas:**

- Not all servers support ranges (check Accept-Ranges header)
- Some CDNs cache entire file even for range requests
- Need to handle 206 Partial Content vs 200 OK
- Concurrent range requests can help (parallel RG downloads)

---

## Schema Inference - Smart Type Detection

### Why Type Inference Matters

**Without types:**
```python
row['age'] = "30"  # String
filter: age > 25   # Compares strings: "30" > "25" ✓
                   # But "30" > "3" is False! (string comparison)
```

**With types:**
```python
row['age'] = 30  # Integer
filter: age > 25  # Compares numbers: 30 > 25 ✓ (correct!)
```

### Inference Strategy

**Level 1: Python Native Types**
```
Value: 42          → int
Value: 3.14        → float  
Value: "hello"     → string
Value: True        → boolean
```

**Level 2: String Pattern Recognition**
```
"2024-01-15"       → date
"2024-01-15 14:30" → datetime
"$1,234.56"        → money
"123-45-6789"      → ssn
"192.168.1.1"      → ipv4
```

**Level 3: Statistical Type Inference**
```
Sample 1000 rows:
- 950 rows parse as dates
- 50 rows are null/malformed

Decision: Column is "date" type
Strategy: Parse valid dates, mark invalid as null
```

**Type Promotion:**
```
Rows 1-100:   All integers (int64)
Row 101:      Contains 3.14 (float)

Action: Promote column to float64
Convert previous integers to floats
```

### Schema Evolution Handling

**Problem:** Columns appear/disappear between files
```
file1.csv: [id, name, age]
file2.csv: [id, name, age, country]
file3.csv: [id, name]
```

**Strategy: Union Schema**
```
Union: [id, name, age, country]

When reading:
- file1: country → null
- file2: all columns ✓
- file3: age → null, country → null
```

**Implementation Hint:**
```
Keep schema as: {column: (type, required: bool)}
For optional columns, fill missing with null/default
```

---

## Memory Management Strategies

### Problem: Large Datasets Don't Fit in RAM

**Strategy 1: Chunked Processing**
```
Don't do: 
  rows = list(all_data)  # OOM!
  filtered = [r for r in rows if ...]

Do:
  for chunk in read_chunks(1000):  # Process 1000 at a time
      for row in chunk:
          if filter(row):
              yield row
```

**Strategy 2: Streaming Aggregations**
```
Don't do:
  all_groups = {}
  for row in all_data:  # Loads everything
      ...
  
Do:
  groups = {}  # Only unique groups
  for row in stream:
      key = row['group_key']
      groups[key] = update_aggregate(groups[key], row)
```

Memory usage: O(unique groups) not O(total rows)

**Strategy 3: External Sorting (for ORDER BY)**
```
When dataset > RAM:
1. Read chunks that fit in memory
2. Sort each chunk
3. Write sorted chunks to disk
4. Merge sorted chunks (merge sort)

Example:
- 10GB data, 1GB RAM
- Create 10 sorted 1GB chunks
- Merge 10 chunks (only need to keep head of each in memory)
```

**Strategy 4: Approximate Algorithms**
```
For analytics, exact answer not always needed:

COUNT(DISTINCT):
  Exact: Store all unique values (high memory)
  Approx: HyperLogLog (fixed 1.5KB, 2% error)

TOP-K:
  Exact: Sort all data
  Approx: Count-Min Sketch + Heap (fixed memory)

PERCENTILE:
  Exact: Store all values
  Approx: T-Digest (fixed size, 1% error)
```

---

## CLI Design Philosophy

### User Experience Principles

**Principle 1: Sensible Defaults**
```bash
# Should just work
sqlstream data.csv "SELECT * WHERE age > 25"

# Not require:
sqlstream data.csv "SELECT * WHERE age > 25" \
  --format csv \
  --output stdout \
  --encoding utf-8 \
  --delimiter comma
```

**Principle 2: Discoverable Features**
```bash
# Show what's possible
sqlstream data.csv --help

# Show file info
sqlstream data.csv --info

# Explain query plan
sqlstream data.csv "SELECT ..." --explain
```

**Principle 3: Pipe-Friendly**
```bash
# Should work in pipelines
cat data.json | sqlstream "SELECT * WHERE active = true" | jq

# Output should be line-delimited JSON by default for pipes
```

### CLI Subcommands

**Option A: Verb-Noun Pattern**
```bash
sqlstream query data.csv "SELECT ..."
sqlstream profile data.csv
sqlstream schema data.csv
sqlstream convert data.csv --to parquet
```

**Option B: Direct Pattern (Simpler)**
```bash
sqlstream data.csv "SELECT ..."  # Implied 'query'
sqlstream --profile data.csv
sqlstream --schema data.csv
```

**Recommendation:** Option B for common operations, subcommands for advanced

---

## Query Optimization - Decision Trees

### When to Pushdown Predicates?

```
Decision Tree:

Is data source a file format? (Parquet/CSV/JSON)
├─ YES → Can pushdown to file reader
│  └─ Is format columnar? (Parquet)
│     ├─ YES → Can use statistics-based pruning
│     └─ NO → Simple row-by-row filtering
└─ NO → Is it a remote source? (HTTP/S3)
   ├─ YES → Can pushdown to server?
   │  ├─ YES (S3 Select) → Pushdown
   │  └─ NO → Download with filtering
   └─ NO → In-memory iteration
```

### When to Materialize vs Stream?

```
Operation Requires Materialization?

SELECT      → No (lazy projection)
WHERE       → No (lazy filter)
LIMIT       → No (early stop)
GROUP BY    → Partial (incremental aggregation)
ORDER BY    → Yes (must sort all data)
JOIN        → Partial (one side in memory)
DISTINCT    → Yes (need to track all seen values)
  └─ Unless: DISTINCT on indexed column → Can use index
```

### Join Strategy Selection

```
Given: Table A (size N), Table B (size M)

If min(N, M) < 10MB:
  → Hash Join (fits in memory)

Else if N << M or M << N:
  → Hash Join with smaller table as build side
  → Add Bloom Filter for large table

Else if both large:
  → Sort-Merge Join (external sort both sides)
  → OR: Partition both tables, join partitions

Special case: If one table already sorted on join key:
  → Merge Join (no sort needed!)
```

---

## Extension Points & Plugin Architecture

### Why Extensibility Matters

Users have custom needs:
- New file formats (Avro, ORC)
- Custom data sources (databases, APIs)
- Domain-specific functions
- Custom aggregations

### Plugin Pattern

**Reader Plugin Interface:**
```
BaseReader:
  - read_lazy() → Iterator
  - supports_pushdown() → bool
  - supports_column_selection() → bool
  - set_filters(filters) → void
  - set_columns(columns) → void
```

**User Defines:**
```python
class PostgreSQLReader(BaseReader):
    def __init__(self, connection_string):
        self.conn = connect(connection_string)
    
    def supports_pushdown(self):
        return True  # Can pushdown to SQL WHERE
    
    def read_lazy(self):
        # Use server-side cursor
        cursor = self.conn.cursor()
        cursor.execute(self.build_query())
        for row in cursor:
            yield row
```

**Registration:**
```python
query.register_reader('postgres', PostgreSQLReader)

# Now users can:
query("postgres://localhost/db?table=users").sql("SELECT ...")
```

### Custom Function Pattern

```python
# User defines
def my_custom_agg(values):
    return sum(values) / len(values)

# Register
query.register_function('MY_AVG', my_custom_agg)

# Use in SQL
query(data).sql("SELECT group, MY_AVG(value) FROM data GROUP BY group")
```

---

## Error Handling & User Feedback

### Error Categories

**1. User Errors (Actionable)**
```
Bad SQL syntax → Show exactly what's wrong
File not found → Suggest similar files
Type mismatch → Show expected vs actual type
```

**2. Data Errors (Recoverable)**
```
Malformed row → Skip with warning, continue
Missing column → Use null, warn user
Type coercion failure → Best effort conversion
```

**3. System Errors (Fatal)**
```
Out of memory → Clear message, suggest --streaming
Network failure → Retry logic, timeout
Permission denied → Clear explanation
```

### User Feedback Levels

**Verbose Mode:**
```
[INFO] Reading file.parquet (1000 rows, 10 columns)
[INFO] Pushed down filter: age > 25
[INFO] Skipped 8 of 10 row groups
[INFO] Read 200 rows in 0.1s
```

**Default (Quiet):**
```
(No output unless error)
```

**Explain Mode:**
```
Query Plan:
  1. Scan file.parquet
     - Row groups: 10 → 2 (pruned 8)
     - Columns: all → [id, name, age] (pruned 7)
     - Predicate pushdown: age > 25
  2. Project: SELECT id, name
  3. Limit: 10
  
Estimated cost: Low (2 row groups × 3 columns)
```

---

## Testing Strategy

### Test Pyramid

**Unit Tests (Fast, Many):**
- Parser: SQL → AST
- Optimizer: Plan transformations
- Filters: Predicate evaluation
- Each reader in isolation

**Integration Tests (Medium, Some):**
- End-to-end queries
- Multi-source queries
- Optimization effectiveness
- File format compatibility

**Performance Tests (Slow, Few):**
- Large file benchmarks
- Memory usage profiling
- Streaming performance
- HTTP range request efficiency

### Test Data Generation

**Small datasets (for unit tests):**
```
10 rows × 5 columns
Fast to generate, fast to test
All edge cases covered
```

**Medium datasets (for integration):**
```
10K rows × 20 columns
Multiple data types
Some nulls, duplicates
```

**Large datasets (for performance):**
```
1M+ rows
Real-world distributions
Test memory limits
```

### Property-Based Testing

Instead of fixed examples, test properties:
```
Property: "Filter then count should equal count with filter"

For all datasets D and filters F:
  count(filter(D, F)) == count_with_filter(D, F)

Generate random D and F, verify property holds
```

---

## Performance Benchmarking

### What to Measure

**Throughput:**
- Rows/second processed
- Varies by operation (scan vs aggregation)

**Latency:**
- Time to first result
- Important for interactive use

**Memory:**
- Peak memory usage
- Memory per row processed

**I/O:**
- Bytes read from disk/network
- I/O reduction from optimizations

### Benchmark Scenarios

**Scenario 1: Selective Filter**
```
Query: SELECT * FROM 1M rows WHERE rare_condition
Expected: Should skip 99% of data
Measure: I/O reduction, time improvement
```

**Scenario 2: Aggregation**
```
Query: SELECT category, COUNT(*) FROM 1M rows GROUP BY category
Expected: Memory = O(unique categories)
Measure: Memory usage, time
```

**Scenario 3: Remote Parquet**
```
Query: SELECT * FROM http://large.parquet WHERE id = 123
Expected: Download <1% of file
Measure: Network bytes transferred
```

**Comparison Points:**
- Naive Python (no optimizations)
- DuckDB (gold standard for speed)
- Pandas (common alternative)
- Your implementation

---

## Future Enhancements (Post-MVP)

### Phase 2 Features

**1. Write Support**
```
query(data).sql("SELECT ...").to_parquet("output.parquet")
  with compression, partitioning, row group size control
```

**2. Incremental Processing**
```
query("logs/*.json").sql("SELECT ...").checkpoint("process.state")
  remembers what's been processed, only reads new files
```

**3. Parallel Execution**
```
query("data.parquet").sql("SELECT ...").parallel(workers=4)
  partition data across workers, merge results
```

**4. Query Compilation**
```
query(data).sql("SELECT ...").compile()
  generate Python code for query, JIT compile
```

### Phase 3 Features

**1. Distributed Mode**
```
query.cluster(["worker1", "worker2"]).sql("SELECT ...")
  distribute data and computation
```

**2. Real-time Streaming**
```
query.stream("kafka://topic").sql("SELECT ... GROUP BY TUMBLE(1 minute)")
  window aggregations on streams
```

**3. ML Integration**
```
query(data).sql("SELECT *, PREDICT(model, features) FROM data")
  embed ML predictions in queries
```

---

## Implementation Gotchas & Lessons

### Gotcha 1: ORDER BY Kills Streaming

**Problem:** Can't yield sorted results until all data is seen

**Solutions:**
- Warn user that ORDER BY materializes
- Offer LIMIT + ORDER BY optimization (top-K heap)
- Consider approximate sorting for large data

### Gotcha 2: GROUP BY Memory Explosion

**Problem:** Unique groups can be very large

**Solutions:**
- Monitor group count, warn at threshold
- Offer approximate GROUP BY (streaming algorithms)
- Suggest pre-aggregation or filtering

### Gotcha 3: HTTP Range Requests Not Universal

**Problem:** Not all servers support ranges

**Solutions:**
- Detect support (Accept-Ranges header)
- Fallback to streaming full file
- Cache-friendly: download once, cache locally

### Gotcha 4: Type Inference Ambiguity

**Problem:** "123" could be string or number

**Solutions:**
- Explicit type hints from user
- Consistent heuristics (numeric strings → numbers)
- Allow schema override

### Gotcha 5: SQL Dialect Differences

**Problem:** Users expect different SQL flavors

**Solutions:**
- Document supported syntax clearly
- Support common subset (Postgres-like)
- Clear error messages for unsupported syntax

---

## Success Metrics

**Technical Metrics:**
- Package size < 250KB ✓
- Query 1M rows CSV in <5s ✓
- Memory < 100MB for 1GB Parquet ✓
- Parquet row group pruning works ✓
- HTTP range requests reduce transfer ✓

**User Metrics:**
- Time from install to first query < 2 min
- Queries feel "instant" (<1s for exploration)
- Clear error messages (user can fix without docs)
- Works on Python 3.8+ without compilation

**Adoption Metrics:**
- GitHub stars, PyPI downloads
- Issues opened (engagement)
- Contributors (extensibility working)

---

## Documentation Structure

**README.md:**
- Quick start (3 examples)
- Installation
- Link to full docs

**TUTORIAL.md:**
- Step-by-step examples
- Common use cases
- Best practices

**API.md:**
- Full API reference
- All parameters explained
- Return types documented

**OPTIMIZATION.md:**
- How optimizations work
- When they trigger
- How to verify (--explain)

**EXTENDING.md:**
- Plugin architecture
- Custom readers
- Custom functions

**CONTRIBUTING.md:**
- Code structure
- How to add features
- Testing guidelines

---

This document provides the conceptual foundation. Implementation is iterative - start simple, add optimizations, measure, improve. The architecture supports both quick scripts and production workloads.