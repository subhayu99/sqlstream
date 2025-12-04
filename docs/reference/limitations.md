# Limitations & Known Issues

This document outlines current limitations and known issues in SQLStream to set proper expectations.

---

## SQL Features

### Supported SQL (Python/Pandas Backends)

The default Python and Pandas backends support a subset of standard SQL-92:

- `SELECT` with `DISTINCT`
- `FROM` (single table or JOINs)
- `WHERE` (filtering with AND/OR)
- `GROUP BY`
- `ORDER BY` (ASC/DESC)
- `LIMIT`
- `JOIN` (INNER, LEFT, RIGHT, CROSS)
- Aggregates: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`

### Full SQL Support (DuckDB Backend) ✅

**To get full SQL support, use the DuckDB backend!**

By using `backend="duckdb"`, you unlock:
- ✅ Window Functions (`OVER`, `PARTITION BY`, `RANK`, etc.)
- ✅ Common Table Expressions (CTEs / `WITH` clause)
- ✅ Subqueries (in `FROM`, `WHERE`, `SELECT`)
- ✅ `HAVING` clause
- ✅ Set operations (`UNION`, `INTERSECT`, `EXCEPT`)
- ✅ Advanced functions (String, Date, Math)

### Not Supported (Python/Pandas Backends)

If you are NOT using the DuckDB backend, the following are **NOT** supported:

- Window Functions (`OVER`, `PARTITION BY`)
- CTEs (`WITH`)
- Subqueries
- `HAVING` clause
- `UNION` / `INTERSECT` / `EXCEPT`
- `CASE` statements
- Complex expressions in `GROUP BY` or `ORDER BY`ers AS (
    SELECT * FROM employees WHERE salary > 100000
)
SELECT * FROM high_earners WHERE department = 'Engineering'
**Workaround**: Break into multiple queries or use pandas for complex transformations.

---

#### Window Functions
```sql
-- ❌ NOT SUPPORTED
SELECT 
    name,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank
FROM employees
```

**Supported alternatives**: Use pandas with `df.groupby().rank()` or DuckDB.

**Planned**: Future roadmap item (Phase 12+)

---

#### Advanced Aggregations
```sql
-- ✅ SUPPORTED: Basic aggregations
SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department

-- ❌ NOT SUPPORTED: DISTINCT in aggregates
SELECT COUNT(DISTINCT department) FROM employees

-- ❌ NOT SUPPORTED: HAVING
SELECT department, AVG(salary) as avg_sal 
FROM employees 
GROUP BY department 
HAVING avg_sal > 80000

-- ❌ NOT SUPPORTED: Statistical functions
SELECT STDDEV(salary), VARIANCE(salary) FROM employees
```

**Workarounds**: Use pandas for statistical functions, use WHERE after GROUP BY (inefficient).

---

#### UNION/INTERSECT/EXCEPT
```sql
-- ❌ NOT SUPPORTED
SELECT * FROM file1.csv
UNION
SELECT * FROM file2.csv
```

**Workaround**: Concatenate files manually or use pandas `pd.concat()`.

---

#### Date/Time Functions
```sql
-- ❌ LIMITED SUPPORT: Date operations
SELECT DATE_ADD(date_col, INTERVAL 1 DAY) FROM data  -- Not supported
SELECT CURRENT_DATE()  -- Not supported
SELECT EXTRACT(YEAR FROM date_col) FROM data  -- Not supported
```

**Current support**: Basic date comparisons only
```sql
-- ✅ SUPPORTED
SELECT * FROM data WHERE date_col > '2024-01-01'
SELECT * FROM data WHERE date_col BETWEEN '2024-01-01' AND '2024-12-31'
```

**Workaround**: Preprocess dates with pandas.

---

#### String Functions
```sql
-- ❌ LIMITED SUPPORT
SELECT UPPER(name), LOWER(name), CONCAT(first, last) FROM data  -- Not supported
SELECT SUBSTRING(name, 1, 5) FROM data  -- Not supported
SELECT TRIM(name) FROM data  -- Not supported
```

**Current support**: Only basic column selection and filtering.

**Workaround**: Use pandas string methods.

---

#### CASE Statements
```sql
-- ❌ NOT SUPPORTED
SELECT 
    name,
    CASE 
        WHEN salary > 100000 THEN 'High'
        WHEN salary > 50000 THEN 'Medium'
        ELSE 'Low'
    END as salary_category
FROM employees
```

**Workaround**: Use pandas `apply()` or `np.select()`.

---

#### SELF JOINS
```sql
-- ❌ NOT RELIABLY SUPPORTED
SELECT a.name, b.name
FROM employees a
JOIN employees b ON a.manager_id = b.id
```

**Status**: May work in simple cases, not tested thoroughly.

**Workaround**: Use DuckDB or pandas merge.

---

### Write Operations

SQLStream is **read-only**. No support for:

- ❌ `INSERT INTO`
- ❌ `UPDATE`
- ❌ `DELETE`
- ❌ `CREATE TABLE`
- ❌ `ALTER TABLE`
- ❌ `DROP TABLE`
- ❌ `CREATE VIEW`
- ❌ `CREATE INDEX`
- ❌ Transactions (`BEGIN`, `COMMIT`, `ROLLBACK`)

**For data modification**: Use pandas, DuckDB, or traditional databases.

---

## File Format Limitations

### CSV

**Size limits**:
- Python backend: ~100 MB comfortably (RAM limited)
- Pandas backend: up to available RAM

**Encoding**:
- ✅ UTF-8 supported
- ❌ Auto-detection of other encodings not implemented
- Workaround: Convert to UTF-8 first

**Delimiters**:
- ✅ Comma (,) - default
- ❌ Custom delimiters not configurable via SQL
- Workaround: Use Python API with custom reader settings

**Special cases**:
- ❌ Multi-line cells with newlines - may cause parsing issues
- ❌ Files without headers - not supported
- ❌ Multiple CSV formats in one query - all must use same format

---

### Parquet

**Size limits**:
- Typically 10x larger than CSV limit due to compression
- Streaming supported for S3

**Version support**:
- Depends on pyarrow version
- Older Parquet formats may not work

**Compression**:
- ✅ Snappy, Gzip, LZ4, ZSTD (via pyarrow)
- Decompression automatic

**Schema evolution**:
- ❌ Not supported - all files must have same schema for JOINs
- ❌ Column addition/removal between versions not handled

---

### HTML Tables

**Limitations**:
- Only tables that `pandas.read_html()` can parse
- ❌ JavaScript-rendered tables not supported
- ❌ Complex nested tables may fail
- ❌ No table selection by class/id (only by index)

**Workaround**: Save HTML locally and inspect table indices.

---

### Markdown Tables

**Limitations**:
- Only GitHub Flavored Markdown tables
- ❌ No support for HTML tables in markdown
- ❌ Tables must be well-formed (aligned pipes)
- ❌ No support for merged cells

---

## Data Source Limitations

### S3

**Authentication**:
- ✅ AWS credentials (env vars, config, IAM roles)
- ❌ Session tokens with expiration not well handled
- ❌ SSO/SAML auth not supported

**Regions**:
- ✅ Works with all AWS regions
- ⚠️ Cross-region access may be slow
- ⚠️ Some operations require correct region setting

**Performance**:
- First query downloads entire file (no streaming for CSV)
- Parquet uses column-level streaming (better)
- No support for S3 Select

**Permissions**:
- Requires `s3:GetObject` permission
- Does not support bucket listing for discovery

**Features not supported**:
- ❌ S3 versioning
- ❌ Requester pays buckets
- ❌ S3 Glacier / Deep Archive
- ❌ S3 Batch Operations

**Compatible services**:
- ✅ MinIO (mostly compatible)
- ✅ DigitalOcean Spaces
- ❌ Google Cloud Storage (use HTTP URLs instead)
- ❌ Azure Blob Storage

---

### HTTP/HTTPS

**Limitations**:
- ❌ No authentication (Basic Auth, Bearer tokens, etc.)
- ❌ No custom headers
- ❌ No POST requests
- ❌ No timeout configuration
- ❌ No retry logic for failures
- ✅ Basic caching (local directory)

**SSL/TLS**:
- ✅ HTTPS supported
- ❌ Certificate verification can't be disabled
- ❌ Client certificates not supported

**Large files**:
- Downloads entire file before processing
- Not ideal for multi-GB files over HTTP

---

### Local Files

**Path issues**:
- ✅ Relative paths supported
- ✅ Absolute paths supported
- ❌ `~` (home directory) expansion - not implemented
- ❌ Glob patterns (`*.csv`) - not supported

**Permissions**:
- Requires read access
- No special handling for permission errors

---

## Performance Limitations

### Memory

**Python backend**:
- Loads entire result set into memory
- No streaming for large result sets
- Risk of `MemoryError` with large data

**Pandas backend**:
- Better memory management
- Still loads results into RAM
- Use LIMIT for very large queries

---

### Concurrency

- ❌ No parallel query execution
- ❌ No multi-threading for single query
- ❌ No connection pooling

**Workaround**: Run multiple Python processes for parallel queries.

---

### Caching

**HTTP sources**:
- Basic file-level caching
- No TTL or cache invalidation
- Cache directory not configurable via SQL

**S3 sources**:
- No caching (downloads each time)
- No query result caching

---

## Platform Limitations

### Windows

**Known issues**:
- Interactive shell may not render perfectly in CMD (use Windows Terminal)
- File paths: Use forward slashes or double backslashes

**Supported shells**:
- ✅ PowerShell
- ✅ Windows Terminal
- ⚠️ CMD (basic support)
- ✅ Git Bash

---

### Python Versions

**Supported**:
- ✅ Python 3.10+
- ✅ Python 3.11
- ✅ Python 3.12
- ✅ Python 3.13

**Not supported**:
- ❌ Python 2.7
- ❌ Python 3.9 and older

---

## Type System Limitations

### Type Inference

**CSV type detection**:
- Samples only first 100 rows
- May mis-detect types if early rows not representative
- No manual type specification

**Ambiguous types**:
- Dates without time may be detected as strings
- Large integers may overflow
- Mixed types in column default to string

**NULL handling**:
- Empty strings vs NULL not distinguished in CSV
- Parquet NULL handling depends on file

---

### Type Coercion

- ❌ No automatic type coercion in JOINs (int != float)
- ❌ No CAST function to convert types
- Limited type checking before operations

**Workaround**: Preprocess with pandas.

---

## Known Bugs

### Critical

None currently identified.

### High

1. **JOIN performance on large files**: O(n*m) complexity for nested loop joins
   - **Workaround**: Use pandas backend or filter first
   - **Planned fix**: Phase 12 - Hash joins

2. **Memory leaks with repeated queries** in interactive shell
   - **Workaround**: Restart shell periodically
   - **Investigation**: Ongoing

### Medium

3. **Tab completion in interactive shell** doesn't suggest all table names
   - **Workaround**: Type full table name
   - **Planned fix**: Phase 10

4. **Error messages** can be cryptic for SQL syntax errors
   - **Workaround**: Check query syntax carefully
   - **Planned fix**: Phase 10 - Better error messages

5. **File browser doesn't show hidden files** in interactive shell
   - **Workaround**: Type path manually
   - **Planned fix**: Add option to show hidden files

### Low

6. **Query history** limited to 100 entries
   - **Workaround**: Export important queries
   - **Enhancement**: Make configurable

7. **No syntax highlighting** in non-interactive mode
   - **Workaround**: Use interactive shell
   - **Enhancement**: Support --pretty flag

---

## Comparison with Other Tools

### vs DuckDB

**DuckDB advantages**:
- 100-1000x faster
- Full SQL support
- Production ready
- ACID transactions
- Write operations

**SQLStream advantages**:
- Simpler installation
- Pure Python (no compilation)
- Educational (understand internals)
- Lighter weight

**Recommendation**: Use DuckDB for production, SQLStream for learning/light analysis.

---

### vs pandas

**pandas advantages**:
- More data manipulation functions
- Better performance for transformations
- Wider adoption
- Jupyter integration

**SQLStream advantages**:
- SQL syntax (if you know SQL)
- Lazy evaluation
- Join multiple files without loading all

**Recommendation**: Use SQLStream to filter/join files, then pandas for analysis.

---

### vs sqlite

**sqlite advantages**:
- Full ACID database
- Mature and stable
- Built into Python
- Write operations

**SQLStream advantages**:
- No database setup
- Query files directly
- S3 and HTTP sources
- Better for file-based workflows

**Recommendation**: Use sqlite for structured data with updates, SQLStream for read-only file analysis.

---

## Workarounds & Best Practices

### 1. Complex Queries

**Problem**: Need features not supported (CTEs, window functions, etc.)

**Solutions**:
1. Use DuckDB for complex queries
2. Use pandas for transformations
3. Break into multiple queries with temporary files
4. Use Python to combine results

---

### 2. Large Files

**Problem**: Files too large for memory

**Solutions**:
1. Use pandas backend: `--backend pandas`
2. Use LIMIT to sample data
3. Use Parquet instead of CSV
4. Filter with WHERE clause before loading
5. Select only needed columns

---

### 3. Performance

**Problem**: Queries are slow

**Solutions**:
1. Use pandas backend
2. Convert CSV to Parquet
3. Add WHERE filters
4. Select specific columns (not `*`)
5. Index on frequently joined columns (Parquet metadata)
6. Use LIMIT for exploration

---

### 4. Type Issues

**Problem**: Wrong types inferred

**Solutions**:
1. Preprocess with pandas
2. Use Parquet with explicit types
3. Ensure first 100 rows are representative
4. Cast in Python after query

---

## Reporting Issues

Found a bug or limitation not listed here?

1. **Check**: [GitHub Issues](https://github.com/subhayu99/sqlstream/issues)
2. **Report**: Create new issue with:
   - SQLStream version
   - Python version
   - Minimal reproduction code
   - Expected vs actual behavior

3. **Discuss**: Use [GitHub Discussions](https://github.com/subhayu99/sqlstream/discussions) for feature requests

---

## Roadmap

See [Project Status](../index.md#project-status) for upcoming features.

**Planned improvements**:
- Phase 10: Better error messages
- Phase 11: Testing & documentation
- Phase 12+: Subqueries, CTEs, window functions, hash joins

---

## See Also

- [FAQ](../guides/faq.md) - Common questions
- [Troubleshooting](../guides/troubleshooting.md) - Error solutions
- [SQL Support](../features/sql-support.md) - Supported SQL syntax
- [Performance Guide](../guides/performance.md) - Optimization tips (coming soon)
