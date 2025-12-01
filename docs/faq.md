# Frequently Asked Questions (FAQ)

---

## General Questions

### How is SQLStream different from DuckDB?

**DuckDB** is a fully-featured embedded database with a complete SQL implementation, optimized for OLAP workloads.

**SQLStream** is a lightweight Python library focused on:
- **Simplicity**: Pure Python, minimal dependencies
- **Learning**: Understand query execution internals
- **Portability**: Works anywhere Python runs
- **File-first**: Query files without database setup

**Use DuckDB if**: You need maximum performance, full SQL compatibility, or production OLAP workloads.

**Use SQLStream if**: You want a lightweight tool for data exploration, learning query engines, or quick CSV/Parquet analysis.

---

### How is SQLStream different from pandas?

**pandas** is a data manipulation library with DataFrame API.

**SQLStream** provides SQL interface for:
- **Familiar syntax**: Use SQL instead of DataFrame methods
- **Lazy evaluation**: Process large files efficiently
- **Join multiple files**: Combine datasets without loading all into memory
- **SQL users**: Leverage existing SQL knowledge

**Interoperability**: You can use pandas backend for performance (`--backend pandas`) or convert SQLStream results to pandas DataFrames.

---

### Can I use this in production?

**Current status**: SQLStream is in active development and suitable for:
- ✅ Data exploration and analysis
- ✅ ETL scripts and data pipelines
- ✅ CI/CD data processing
- ✅ Learning and prototyping

**Considerations**:
- 🚧 API may change between versions
- 🚧 Limited SQL feature set (no WINDOW functions, CTEs, etc.)
- 🚧 No transaction support (read-only)
- ✅ Good test coverage (377 tests, 53%)

**Recommendation**: Use for non-critical workloads or alongside other tools. Consider DuckDB for mission-critical production systems.

---

### What's the maximum file size?

**Python Backend**:
- **RAM limited**: Can handle files up to available memory
- **Typical**: 10-100 MB comfortably
- **Large files**: Use `LIMIT` or pandas backend

**Pandas Backend**:
- **Much larger**: 100 MB - 10 GB+ depending on RAM
- **Chunk processing**: Automatically handles larger-than-memory with streaming

**S3 files**:
- **Streaming**: Data is streamed, not loaded entirely into memory
- **Practical limit**: 10 GB+ for Parquet, smaller for CSV

**Best practices**:
```bash
# Preview large files
sqlstream query large.csv "SELECT * FROM large LIMIT 1000" --backend pandas

# Use Parquet for better compression
sqlstream query large.parquet "SELECT * FROM large WHERE date > '2024-01-01'" --backend pandas
```

---

### Does it work on Windows?

**Yes!** SQLStream is pure Python and works on:
- ✅ **Windows** (Windows 10/11, Server)
- ✅ **macOS** (Intel and Apple Silicon)
- ✅ **Linux** (Ubuntu, Debian, CentOS, etc.)

**Platform-specific notes**:
- **Windows**: Use PowerShell or CMD
- **Interactive shell**: Works best in Windows Terminal (not CMD)
- **File paths**: Use forward slashes `/` or escape backslashes `\\`

---

### Can I use it with Jupyter notebooks?

**Yes!** SQLStream works great in Jupyter:

```python
# Install in notebook
!pip install "sqlstream[all]"

# Use in cells
from sqlstream import query

results = query("data.csv").sql("""
    SELECT department, AVG(salary) as avg_salary
    FROM data
    GROUP BY department
""")

# Display as DataFrame
import pandas as pd
df = pd.DataFrame(results.to_list())
df
```

**Tips**:
- Use `to_list()` for small results
- Use iteration for large results to avoid memory issues
- Consider DuckDB for faster notebook performance

See [Jupyter Integration Guide](integrations/jupyter.md) (coming soon) for more details.

---

### How do I report bugs?

1. **Check existing issues**: [GitHub Issues](https://github.com/subhayu99/sqlstream/issues)
2. **Create new issue**: Include:
   - SQLStream version (`pip show sqlstream`)
   - Python version
   - Operating system
   - Minimal reproduction code
   - Error message with full traceback
   - Expected vs actual behavior

**Example**:
```markdown
## Bug Report

**Environment:**
- SQLStream: 0.2.5
- Python: 3.11.0
- OS: Ubuntu 22.04

**Code:**
\```python
from sqlstream import query
results = query("data.csv").sql("SELECT * FROM data")
\```

**Error:**
\```
TypeError: ...
\```

**Expected:** Should return results
**Actual:** Raises TypeError
```

---

### How do I request features?

1. **Check roadmap**: See [Project Status](../index.md#project-status)
2. **Search discussions**: [GitHub Discussions](https://github.com/subhayu99/sqlstream/discussions)
3. **Create discussion**: Use "Ideas" category
4. **Include**:
   - Use case description
   - Example of desired behavior
   - Why existing features don't work
   - Willingness to contribute

**What features are planned?**
- See [Development Status](../index.md#project-status) for roadmap
- Phase 10: Error handling & user feedback
- Phase 11: Testing & documentation
- Future: WINDOW functions, CTEs, subqueries

---

### Is there commercial support?

**Currently**: No commercial support or SLA.

**Community support**:
- [GitHub Discussions](https://github.com/subhayu99/sqlstream/discussions)
- [GitHub Issues](https://github.com/subhayu99/sqlstream/issues)
- Documentation: [https://subhayu99.github.io/sqlstream](https://subhayu99.github.io/sqlstream)

**For enterprises**: Consider DuckDB, which has commercial support options.

---

## Installation & Setup

### What dependencies does SQLStream have?

**Minimal (CSV only)**:
```bash
pip install sqlstream
# Only requires: Python standard library
```

**Recommended (all features)**:
```bash
pip install "sqlstream[all]"
# Includes: pandas, pyarrow, s3fs, textual, httpx
```

**By feature**:
- `sqlstream[parquet]` - Parquet support (pyarrow)
- `sqlstream[pandas]` - Pandas backend (pandas)
- `sqlstream[duckdb]` - DuckDB backend (duckdb)
- `sqlstream[s3]` - S3 support (s3fs)
- `sqlstream[cli]` - Basic CLI shell (click)
- `sqlstream[interactive]` - Full blown TUI (textual)
- `sqlstream[http]` - HTTP data sources (httpx)
- `sqlstream[html]` - HTML parsing (lxml)

---

### How do I upgrade SQLStream?

```bash
# Upgrade to latest version
pip install --upgrade sqlstream

# Upgrade with all features
pip install --upgrade "sqlstream[all]"

# Check current version
pip show sqlstream
# or
sqlstream --version
```

**Breaking changes?** Check [Changelog](changelog.md) (coming soon).

---

## Usage Questions

### Can I query multiple files at once?

**Yes!** Use JOINs:

```sql
SELECT e.name, d.department_name, e.salary
FROM 'employees.csv' e
JOIN 'departments.csv' d ON e.dept_id = d.id
WHERE e.salary > 80000
```

**Limitations**:
- All files must be accessible (local or remote)
- JOIN performance depends on file sizes
- Use pandas backend for better performance

---

### Can I use aggregate functions?

**Yes!** Supported aggregations:

```sql
SELECT
    department,
    COUNT(*) as count,
    AVG(salary) as avg_salary,
    MIN(salary) as min_salary,
    MAX(salary) as max_salary,
    SUM(salary) as total_salary
FROM 'employees.csv'
GROUP BY department
ORDER BY avg_salary DESC
```

**Supported**: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`  
**Not yet**: `COUNT(DISTINCT)`, `STDDEV`, `VARIANCE`, custom aggregates

---

### Can I use subqueries or CTEs?

**Not yet.** Currently not supported:
- ❌ Subqueries in FROM clause
- ❌ Common Table Expressions (WITH clause)
- ❌ Correlated subqueries
- ❌ Subqueries in WHERE clause

**Workarounds**:
1. Save intermediate results
2. Use temporary files
3. Use pandas for complex queries

**Planned**: Future roadmap item

---

### How do I specify column types manually?

**Currently**: Types are inferred automatically.

**CSV type inference**:
- Samples first 100 rows
- Detects: int, float, string, date, datetime, boolean

**Parquet**: Types come from file metadata

**Manual types**: Not yet supported. Planned for future release.

**Workaround**: Use CAST (if implemented) or process with pandas first.

---

### Can I write data (INSERT, UPDATE, DELETE)?

**No.** SQLStream is **read-only**.

**Supported**: `SELECT` queries only

**Not supported**:
- ❌ INSERT
- ❌ UPDATE
- ❌ DELETE
- ❌ CREATE TABLE
- ❌ ALTER TABLE

**For data modification**: Use pandas, DuckDB, or traditional databases.

---

## Performance & Optimization

### Why is my query slow?

**Common reasons**:

1. **Using Python backend on large files**
   - Solution: Use `--backend pandas`

2. **Not using WHERE filters**
   - Solution: Add filters to reduce data scanned

3. **Using SELECT \***
   - Solution: Select only needed columns

4. **Large JOINs**
   - Solution: Filter before joining

5. **CSV vs Parquet**
   - Solution: Convert to Parquet for better performance

**Example optimization**:
```bash
# Slow (Python backend, all columns, all rows)
sqlstream query large.csv "SELECT * FROM large"

# Fast (pandas, filtered, specific columns)
sqlstream query large.parquet \
  "SELECT name, salary FROM large WHERE date > '2024-01-01'" \
  --backend pandas
```

### What backends are supported?

SQLStream supports three execution backends:

1. **Python (Default/Educational)**: Pure Python implementation of the Volcano model. Great for learning how databases work, but slower for large data.
2. **Pandas**: Translates SQL to pandas operations. 10-100x faster than Python backend. Best for general use.
3. **DuckDB (New!)**: Uses DuckDB's engine for **full SQL support** (window functions, CTEs, etc.) and maximum performance. 10-1000x faster.

### How do I use the DuckDB backend?

Install with `pip install duckdb` or `pip install "sqlstream[duckdb]"`.

Then use it in your code:
```python
query("data.csv").sql("...", backend="duckdb")
```

Or via CLI:
```bash
sqlstream query "..." --backend duckdb
```

### Does DuckDB backend support all file formats?

**Yes!** The DuckDB backend uses SQLStream's unified reader architecture, so it supports:
- CSV, Parquet, JSON
- HTML tables, Markdown tables
- S3 files (s3://)
- HTTP/HTTPS URLs

It automatically handles authentication and caching just like the other backends.

See [Performance Guide](performance.md) (coming soon) for details.

---

### Should I use CSV or Parquet?

**Parquet** is almost always better:

| Feature | CSV | Parquet |
|---------|-----|---------|
| **Read Speed** | Baseline | 10-100x faster |
| **File Size** | Large | 2-10x smaller |
| **Type Safety** | Inferred | Stored in file |
| **Column Access** | Read all | Columnar (faster) |
| **Compression** | None/gzip | Snappy/Gzip/LZ4 |

**Use CSV when**:
- Need human-readable format
- Editing files manually
- Tool doesn't support Parquet

**Use Parquet when**:
- Performance matters
- Large files
- Production pipelines

**Convert CSV to Parquet**:
```python
import pandas as pd
df = pd.read_csv('data.csv')
df.to_parquet('data.parquet')
```

---

## Error Messages

### "No module named 'textual'"

**Problem**: Interactive shell not installed

**Solution**:
```bash
pip install "sqlstream[cli]"
# or
pip install "sqlstream[all]"
```

---

### "No module named 'pandas'"

**Problem**: Pandas backend not installed

**Solution**:
```bash
pip install "sqlstream[pandas]"
# or
pip install "sqlstream[all]"
```

---

### "File not found" error

**Possible causes**:

1. **Wrong path**:
   ```bash
   # Wrong
   sqlstream query data.csv "SELECT * FROM data"
   
   # Correct (use file path in query)
   sqlstream query "SELECT * FROM 'data.csv'"
   ```

2. **Relative vs absolute path**:
   ```bash
   # Use absolute path if unsure
   sqlstream query "SELECT * FROM '/home/user/data.csv'"
   ```

3. **Path with spaces**:
   ```sql
   -- Use quotes
   SELECT * FROM 'my data.csv'
   ```

---

### S3 authentication errors

**Problem**: Can't access S3 files

**Solutions**:

1. **Set AWS credentials**:
   ```bash
   export AWS_ACCESS_KEY_ID=your_key
   export AWS_SECRET_ACCESS_KEY=your_secret
   export AWS_DEFAULT_REGION=us-east-1
   ```

2. **Use AWS config**:
   ```bash
   aws configure
   ```

3. **Check bucket permissions**: Ensure you have read access

4. **Check region**: Some buckets require specific region

See [S3 Support](features/s3-support.md) for details.

---

## Contributing

### How can I contribute?

**Ways to contribute**:

1. **Report bugs**: See "How do I report bugs?" above
2. **Request features**: See "How do I request features?" above
3. **Fix bugs**: Pick an issue labeled "good first issue"
4. **Add features**: Discuss first in GitHub Discussions
5. **Improve docs**: Submit PRs for typos or clarifications
6. **Write tests**: Increase test coverage
7. **Write examples**: Add real-world examples

**Get started**:
```bash
git clone https://github.com/subhayu99/sqlstream.git
cd sqlstream
pip install -e ".[dev]"
pytest
```

See [Contributing Guide](contributing.md) for details.

---

### Where should I start as a new contributor?

**Good first issues**:
- Look for "good first issue" label
- Documentation improvements
- Test coverage improvements
- Bug fixes with reproduction steps

**Learning path**:
1. Read [Architecture Guide](architecture/design.md)
2. Read [Volcano Model](architecture/volcano-model.md)
3. Browse code in `sqlstream/` directory
4. Run tests to understand behavior
5. Pick a small issue to work on

---

## Next Steps

- 📖 [Quick Start Guide](getting-started/quickstart.md)
- 🔧 [Troubleshooting](troubleshooting.md)
- 📚 [Full Documentation](index.md)
- 💬 [GitHub Discussions](https://github.com/subhayu99/sqlstream/discussions)
