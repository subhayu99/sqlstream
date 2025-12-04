# <div align="center"><img src="assets/logo_cropped_transparent.png" alt="SQLStream" width="280"/></div>

**A lightweight, pure-Python SQL query engine for CSV, Parquet, JSON, JSONL, HTML, Markdown, and XML files with lazy evaluation and intelligent optimizations.**

---

## Quick Example

```bash
# Query a CSV file
$ sqlstream query "SELECT * FROM 'data.csv' WHERE age > 25"

# Query JSON with nested paths
$ sqlstream query "users.json#json:data.users" "SELECT name, email FROM users"

# Query HTML tables from files or URLs
$ sqlstream query "report.html#html:0" "SELECT * FROM report WHERE revenue > 1000000"

# Query Markdown tables
$ sqlstream query "README.md#markdown:1" "SELECT column1, column2 FROM readme"

# Query XML files
$ sqlstream query "data.xml#xml:record" "SELECT name, age FROM data WHERE age > 25"

# Join multiple files (any format combination)
$ sqlstream query "SELECT c.name, o.total FROM 'customers.csv' c JOIN 'orders.parquet' o ON c.id = o.customer_id"

# Interactive shell with full TUI
$ sqlstream shell

# Query S3 files
$ sqlstream query "SELECT * FROM 's3://my-bucket/data.parquet' WHERE date > '2024-01-01'"
```

---

## Key Features

<div class="grid cards" markdown>

-   :material-language-python:{ .lg .middle } __Pure Python__

    ---

    No database installation required. Works anywhere Python runs.

-   :material-file-chart:{ .lg .middle } __Multiple Formats__

    ---

    Support for CSV, Parquet, JSON, JSONL, HTML, Markdown, XML files, HTTP URLs, and S3 buckets.

-   :material-lightning-bolt:{ .lg .middle } __10-100x Faster__

    ---

    Optional pandas backend for massive performance boost.

-   :material-link-variant:{ .lg .middle } __JOIN Support__

    ---

    INNER, LEFT, RIGHT joins across multiple files.

-   :material-chart-bar:{ .lg .middle } __Aggregations__

    ---

    GROUP BY with COUNT, SUM, AVG, MIN, MAX functions.

-   :material-palette:{ .lg .middle } __Beautiful Output__

    ---

    Rich tables, JSON, CSV with syntax highlighting.

-   :material-television:{ .lg .middle } __Interactive Shell__

    ---

    Full-featured TUI with multiple tabs, state persistence, file browser, and query plan visualization.

-   :material-file-search:{ .lg .middle } __Table Extraction__

    ---

    Extract and query tables from HTML pages and Markdown documents with multi-table support.

-   :material-tune:{ .lg .middle } __Smart Optimizations__

    ---

    Column pruning, predicate pushdown, lazy evaluation.

</div>

---

## Installation

### Using [`uv`](https://docs.astral.sh/uv/#installation) (recommended)

=== "Basic (CSV only)"

    ```bash
    uv tool install sqlstream
    ```

=== "With Parquet and Pandas support"

    ```bash
    uv tool install "sqlstream[parquet,pandas]"
    ```

=== "All Features"

    ```bash
    uv tool install "sqlstream[all]"
    ```

=== "Multiple Sub Dependencies"

    ```bash
    uv tool install "sqlstream[interactive,pandas,s3,http,html,duckdb]"
    ```

### Using `pip`

=== "Basic (CSV only)"

    ```bash
    pip install sqlstream
    ```

=== "All Features"

    ```bash
    pip install "sqlstream[all]"
    ```

---

## Quick Start

### CLI Usage

```bash
# Simple query
$ sqlstream query data.csv "SELECT name, age FROM data WHERE age > 25"

# With output format
$ sqlstream query data.csv "SELECT * FROM data" --format json

# Show execution time
$ sqlstream query data.csv "SELECT * FROM data" --time

# Use pandas backend for performance
$ sqlstream query data.parquet "SELECT * FROM data" --backend pandas
```

### Python API

```python
from sqlstream import query

# Execute query
results = query("data.csv").sql("SELECT * FROM data WHERE age > 25")

# Iterate over results (lazy evaluation)
for row in results:
    print(row)

# Or convert to list
results_list = query("data.csv").sql("SELECT * FROM data").to_list()
```

---

## Why SQLStream?

!!! success "Perfect For"
    - **Data Exploration**: Quick analysis without database setup
    - **ETL Pipelines**: Process CSV/Parquet files with SQL
    - **Data Science**: Filter and join datasets before pandas
    - **DevOps**: Query logs and data files in CI/CD
    - **Learning**: Understand query execution internals

!!! warning "Not For"
    - **Large Databases**: Use PostgreSQL, MySQL instead
    - **Real-time Analytics**: Use ClickHouse, DuckDB
    - **Production OLTP**: SQLStream is read-only

---

## Performance

SQLStream offers three execution backends:

| Backend | Speed | Use Case |
|---------|-------|----------|
| **Python** | Baseline | Learning, small files (<100K rows) |
| **Pandas** | **10-100x faster** | Basic queries, large files (>100K rows) |
| **DuckDB** | **100x+ faster** | Complex SQL, analytics, huge files |

!!! tip "Performance Tips"
    - Use `--backend duckdb` for complex SQL (CTEs, window functions)
    - Use `--backend pandas` for simple queries on large files
    - Use column pruning: `SELECT name, age` instead of `SELECT *`
    - Add WHERE filters to reduce data scanned
    - Use Parquet format for better compression

---

## What's Next?

<div class="grid cards" markdown>

-   :material-clock-fast:{ .lg .middle } [__Quick Start Guide__](getting-started/quickstart.md)

    ---

    Get up and running in 5 minutes with hands-on examples.

-   :material-code-braces:{ .lg .middle } [__SQL Reference__](features/sql-support.md)

    ---

    Learn about supported SQL syntax and features.

-   :material-console:{ .lg .middle } [__CLI Reference__](cli/overview.md)

    ---

    Complete guide to the command-line interface.

-   :material-api:{ .lg .middle } [__Python API__](api/overview.md)

    ---

    Deep dive into the programmatic API.

</div>

---

## Project Status

SQLStream is in **active development**. Current phase: **12**

- ✅ Phase 0-2: Core query engine with Volcano model
- ✅ Phase 3: Parquet support
- ✅ Phase 4: Aggregations & GROUP BY
- ✅ Phase 5: JOIN operations (INNER, LEFT, RIGHT, FULL OUTER)
- ✅ Phase 5.5: Pandas backend (10-100x speedup)
- ✅ Phase 6: HTTP data sources
- ✅ Phase 7: CLI with beautiful output
- ✅ Phase 7.5: Interactive shell with Textual
- ✅ Phase 7.6: Inline file path support
- ✅ Phase 7.7: S3 Support for CSV and Parquet
- ✅ Phase 8: Type system & schema inference
- ✅ Phase 9: Enhanced interactive shell (multiple tabs, state persistence, file browser, query plan)
- ✅ Phase 10: HTML & Markdown readers with table extraction
- ✅ Phase 11: Enhanced type system (Decimal, DateTime, Date, Time, JSON) & DuckDB backend integration
- 🚧 Phase 12: Comprehensive testing & documentation (560 tests, 15% coverage → 80% target)

---

## License

SQLStream is licensed under the [MIT License](https://github.com/subhayu99/sqlstream/blob/main/LICENSE).

---

## Contributing

Contributions are welcome! See the [Contributing Guide](contributing.md) for details.
