# SQLStream

**A lightweight, pure-Python SQL query engine for CSV and Parquet files with lazy evaluation and intelligent optimizations.**

---

## Quick Example

```bash
# Query a CSV file
$ sqlstream query "SELECT * FROM 'data.csv' WHERE age > 25"

# Join multiple files
$ sqlstream query "SELECT c.name, o.total FROM 'customers.csv' c JOIN 'orders.csv' o ON c.id = o.customer_id"

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

    Support for CSV, Parquet files, HTTP URLs, and S3 buckets.

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

    Full-featured TUI with modal dialogs, file browser, query plan visualization.

-   :material-file-search:{ .lg .middle } __Inline File Paths__

    ---

    Specify files directly in SQL queries (Phase 7.6).

-   :material-tune:{ .lg .middle } __Smart Optimizations__

    ---

    Column pruning, predicate pushdown, lazy evaluation.

</div>

---

## Installation

=== "Basic (CSV only)"

    ```bash
    pip install sqlstream
    ```

=== "With Parquet"

    ```bash
    pip install "sqlstream[parquet]"
    ```

=== "With Pandas (10-100x faster)"

    ```bash
    pip install "sqlstream[pandas]"
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

SQLStream offers two execution backends:

| Backend | Speed | Use Case |
|---------|-------|----------|
| **Python** | Baseline | Learning, small files (<100K rows) |
| **Pandas** | **10-100x faster** | Production, large files (>100K rows) |

!!! tip "Performance Tips"
    - Use `--backend pandas` for files >100K rows
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

SQLStream is in **active development**. Current phase: **8**

- ✅ Phase 0-2: Core query engine with Volcano model
- ✅ Phase 3: Parquet support
- ✅ Phase 4: Aggregations & GROUP BY
- ✅ Phase 5: JOIN operations (INNER, LEFT, RIGHT)
- ✅ Phase 5.5: Pandas backend (10-100x speedup)
- ✅ Phase 6: HTTP data sources
- ✅ Phase 7: CLI with beautiful output
- ✅ Phase 7.5: Interactive shell with Textual
- ✅ Phase 7.6: Inline file path support
- ✅ Phase 7.7: S3 Support for CSV and Parquet
- ✅ Phase 8: Type system & schema inference
- 🚧 Phase 9: Enhanced interactive shell (modal dialogs, file browser, query plan)
- 🚧 Phase 10: Error handling & user feedback

---

## License

SQLStream is licensed under the [MIT License](https://github.com/subhayu99/sqlstream/blob/main/LICENSE).

---

## Contributing

Contributions are welcome! See the [Contributing Guide](contributing.md) for details.
