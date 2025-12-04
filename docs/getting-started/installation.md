# Installation

SQLStream offers multiple installation options depending on your needs.

---

## Requirements

- **Python**: 3.10 or higher
- **OS**: Linux, macOS, Windows

---

## Installation Options

### Basic Installation (CSV only)

For querying CSV files only:

```bash
pip install sqlstream
```

This gives you:

- ✅ Core SQL engine
- ✅ CSV file support
- ✅ JSON and JSONL file support
- ✅ Markdown table support
- ✅ Basic CLI commands
- ❌ No Parquet support
- ❌ No HTML table extraction
- ❌ No performance optimizations (Pandas/DuckDB)

---

### With Parquet Support

To query both CSV and Parquet files:

```bash
pip install "sqlstream[parquet]"
```

Additional features:

- ✅ Everything from basic install
- ✅ Parquet file support via PyArrow
- ✅ Better compression and performance

---

### With Pandas Backend (Recommended)

For **10-100x performance boost** with large files:

```bash
pip install "sqlstream[pandas]"
```

Additional features:

- ✅ Everything from basic install
- ✅ Parquet support
- ✅ **Pandas-powered execution** (10-100x faster)
- ✅ Optimized for large datasets (>100K rows)

---

### With DuckDB Backend (Production-Ready)

For **100x+ performance boost** and full SQL support:

```bash
pip install "sqlstream[duckdb]"
```

Additional features:

- ✅ Everything from basic install
- ✅ **DuckDB-powered execution** (100x+ faster)
- ✅ Full SQL support (CTEs, window functions, subqueries)
- ✅ Optimized for huge datasets (10M+ rows)
- ✅ Production-ready analytics engine

---

### With HTML Table Extraction

To query tables from HTML documents:

```bash
pip install "sqlstream[html]"
```

Additional features:

- ✅ Extract tables from HTML files and URLs
- ✅ Multi-table support with index selection
- ✅ Automatic schema inference from HTML tables
- ✅ Works with Pandas backend for fast processing

---

### With S3 Support

To query files directly from Amazon S3:

```bash
pip install "sqlstream[s3]"
```

Additional features:

- ✅ Direct S3 bucket access
- ✅ Partition-aware queries
- ✅ AWS credentials integration
- ✅ Works with CSV and Parquet on S3

---

### With HTTP Support

To query CSV/Parquet/JSON files from URLs:

```bash
pip install "sqlstream[http]"
```

Additional features:

- ✅ Query files from HTTP/HTTPS URLs
- ✅ Automatic format detection
- ✅ Streaming support for large remote files

---

### With CLI Features

For beautiful terminal output and interactive mode:

```bash
pip install "sqlstream[cli]"
```

Additional features:

- ✅ Rich table formatting
- ✅ Interactive scrollable table viewer
- ✅ Syntax highlighting
- ✅ Multiple output formats (JSON, CSV, table)

---

### All Features

To install everything:

```bash
pip install "sqlstream[all]"
```

This includes:

- ✅ CSV, Parquet, JSON, JSONL support
- ✅ HTML and Markdown table extraction
- ✅ Pandas and DuckDB backends for maximum performance
- ✅ HTTP and S3 data sources
- ✅ Full CLI with interactive mode
- ✅ All output formats (table, JSON, CSV, Parquet, Markdown)

---

## Development Installation

For contributing or development:

```bash
# Clone the repository
git clone https://github.com/subhayu99/sqlstream.git
cd sqlstream

# Install in development mode with all dev dependencies
pip install -e ".[dev]"
```

This includes:

- Testing: pytest, pytest-cov
- Linting: ruff
- Type checking: mypy
- Documentation: mkdocs, mkdocs-material

---

## Verifying Installation

After installation, verify SQLStream is working:

```bash
# Check version
sqlstream --version

# Run a quick query (requires a CSV file)
echo "name,age\nAlice,30\nBob,25" > test.csv
sqlstream query test.csv "SELECT * FROM test"
```

Expected output:

```
┌───────┬─────┐
│ name  │ age │
├───────┼─────┤
│ Alice │  30 │
│ Bob   │  25 │
└───────┴─────┘

2 rows
```

---

## Upgrading

To upgrade to the latest version:

```bash
pip install --upgrade sqlstream
```

To upgrade with all features:

```bash
pip install --upgrade "sqlstream[all]"
```

---

## Uninstalling

To remove SQLStream:

```bash
pip uninstall sqlstream
```

---

## Troubleshooting

### Import Errors

If you see `ModuleNotFoundError`:

```bash
# Reinstall with verbose output
pip install --verbose "sqlstream[all]"
```

### Pandas Not Found

If you get "pandas backend requested but pandas is not installed":

```bash
pip install "sqlstream[pandas]"
```

### CLI Not Working

If `sqlstream` command is not found:

```bash
# Check if it's in your PATH
which sqlstream

# Try running as module
python -m sqlstream.cli.main --help
```

---

## Next Steps

- [Quick Start Guide](quickstart.md) - Get started in 5 minutes
- [Core Concepts](core-concepts.md) - Understand the basics
- [SQL Support](../features/sql-support.md) - Learn supported SQL syntax
