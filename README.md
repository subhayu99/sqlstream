# SQLStream

**A lightweight, pure-Python SQL query engine for CSV and Parquet files with lazy evaluation and intelligent optimizations.**

[![Tests](https://github.com/subhayu99/sqlstream/workflows/tests/badge.svg)](https://github.com/subhayu99/sqlstream/actions)
[![Documentation](https://github.com/subhayu99/sqlstream/workflows/docs/badge.svg)](https://subhayu99.github.io/sqlstream)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

📖 **[Full Documentation](https://subhayu99.github.io/sqlstream)** | 🚀 **[Quick Start](https://subhayu99.github.io/sqlstream/getting-started/quickstart/)** | 💬 **[Discussions](https://github.com/subhayu99/sqlstream/discussions)**

---

## Quick Example

```bash
# Query a CSV file (explicit source)
$ sqlstream query data.csv "SELECT * FROM data WHERE age > 25"

# Query with inline file path (source inferred from SQL)
$ sqlstream query "SELECT * FROM 'data.csv' WHERE age > 25"

# Query S3 files
$ sqlstream query "SELECT * FROM 's3://my-bucket/data.parquet' WHERE date > '2024-01-01'"

# Join multiple files
$ sqlstream query "SELECT c.name, o.total FROM 'customers.csv' c JOIN 'orders.csv' o ON c.id = o.customer_id"

# Interactive shell with full TUI
$ sqlstream shell data.csv
```

## Features

- 🚀 **Pure Python** - No database installation required
- 📊 **Multiple Formats** - CSV, Parquet files, HTTP URLs, S3 buckets
- ⚡ **10-100x Faster** - Optional pandas backend for performance
- 🔗 **JOIN Support** - INNER, LEFT, RIGHT joins
- 📈 **Aggregations** - GROUP BY with COUNT, SUM, AVG, MIN, MAX
- 🔢 **Type System** - Automatic schema inference with type checking
- ☁️ **S3 Support** - Query files directly from Amazon S3
- 🎨 **Beautiful Output** - Rich tables, JSON, CSV formatting
- 🖥️ **Interactive Shell** - Full-featured TUI with multiple tabs, state persistence, file browser, query plan visualization, multi-format export
- 🔍 **Smart Optimizations** - Column pruning, predicate pushdown, lazy evaluation
- 📦 **Lightweight** - Minimal dependencies, works everywhere

## Installation

### Using [`uv`](https://docs.astral.sh/uv/#installation) (recommended)

**Basic (CSV only)**:
```bash
uv tool install sqlstream
```

**All features**:
```bash
uv tool install "sqlstream[all]"
```

**Multiple Sub Dependencies**:
```bash
uv tool install "sqlstream[interactive,pandas,s3,http,html,duckdb]"
```

### Using `pip`

**Basic (CSV only)**:
```bash
pip install sqlstream
```

**All features**:
```bash
pip install "sqlstream[all]"
```

See [Installation Guide](https://subhayu99.github.io/sqlstream/getting-started/installation/) for more options.

## Quick Start

### CLI Usage

```bash
# Simple query
$ sqlstream query data.csv "SELECT name, salary FROM data WHERE salary > 80000"

# With pandas backend for performance
$ sqlstream query data.csv "SELECT * FROM data" --backend pandas

# JSON output
$ sqlstream query data.csv "SELECT * FROM data" --format json

# Interactive shell with TUI
$ sqlstream shell data.csv
```

### Interactive Shell

```bash
$ sqlstream shell
```

Features:
- **Multiple Query Tabs** (`Ctrl+T`/`Ctrl+W`): Work with multiple queries simultaneously
- **State Persistence**: Automatically saves and restores your tabs and queries between sessions
- **Tabbed Sidebar** (`F2`): Toggle between Schema browser and File explorer
- **File Browser** (`Ctrl+O`): Browse and select files to query with tree structure
- **Query History** (`Ctrl+Up/Down`): Navigate through previous queries (multiline supported)
- **Word Deletion** (`Ctrl+Delete`/`Ctrl+Backspace`): Fast editing with word-aware deletion
- **Execution Plan** (`F4`): View detailed query execution steps
- **Smart Export** (`Ctrl+X`): Save results as CSV, JSON, or Parquet with custom filenames
- **Live Filtering** (`Ctrl+F`): Search across all columns
- **Pagination**: Handle large result sets (100 rows per page)
- **Column Sorting**: Click headers to sort ascending/descending
- **Syntax Highlighting**: Dracula theme for SQL queries
- **Exit & Save** (`Ctrl+Q` or `Ctrl+D`): Quit with automatic state saving

### Python API

```python
from sqlstream import query

# Execute query with explicit source
results = query("data.csv").sql("SELECT * FROM data WHERE age > 25")

# Execute query with inline source (extracted from SQL)
results = query().sql("SELECT * FROM 'data.csv' WHERE age > 25")

# Iterate over results
for row in results:
    print(row)

# Or convert to list
results_list = query().sql("SELECT * FROM 'data.csv'").to_list()
```

## Documentation

**Full documentation**: [https://subhayu99.github.io/sqlstream](https://subhayu99.github.io/sqlstream)

Key sections:

- [Quick Start Guide](https://subhayu99.github.io/sqlstream/getting-started/quickstart/) - Get started in 5 minutes
- [SQL Reference](https://subhayu99.github.io/sqlstream/features/sql-support/) - Supported SQL syntax
- [CLI Reference](https://subhayu99.github.io/sqlstream/cli/overview/) - Command-line interface
- [Python API](https://subhayu99.github.io/sqlstream/api/overview/) - Programmatic usage
- [Examples](https://subhayu99.github.io/sqlstream/examples/basic-queries/) - Real-world examples
- [Architecture](https://subhayu99.github.io/sqlstream/architecture/design/) - How it works

## Development Status

**Current Phase**: 9 (Enhanced Interactive Shell - Complete!)

- ✅ **Phase 0-2**: Core query engine with Volcano model
- ✅ **Phase 3**: Parquet support
- ✅ **Phase 4**: Aggregations & GROUP BY
- ✅ **Phase 5**: JOIN operations (INNER, LEFT, RIGHT)
- ✅ **Phase 5.5**: Pandas backend (10-100x speedup)
- ✅ **Phase 6**: HTTP data sources
- ✅ **Phase 7**: CLI with beautiful output
- ✅ **Phase 7.5**: Interactive mode with Textual
- ✅ **Phase 7.6**: Inline file path support
- ✅ **Phase 8**: Type system & schema inference
- ✅ **Phase 9**: Enhanced interactive shell (multiple tabs, state persistence, file browser, query plan)
- 🚧 **Phase 10**: Error handling & user feedback
- 🚧 **Phase 11**: Testing & documentation

**Test Coverage**: 377 tests, 53% coverage

## Performance

SQLStream offers two execution backends:

| Backend | Speed | Use Case |
|---------|-------|----------|
| Python | Baseline | Learning, small files (<100K rows) |
| Pandas | **10-100x faster** | Production, large files (>100K rows) |
| DuckDB | **100x+ faster** | Complex SQL, analytics, huge files |

Benchmark (1M rows):

- Python backend: 52s
- Pandas backend: 0.8s ⚡ **65x faster**

## Architecture

SQLStream uses the **Volcano iterator model** for query execution:

```
SQL Query → Parser → AST → Planner → Optimizer → Executor → Results
                                          ↓
                            (Column Pruning, Predicate Pushdown,
                             Lazy Evaluation)
```

Key concepts:

- **Lazy Evaluation**: Rows are processed on-demand
- **Column Pruning**: Only read columns that are used
- **Predicate Pushdown**: Apply filters early to reduce data scanned
- **Two Backends**: Pure Python (learning) and Pandas (performance)

See [Architecture Guide](https://subhayu99.github.io/sqlstream/architecture/design/) for details.

## Contributing

Contributions are welcome! See [Contributing Guide](https://subhayu99.github.io/sqlstream/contributing/) for details.

**Development setup**:

```bash
# Clone repository
git clone https://github.com/subhayu99/sqlstream.git
cd sqlstream

# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
ruff format .
ruff check .
```

## License

MIT License - see [LICENSE](LICENSE) for details.

---

**Built with ❤️ by the SQLStream Team**

📖 [Documentation](https://subhayu99.github.io/sqlstream) •
🐛 [Issues](https://github.com/subhayu99/sqlstream/issues) •
💬 [Discussions](https://github.com/subhayu99/sqlstream/discussions)
