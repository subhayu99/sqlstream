# SQLStream

**A lightweight, pure-Python SQL query engine for CSV, Parquet, JSON, JSONL, HTML, and Markdown files with lazy evaluation and intelligent optimizations.**

[![Tests](https://img.shields.io/github/actions/workflow/status/subhayu99/sqlstream/tests.yml?branch=main&label=Tests)](https://github.com/subhayu99/sqlstream/actions)
[![Documentation](https://img.shields.io/github/actions/workflow/status/subhayu99/sqlstream/docs.yml?branch=main&label=Documentation)](https://subhayu99.github.io/sqlstream)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

📖 **[Full Documentation](https://subhayu99.github.io/sqlstream)** | 🚀 **[Quick Start](https://subhayu99.github.io/sqlstream/getting-started/quickstart/)** | 💬 **[Discussions](https://github.com/subhayu99/sqlstream/discussions)**

---

## Quick Example

```bash
# Query a CSV file (explicit source)
$ sqlstream query data.csv "SELECT * FROM data WHERE age > 25"

# Query with inline file path (source inferred from SQL)
$ sqlstream query "SELECT * FROM 'data.csv' WHERE age > 25"

# Query JSON with nested paths
$ sqlstream query "users.json#json:data.users" "SELECT name, email FROM users"

# Query JSONL (JSON Lines) files
$ sqlstream query logs.jsonl "SELECT timestamp, level, message FROM logs WHERE level = 'ERROR'"

# Query HTML tables
$ sqlstream query "report.html#html:0" "SELECT * FROM report WHERE revenue > 1000000"

# Query Markdown tables
$ sqlstream query "README.md#markdown:1" "SELECT column1, column2 FROM readme"

# Query S3 files with partitions
$ sqlstream query "SELECT * FROM 's3://my-bucket/data/year=2024/' WHERE date > '2024-01-01'"

# Join multiple formats (CSV + Parquet + JSON)
$ sqlstream query "SELECT c.name, o.total, u.email
  FROM 'customers.csv' c
  JOIN 'orders.parquet' o ON c.id = o.customer_id
  JOIN 'users.json#json:users' u ON c.user_id = u.id"

# Interactive shell with full TUI
$ sqlstream shell
```

## Features

- 🚀 **Pure Python** - No database installation required
- 📊 **Multiple Formats** - CSV, Parquet, JSON, JSONL, HTML, Markdown with nested path syntax, HTTP URLs, S3 buckets
- ⚡ **100x+ Faster** - DuckDB backend for complex SQL, Pandas backend for simple queries
- 🔗 **JOIN Support** - INNER, LEFT, RIGHT, FULL OUTER joins across different file formats
- 📈 **Aggregations** - GROUP BY with COUNT, SUM, AVG, MIN, MAX, DISTINCT
- 🔢 **Rich Type System** - 10 data types (INTEGER, FLOAT, DECIMAL, STRING, JSON, BOOLEAN, DATE, TIME, DATETIME, NULL) with automatic inference
- ☁️ **S3 Support** - Query files directly from Amazon S3 with partition support
- 🎨 **Beautiful Output** - Rich tables, JSON, CSV, Parquet, Markdown formatting
- 🖥️ **Advanced Interactive Shell** - Multiple tabs, sidebars, layout cycling, backend toggle, state persistence, file browser
- 🔍 **Smart Optimizations** - Column pruning, predicate pushdown, limit pushdown, lazy evaluation
- 🌐 **REST API Ready** - Query HTTP endpoints and APIs (coming soon)
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

**Optional Dependencies**:
- `pandas` - Pandas backend for 10-100x speedup
- `duckdb` - DuckDB backend for 100x+ speedup and advanced SQL
- `parquet` - Parquet file support
- `s3` - Amazon S3 file access
- `http` - HTTP/HTTPS data sources
- `html` - HTML table extraction (requires pandas, html5lib, beautifulsoup4)
- `interactive` - Interactive shell with rich TUI
- `all` - All features combined

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

**Enhanced TUI Features:**

**Query Management:**
- **Multiple Tabs** (`Ctrl+T`/`Ctrl+W`) - Work on multiple queries simultaneously
- **State Persistence** - Auto-save tabs, queries, and layout between sessions
- **Query History** (`Ctrl+Up/Down`) - Navigate previous queries with multiline support
- **Auto-completion** - Schema-aware suggestions for tables and columns
- **Syntax Highlighting** - SQL syntax with Dracula theme

**Sidebars & Layout:**
- **Dynamic Sidebars** (`F2`/`F3`) - Schema browser, File explorer, Filter, Export, Config
- **Layout Cycling** (`Ctrl+L`) - Resize query editor: 50%, 60%, 70%, 80%, 100%
- **File Browser** (`Ctrl+O`) - Tree view with directory navigation
- **Schema Browser** - Real-time schema and type information

**Execution & Performance:**
- **Backend Toggle** (`F5` or `Ctrl+B`) - Cycle: Auto → DuckDB → Pandas → Python
- **Execution Plan** (`F4`) - View query optimization steps
- **Async Execution** - Responsive UI during long queries
- **Cancel Queries** (`Ctrl+C`) - Stop running queries

**Results Management:**
- **Advanced Filtering** (`Ctrl+F`) - Column-specific or global search
- **Smart Export** (`Ctrl+X`) - CSV, JSON, or Parquet with format selection
- **Pagination** - 100 rows per page, configurable
- **Column Sorting** - Click headers to sort
- **Live Stats** - Row counts and filter status

**Keyboard Shortcuts:**
- `Ctrl+Delete`/`Ctrl+Backspace` - Word-aware deletion
- `Ctrl+Q` or `Ctrl+D` - Exit with auto-save

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
- [Troubleshooting](https://subhayu99.github.io/sqlstream/troubleshooting/) - Common issues and solutions
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
- ✅ **Phase 10**: HTML & Markdown readers with table extraction
- ✅ **Phase 11**: Enhanced type system (Decimal, DateTime, Date, Time, JSON)
- 🚧 **Phase 12**: Comprehensive testing & documentation (15% coverage → 80% target)

**Test Coverage**: 560 tests, 15% coverage (actively improving)

## Performance

SQLStream offers **three execution backends**:

| Backend | Speed | Use Case |
|---------|-------|----------|
| Python | Baseline | Learning, small files (<100K rows) |
| Pandas | **10-100x faster** | Basic queries, large files (>100K rows) |
| DuckDB | **100x+ faster** | Complex SQL, analytics, huge files (10M+ rows) |

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
- **Three Backends**: Pure Python (learning), Pandas (performance), and DuckDB (full SQL)

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
