# SQLStream

**A lightweight, pure-Python SQL query engine for CSV and Parquet files with lazy evaluation and intelligent optimizations.**

[![Tests](https://github.com/subhayu99/sqlstream/workflows/tests/badge.svg)](https://github.com/subhayu99/sqlstream/actions)
[![Documentation](https://github.com/subhayu99/sqlstream/workflows/docs/badge.svg)](https://subhayu99.github.io/sqlstream)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

📖 **[Full Documentation](https://subhayu99.github.io/sqlstream)** | 🚀 **[Quick Start](https://subhayu99.github.io/sqlstream/getting-started/quickstart/)** | 💬 **[Discussions](https://github.com/subhayu99/sqlstream/discussions)**

---

## Quick Example

```bash
# Query a CSV file
$ sqlstream query "SELECT * FROM 'data.csv' WHERE age > 25"

# Join multiple files
$ sqlstream query "SELECT c.name, o.total FROM 'customers.csv' c JOIN 'orders.csv' o ON c.id = o.customer_id"

# Interactive mode for wide tables
$ sqlstream query data.csv "SELECT * FROM data" --interactive
```

## Features

- 🚀 **Pure Python** - No database installation required
- 📊 **Multiple Formats** - CSV, Parquet files, HTTP URLs
- ⚡ **10-100x Faster** - Optional pandas backend for performance
- 🔗 **JOIN Support** - INNER, LEFT, RIGHT joins
- 📈 **Aggregations** - GROUP BY with COUNT, SUM, AVG, MIN, MAX
- 🎨 **Beautiful Output** - Rich tables, JSON, CSV formatting
- 🖥️ **Interactive Mode** - Scrollable table viewer with Textual
- 🔍 **Smart Optimizations** - Column pruning, predicate pushdown, lazy evaluation
- 📦 **Lightweight** - Minimal dependencies, works everywhere

## Installation

**Basic (CSV only)**:
```bash
pip install sqlstream
```

**All features** (recommended):
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

# Interactive mode
$ sqlstream query data.csv "SELECT * FROM data" --interactive
```

### Python API

```python
from sqlstream import query

# Execute query (lazy evaluation)
results = query("data.csv").sql("SELECT * FROM data WHERE age > 25")

# Iterate over results
for row in results:
    print(row)

# Or convert to list
results_list = query("data.csv").sql("SELECT * FROM data").to_list()
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

**Current Phase**: 7.6 (Inline File Path Support)

- ✅ **Phase 0-2**: Core query engine with Volcano model
- ✅ **Phase 3**: Parquet support
- ✅ **Phase 4**: Aggregations & GROUP BY
- ✅ **Phase 5**: JOIN operations (INNER, LEFT, RIGHT)
- ✅ **Phase 5.5**: Pandas backend (10-100x speedup)
- ✅ **Phase 6**: HTTP data sources
- ✅ **Phase 7**: CLI with beautiful output
- ✅ **Phase 7.5**: Interactive mode with Textual
- ✅ **Phase 7.6**: Inline file path support
- 🚧 **Phase 8**: Type system & schema inference
- 🚧 **Phase 9**: Error handling & user feedback
- 🚧 **Phase 10**: Testing & documentation

**Test Coverage**: 307 tests, 38% coverage

## Performance

SQLStream offers two execution backends:

| Backend | Speed | Use Case |
|---------|-------|----------|
| Python | Baseline | Learning, small files (<100K rows) |
| Pandas | **10-100x faster** | Production, large files (>100K rows) |

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
