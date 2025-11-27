# SQLStream

A lightweight SQL query engine for data exploration with lazy evaluation and intelligent optimizations.

## Features (Planned)

- 🚀 **Lazy Evaluation** - Stream through data without loading everything into memory
- 📊 **Multiple Formats** - CSV, Parquet, JSON support
- ⚡ **Smart Optimizations** - Predicate pushdown, column pruning, partition pruning
- 🌐 **Remote Files** - Query files over HTTP with intelligent range requests
- 🔍 **Statistics-Driven** - Use Parquet metadata to skip irrelevant data
- 💻 **Simple CLI** - Easy-to-use command-line interface
- 📦 **Tiny Package** - < 250KB, minimal dependencies

## Quick Start (Coming Soon)

```python
from sqlstream import query

# Query a local CSV file
results = query("data.csv").sql("""
    SELECT name, age
    WHERE age > 25
    LIMIT 10
""")

for row in results:
    print(row)
```

## Installation

**Using uv (recommended):**
```bash
uv pip install sqlstream
```

**Using pip:**
```bash
pip install sqlstream
```

## Development

This project uses modern Python tooling:

- **uv** - Fast Python package installer
- **ruff** - Fast Python linter and formatter
- **pytest** - Testing framework
- **hatch** - Modern Python packaging

### Setup

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
uv pip install -r requirements-dev.txt

# Or using regular pip
pip install -r requirements-dev.txt
```

### Running Tests

```bash
pytest
```

### Code Formatting

```bash
# Format code
ruff format .

# Lint code
ruff check .

# Type checking
mypy sqlstream/
```

## Architecture

SQLStream uses a Volcano-style pull-based query execution model with lazy evaluation:

```
Query → Parser → Optimizer → Executor → Results
                      ↓
        (Predicate Pushdown, Column Pruning,
         Partition Pruning, Statistics)
```

See [Technical Design Document](SQLStream%20-%20Technical%20Design%20Document.md) for details.

## Current Status

🚧 **Under Active Development** - Phase 0 (Bootstrap) Complete

- [x] Phase 0: Project Bootstrap
- [ ] Phase 1: Core Foundation (Parser + Basic Operators)
- [ ] Phase 2: Optimization Layer
- [ ] Phase 3: Parquet Support
- [ ] Phase 4: Advanced SQL (GROUP BY, ORDER BY)
- [ ] Phase 5: JOIN Support
- [ ] Phase 6: HTTP Streaming
- [ ] Phase 7: CLI Interface
- [ ] Phase 8: Type System
- [ ] Phase 9: Error Handling
- [ ] Phase 10: Testing & Documentation

## License

MIT
