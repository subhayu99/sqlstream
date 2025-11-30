# Scripts Documentation

This directory contains utility scripts for maintaining the SQLstream project.

## generate_api_docs.py

Automatically generates API reference documentation files from source code.

### Usage

```bash
# Generate docs and update mkdocs.yml navigation (local development)
python3 scripts/generate_api_docs.py

# Generate docs without updating navigation (CI/CD)
python3 scripts/generate_api_docs.py --skip-nav-update
```

### What it does

1. Scans the `sqlstream/` source directory to discover all modules, classes, and functions
2. Groups them into categories (Query, Readers, Types, Operators, Optimizers, etc.)
3. Generates markdown files in `docs/api/reference/` with mkdocstrings syntax
4. Each file contains auto-generated API documentation extracted from docstrings
5. Optionally updates `mkdocs.yml` navigation (skipped in CI/CD)

### Workflow integration

**CI/CD (GitHub Actions)**:
- Runs with `--skip-nav-update` flag
- Generates files during build (not committed to repo)
- `docs/api/reference/` is in `.gitignore`

**Local Development**:
- Run without flags to update navigation
- Helps keep mkdocs.yml in sync with discovered modules

### No Hard-Coding!

The script automatically discovers all modules by scanning the file system. When you add a new module:
1. Write your code with docstrings
2. Run `python3 scripts/generate_api_docs.py` locally (optional)
3. GitHub Actions will auto-generate docs on push

---

## publish.py

Publishes a new version of SQLstream to PyPI.

### Usage

```bash
python3 scripts/publish.py
```

### What it does

1. Prompts for version number
2. Updates `__version__` in source code
3. Cleans previous builds
4. Builds wheel and source distribution
5. Uploads to PyPI using twine

### Prerequisites

- `build` package installed
- `twine` installed and configured with PyPI credentials
- Write access to the sqlstream PyPI project

---

## Development Workflow

### Adding a New Module

1. Create your module in the appropriate directory
2. Add docstrings (Google style recommended)
3. Commit your code
4. GitHub Actions will automatically:
   - Discover your new module
   - Generate API docs
   - Deploy updated documentation

### Updating Documentation

**Auto-generated (API Reference)**:
- Never edit files in `docs/api/reference/` - they're auto-generated
- Update docstrings in source code instead

**Manual (Guides and Tutorials)**:
- Edit markdown files in `docs/` directories
- These are hand-crafted guides that won't be overwritten
