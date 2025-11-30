# Interactive SQL Shell

SQLStream includes a powerful interactive shell built with [Textual](https://textual.textualize.io/), providing a modern terminal UI for exploring and querying data.

---

## Features

- 🎨 **Syntax Highlighting** - Monokai theme for SQL queries
- 📊 **Scrollable Results** - Zebra-striped table with smooth scrolling
- 📜 **Query History** - Navigate previous queries with keyboard shortcuts
- 🗂️ **Schema Browser** - View file schemas with column types
- 📄 **Pagination** - Handle large result sets (100 rows per page)
- 🔀 **Column Sorting** - Click headers to sort ascending/descending
- 💾 **Multi-Format Export** - Save results as CSV, JSON, and Parquet
- 🔍 **Filtering** - Search across all columns
- ☁️ **S3 Support** - Query files directly from S3 buckets
- ⚡ **Fast** - Execution time display and row counts

---

## Installation

The interactive shell requires the `textual` library:

```bash
# Install with CLI support
pip install "sqlstream[cli]"

# Or install all features
pip install "sqlstream[all]"
```

---

## Getting Started

### Launch the Shell

```bash
# Empty shell
sqlstream shell

# With initial file
sqlstream shell employees.csv

# Custom history location
sqlstream shell --history-file ~/.my_sqlstream_history
```

### Basic Usage

1. **Write a query** in the editor (supports multi-line)
2. **Execute** with `Ctrl+Enter` or `Ctrl+E`
3. **View results** in the table below
4. **Navigate** large result sets with pagination
5. **Export** results with `Ctrl+X`

---

## Keybindings

| Key | Action | Description |
|-----|--------|-------------|
| `Ctrl+Enter` | Execute Query | Run the query in editor |
| `Ctrl+E` | Execute Query | Alternative execution key |
| `Ctrl+L` | Clear Editor | Clear query text |
| `Ctrl+D` | Exit | Close the shell |
| `F1` | Help | Show help message |
| `F2` | Toggle Schema | Show/hide schema browser |
| `F4` | Explain Mode | Show query plan |
| `Ctrl+O` | Open File | Browse files to add to query |
| `Ctrl+X` | Export | Export with custom filename |
| `Ctrl+F` | Filter | Filter current results |
| `[` | Previous Page | Navigate to previous page |
| `]` | Next Page | Navigate to next page |
| `Ctrl+Up` | Prev Query | Load previous from history |
| `Ctrl+Down` | Next Query | Load next from history |
| **Click Header** | Sort Column | Sort by column (click again to reverse) |

---

## Query Examples

### Local Files

```sql
-- Simple query
SELECT * FROM 'employees.csv' WHERE age > 30

-- Aggregations
SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
FROM 'employees.csv'
GROUP BY department
ORDER BY avg_salary DESC

-- JOINs
SELECT e.name, e.salary, d.department_name
FROM 'employees.csv' e
JOIN 'departments.csv' d ON e.dept_id = d.id
```

### S3 Files

```sql
-- Query S3 CSV
SELECT * FROM 's3://my-bucket/data.csv' WHERE date > '2024-01-01'

-- Query S3 Parquet with aggregation
SELECT product_id, SUM(revenue) as total
FROM 's3://my-bucket/sales.parquet'
WHERE date > '2024-01-01'
GROUP BY product_id
ORDER BY total DESC
LIMIT 10
```

### HTTP Files

```sql
SELECT * FROM 'https://example.com/data.csv'
WHERE category = 'electronics'
```

---

## Advanced Features

### 1. Query History

The shell maintains a persistent history of your queries (up to 100 queries).

**Location**: `~/.sqlstream_history` (or custom with `--history-file`)

**Navigation**:
- `Ctrl+Up` - Load previous query
- `Ctrl+Down` - Load next query

**Behavior**:
- History loads automatically on startup
- Each executed query is saved
- Navigate through history without re-executing

### 2. Schema Browser

Press `F2` to toggle the schema browser panel.

**Shows**:
- All loaded files
- Column names (green)
- Data types (dim text)
- Errors (red)

**Example**:
```
Data Sources
├─ employees.csv
│  ├─ name: string
│  ├─ age: int
│  ├─ city: string
│  └─ salary: float
└─ sales.parquet
   ├─ product_id: int
   ├─ revenue: float
   └─ date: date
```

**Features**:
- Asynchronous schema loading (non-blocking)
- Updates automatically when querying new files
- Helps discover available columns before writing queries

### 3. Pagination

When a query returns more than 100 rows, results are automatically paginated.

**Status Display**:
```
Showing 101-200 of 450 rows | Page 2/5
```

**Navigation**:
- `Ctrl+N` or `]` - Next page
- `Ctrl+P` or `[` - Previous page
- Sorting and filtering reset to page 1

**Performance**:
- Only 100 rows rendered at a time
- Instant navigation between pages
- Handles millions of rows efficiently

### 4. Column Sorting

Click any column header to sort results.

**Behavior**:
1. **First click**: Sort ascending (↑)
2. **Second click**: Sort descending (↓)
3. **Click another column**: Sort by that column

**Status Display**:
```
Sorted by salary ↓
```

**Notes**:
- Sorting works across all pages
- Resets current page to 1
- Works with filtered results

### 5. Multi-Format Export

Press `Ctrl+X` to export current results to multiple formats simultaneously.

**Exported Formats**:
- **CSV**: `results_YYYYMMDD_HHMMSS.csv`
- **JSON**: `results_YYYYMMDD_HHMMSS.json` (pretty-printed)
- **Parquet**: `results_YYYYMMDD_HHMMSS.parquet` (if `pyarrow` installed)

**Example**:
```
Exported to: CSV (results_20241130_143022.csv),
             JSON (results_20241130_143022.json),
             Parquet (results_20241130_143022.parquet)
```

**Notes**:
- Exports current page or filtered results
- Timestamped filenames prevent overwrites
- Parquet export requires `pip install pyarrow`

### 6. Filtering

Press `Ctrl+F` to filter current results.

**Features**:
- Case-insensitive search
- Searches across all columns
- Updates row count in status bar

**Example**:
```
Filtered to 45 rows (from 450 total)
```

---

## Performance

| Feature | Performance |
|---------|-------------|
| Pagination | Shows first 100 rows instantly |
| Sorting | In-memory sort of all results |
| Filtering | Scans all rows once, then cached |
| S3 Loading | Streams data, doesn't load all into memory |
| Schema Loading | Async worker, doesn't block UI |

---

## Examples

### Example 1: Explore Large Dataset

```sql
-- Load first 1000 rows
SELECT * FROM 'big_file.csv' LIMIT 1000

-- Results: 1000 rows → 10 pages
-- Use ] to navigate pages
-- Click 'revenue' header to sort
-- Use Ctrl+X to export
```

### Example 2: S3 Analytics

```sql
-- Query S3 Parquet
SELECT
    category,
    COUNT(*) as count,
    AVG(price) as avg_price
FROM 's3://my-bucket/products.parquet'
GROUP BY category
ORDER BY count DESC

-- Click 'count' to sort
-- Export to CSV for sharing
```

### Example 3: Schema Exploration

```sql
-- Press F2 to see schema
-- Write query with column names visible
SELECT name, age, city
FROM 'employees.csv'
WHERE age > 25
ORDER BY name

-- Sort by different columns using headers
-- Export to JSON for API use
```

---

## Tips & Tricks

1. **Large Datasets**: Use `LIMIT` to preview data quickly
2. **S3 Performance**: Use partitioned Parquet files for best performance
3. **History**: Use `Ctrl+Up` to quickly re-run previous queries
4. **Sorting**: Click column headers to explore data patterns
5. **Export**: Export to Parquet for best compression
6. **Schema**: Press F2 before writing queries to see available columns

---

## Troubleshooting

### Issue: Footer Visibility

**Problem**: Footer may be clipped on small terminal windows

**Solution**: Increase terminal height or scroll down

### Issue: Keybinding Conflicts

**Problem**: Some keybindings don't work in VSCode terminal

**Solution**: Use a native terminal (gnome-terminal, iTerm2, Windows Terminal)

### Issue: Textual Not Installed

**Problem**: `ImportError: No module named 'textual'`

**Solution**: `pip install "sqlstream[cli]"`

---

## Next Steps

- [Query Command](query-command.md) - Learn about non-interactive queries
- [Output Formats](output-formats.md) - Formatting options
- [S3 Support](../features/s3-support.md) - Query cloud data
- [SQL Support](../features/sql-support.md) - Supported SQL syntax
