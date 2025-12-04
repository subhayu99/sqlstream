# Interactive SQL Shell

SQLStream includes a powerful interactive shell built with [Textual](https://textual.textualize.io/), providing a modern terminal UI for exploring and querying data.

---

## Features

- 🎨 **Syntax Highlighting** - Dracula theme for SQL queries
- 📑 **Multiple Query Tabs** - Work on multiple queries simultaneously (`Ctrl+T` to add, `Ctrl+W` to close)
- 💾 **State Persistence** - Automatically saves and restores tabs and queries between sessions
- 📊 **Scrollable Results** - Zebra-striped table with smooth scrolling
- 📜 **Query History** - Navigate previous queries with keyboard shortcuts
- **Word Deletion** - Fast editing with `Ctrl+Delete` and `Ctrl+Backspace`
- 🗂️ **Tabbed Sidebar** - Toggle between Schema browser and File explorer
- 📁 **File Browser** - Tree-structured file navigation in sidebar
- 📄 **Pagination** - Handle large result sets (100 rows per page)
- 🔀 **Column Sorting** - Click headers to sort ascending/descending
- 💾 **Multi-Format Export** - Save results as CSV, JSON, and Parquet
- 🔍 **Filtering** - Search across all columns
- ⚙️ **Backend Toggle** - Cycle through execution backends (`F5` or `Ctrl+B`: auto/duckdb/pandas/python)
- ☁️ **S3 Support** - Query files directly from S3 buckets
- ⚡ **Fast** - Execution time display and row counts

---

## Installation

The interactive shell requires the `textual` library:

```bash
# Install with CLI support
pip install "sqlstream[cli]"

# Install full TUI
pip install "sqlstream[interactive]"

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
| `Ctrl+Backspace` | Delete Word Left | Delete word to the left of cursor |
| `Ctrl+Delete` | Delete Word Right | Delete word to the right of cursor |
| `Ctrl+Q` | Exit | Close the shell (auto-saves state) |
| `Ctrl+D` | Exit | Alternative exit key (auto-saves state) |
| `Ctrl+T` | New Tab | Create a new query tab |
| `Ctrl+W` | Close Tab | Close current query tab |
| `Ctrl+S` | Save State | Manually save current state |
| `F1` | Help | Show help message |
| `F2` | Toggle Sidebar | Show/hide tabbed sidebar (Schema/Files) |
| `F4` | Explain Mode | Show query plan |
| `F5` | Backend Toggle | Cycle through backends (auto/duckdb/pandas/python) |
| `Ctrl+B` | Backend Toggle | Alternative backend cycle key |
| `Ctrl+O` | Open Files Tab | Switch to file browser in sidebar |
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

### 2. Multiple Query Tabs

Work on multiple queries simultaneously without losing your work.

**Creating Tabs**:
- `Ctrl+T` - Create a new tab (automatically named "Query 1", "Query 2", etc.)
- Each tab has its own independent query editor

**Switching Tabs**:
- Click on tab labels to switch between them
- Each tab maintains its own query text

**Closing Tabs**:
- `Ctrl+W` - Close the current tab
- If you close the last tab, a new empty one is created automatically

**Features**:
- Tabs are saved automatically when you exit
- Restored when you restart the shell
- Work on complex queries in one tab while exploring data in another

**Example Workflow**:
```
Tab 1: "Query 1" - Exploratory SELECT * FROM 'data.csv' LIMIT 100
Tab 2: "Query 2" - Complex aggregation with GROUP BY
Tab 3: "Query 3" - JOIN query combining multiple files
```

### 3. State Persistence

Your work is automatically saved and restored between sessions.

**What's Saved**:
- All open tabs (titles and content)
- Query text in each tab
- Tab order

**Storage Location**: `~/.sqlstream_state`

**Behavior**:
- State saves automatically when you exit (`Ctrl+Q` or `Ctrl+D`)
- Manual save available with `Ctrl+S`
- State loads automatically on startup
- If no saved state, starts with one empty tab

**Benefits**:
- Resume work exactly where you left off
- Never lose in-progress queries
- Maintain context across sessions

### 4. Tabbed Sidebar

The sidebar now has two tabs: Schema and Files.

**Schema Tab**:
- Shows all loaded data sources
- Displays column names and types
- Updates when new files are queried

**Files Tab** (`Ctrl+O` to activate):
- Tree-structured file browser
- Navigate your filesystem
- Click files to insert `SELECT * FROM 'file_path'` into active tab

**Toggle**: Press `F2` to show/hide the entire sidebar

### 5. File Browser

Browse and select files directly from the UI.

**Access**: `Ctrl+O` or click "Files" tab in sidebar

**Features**:
- Tree view starting from current directory (`./`)
- Expand/collapse directories
- Click any file to load it into the active query tab

**Auto-Insert**: Selecting a file inserts:
```sql
SELECT * FROM 'path/to/file.csv'
```

**Behavior**:
- Works with the currently active query tab
- Inserts at cursor position if editor has existing content
- Automatically shows sidebar if hidden

### 6. Schema Browser

Press `F2` to toggle the sidebar, then switch to the Schema tab.

**Shows**:
- All loaded files
- Column names (green)
- Data types (dim text)
- Errors (red)

**Examples**:
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

### 7. Pagination

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

### 8. Column Sorting

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

### 9. Multi-Format Export

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

### 10. Filtering

Press `Ctrl+F` to filter current results.

**Features**:
- Case-insensitive search
- Searches across all columns
- Updates row count in status bar

**Example**:
```
Filtered to 45 rows (from 450 total)
```

### 11. Backend Toggle

Press `F5` or `Ctrl+B` to cycle through available execution backends on-the-fly.

**Available Backends**:
- **auto** - Automatically selects best backend (pandas > duckdb > python)
- **duckdb** - Full SQL support with window functions, CTEs, subqueries
- **pandas** - Fast execution for basic queries (10-100x faster than Python)
- **python** - Educational Volcano model implementation

**Status Display**:
```
⚙️ DUCKDB
```

**Behavior**:
- Current backend shown in status bar
- Press `F5` or `Ctrl+B` to cycle to next backend
- Backend preference saved in state (persists between sessions)
- Allows testing queries with different backends without restarting

**Example Workflow**:
```
1. Start with 'auto' backend
2. Press F5 → switches to 'duckdb'
3. Run complex query with window functions
4. Press F5 → switches to 'pandas'
5. Run simple aggregation
6. Press F5 → switches to 'python'
7. Inspect Volcano model behavior
```

**Notes**:
- Some SQL features only work with specific backends
- DuckDB backend required for window functions, CTEs, subqueries
- If a backend is not installed, query will fail with helpful error message


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

1. **Multiple Tabs**: Use tabs to work on different queries simultaneously - one for exploration, one for final analysis
2. **State Persistence**: Your tabs are automatically saved - feel confident closing the shell anytime
3. **Large Datasets**: Use `LIMIT` to preview data quickly in a dedicated tab
4. **S3 Performance**: Use partitioned Parquet files for best performance
5. **History**: Use `Ctrl+Up` to quickly re-run previous queries in any tab
6. **File Browser**: Use `Ctrl+O` to quickly add files to your query without typing paths
7. **Sorting**: Click column headers to explore data patterns
8. **Export**: Export to Parquet for best compression
9. **Sidebar**: Toggle with `F2` to maximize editor space when needed
10. **Manual Save**: Use `Ctrl+S` if you want to save state before experimenting
11. **Backend Toggle**: Use `F5` to test complex SQL features - start with `auto`, switch to `duckdb` for window functions/CTEs

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

- [Query Command](../cli-tool/query-command.md) - Learn about non-interactive queries
- [Output Formats](../cli-tool/output-formats.md) - Formatting options
- [S3 Support](../features/s3-support.md) - Query cloud data
- [SQL Support](../features/sql-support.md) - Supported SQL syntax
