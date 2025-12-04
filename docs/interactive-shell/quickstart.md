# Interactive Shell Quick Start

Get started with the SQLStream interactive TUI in 5 minutes!

---

## Step 1: Install SQLStream with Interactive Support

```bash
# Using uv (recommended)
uv tool install "sqlstream[all]"

# Or using pip
pip install "sqlstream[interactive,all]"
```

---

## Step 2: Launch the Shell

### With a File

```bash
$ sqlstream shell employees.csv
```

### Without a File

```bash
$ sqlstream shell
```

Then use the file browser (`Ctrl+O`) to open files.

---

## Step 3: Your First Query

Once the shell opens, you'll see:

- **Top**: Query editor with syntax highlighting
- **Bottom**: Results table
- **Sidebar**: Schema browser or file explorer

Type your first query:

```sql
SELECT * FROM employees WHERE salary > 80000
```

Press **`F5`** or **`Ctrl+Enter`** to execute.

---

## Essential Keyboard Shortcuts

### Query Execution
- **`F5`** or **`Ctrl+Enter`**: Execute query
- **`Ctrl+C`**: Cancel running query

### Navigation
- **`Ctrl+T`**: New tab
- **`Ctrl+W`**: Close tab
- **`Ctrl+Tab`** / **`Ctrl+Shift+Tab`**: Switch between tabs
- **`Ctrl+Up/Down`**: Navigate query history

### Editing
- **`Ctrl+Delete`**: Delete word forward
- **`Ctrl+Backspace`**: Delete word backward
- **`Ctrl+A`**: Select all

### Panels
- **`F2`**: Toggle left sidebar (Schema/Files)
- **`F3`**: Toggle right sidebar (Filters/Export/Config)
- **`Ctrl+O`**: Open file browser
- **`Ctrl+L`**: Cycle layout sizes (50%/60%/70%/80%/100%)

### Backend
- **`F5`** or **`Ctrl+B`**: Cycle backends (Auto → DuckDB → Pandas → Python)

### Results
- **`Ctrl+F`**: Filter results
- **`Ctrl+X`**: Export results (CSV/JSON/Parquet)
- **Click column headers**: Sort by column

### Other
- **`F4`**: Show query execution plan
- **`Ctrl+S`**: Save current state
- **`Ctrl+Q`** or **`Ctrl+D`**: Quit (auto-saves)

---

## Step 4: Working with Multiple Tabs

The interactive shell supports multiple tabs for different queries:

1. Press **`Ctrl+T`** to create a new tab
2. Switch tabs with **`Ctrl+Tab`**
3. Each tab has its own:
   - Query editor
   - Query history
   - Results
   - Schema context

Example workflow:
- **Tab 1**: Exploratory queries (`SELECT * FROM employees`)
- **Tab 2**: Aggregations (`SELECT department, COUNT(*) FROM employees GROUP BY department`)
- **Tab 3**: Complex JOINs across multiple files

---

## Step 5: File Browser

Press **`Ctrl+O`** to open the file browser:

- Navigate with **arrow keys**
- Press **Enter** to load a file
- Shows file tree with directories
- Supports CSV, Parquet, JSON, HTML, Markdown, XML

---

## Step 6: Schema Browser

Toggle with **`F2`** to see:

- All tables/files loaded
- Column names and types
- Row counts
- File paths

This helps you write queries without remembering column names!

---

## Step 7: Backend Toggle

Press **`F5`** or **`Ctrl+B`** to cycle through backends:

1. **Auto** (default): Automatically chooses best backend
2. **DuckDB**: For complex SQL (CTEs, window functions)
3. **Pandas**: For large files, basic queries (10-100x faster)
4. **Python**: Pure Python implementation (learning/debugging)

The current backend is shown in the status bar.

---

## Step 8: Query Plan Visualization

Press **`F4`** to see how your query will be executed:

- Shows optimization steps
- Column pruning details
- Predicate pushdown
- Join strategies

Useful for:
- Understanding performance
- Debugging slow queries
- Learning query optimization

---

## Step 9: Filtering Results

After executing a query:

1. Press **`Ctrl+F`** to open filter sidebar
2. Choose filter type:
   - **Column-specific**: Filter by specific column value
   - **Global search**: Search across all columns
3. Results update live

---

## Step 10: Exporting Results

Press **`Ctrl+X`** to export:

1. Choose format (CSV, JSON, Parquet)
2. Enter filename
3. Results are saved to disk

---

## Common Workflows

### Data Exploration

1. Launch shell: `sqlstream shell data.csv`
2. Check schema (F2)
3. Run exploratory queries:
   ```sql
   SELECT * FROM data LIMIT 10
   SELECT COUNT(*) FROM data
   SELECT DISTINCT category FROM data
   ```
4. Create new tabs for different analyses

### Multi-File Analysis

1. Launch shell: `sqlstream shell`
2. Open file browser (`Ctrl+O`)
3. Load multiple files
4. JOIN them in queries:
   ```sql
   SELECT *
   FROM 'employees.csv' e
   JOIN 'orders.csv' o ON e.id = o.employee_id
   ```

### Report Generation

1. Write complex query
2. Toggle to DuckDB backend (`F5`) for advanced SQL
3. Filter results (`Ctrl+F`)
4. Export to CSV/JSON (`Ctrl+X`)
5. Save query in tab for future use (auto-saved on exit)

---

## State Persistence

The shell automatically saves:

- All open tabs and their queries
- Query history per tab
- Layout preferences
- Backend selection
- File browser state

When you reopen the shell, everything is restored!

---

## Tips & Tricks

### Quick Column Reference

Instead of memorizing column names:

1. Toggle schema browser (`F2`)
2. Write query while viewing columns
3. Toggle back to full-screen editor (`F2`)

### Query History

- Use `Ctrl+Up/Down` to cycle through previous queries
- Works across sessions (history is persisted)
- Each tab has its own history

### Layout Optimization

- Use `Ctrl+L` to cycle between layout sizes
- Large query? Use 70-80% editor space
- Large results? Use 50% split

### Performance Monitoring

- Enable `--time` mode to see execution times
- Use `F4` to see query plan
- Toggle to pandas/duckdb backend for better performance

---

## What's Next?

- [Interface Guide](interface-guide.md) - Complete UI reference
- [Keyboard Shortcuts](keyboard-shortcuts.md) - All shortcuts
- [Workflows](workflows.md) - Advanced workflows and tips

---

## Need Help?

Press **`F1`** in the shell for help, or:

- 📖 [Full Documentation](../index.md)
- 🐛 [Report Issues](https://github.com/subhayu99/sqlstream/issues)
- 💬 [Discussions](https://github.com/subhayu99/sqlstream/discussions)
