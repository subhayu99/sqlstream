# Common Workflows

Learn efficient workflows for using the SQLStream interactive shell.

---

## Data Exploration Workflow

### Step-by-Step

1. **Launch shell** with file:
   ```bash
   sqlstream shell data.csv
   ```

2. **Check schema** (`F2` to toggle schema browser):
   - See all columns and types
   - Check row counts

3. **Run exploratory queries** in separate tabs:
   - Tab 1: `SELECT * FROM data LIMIT 10`
   - Tab 2: `SELECT COUNT(*) FROM data`
   - Tab 3: `SELECT DISTINCT category FROM data`

4. **Use query history** (`Ctrl+Up/Down`) to refine queries

5. **Export results** when satisfied (`Ctrl+X`)

---

## Multi-File Analysis Workflow

### Joining Multiple Files

1. **Launch shell** without file:
   ```bash
   sqlstream shell
   ```

2. **Open file browser** (`Ctrl+O`)
   - Navigate and load first file
   - Repeat for additional files

3. **Check schemas** (`F2`) for all loaded files

4. **Write JOIN query**:
   ```sql
   SELECT *
   FROM 'customers.csv' c
   JOIN 'orders.csv' o ON c.id = o.customer_id
   WHERE o.amount > 1000
   ```

5. **Toggle to DuckDB backend** (`F5`) for better JOIN performance

6. **Filter and export** results

---

## Report Generation Workflow

### Creating Analysis Reports

1. **Tab 1**: Summary statistics
   ```sql
   SELECT
       COUNT(*) as total_records,
       SUM(amount) as total_revenue,
       AVG(amount) as avg_order_value
   FROM sales
   ```

2. **Tab 2**: Breakdown by category
   ```sql
   SELECT category, COUNT(*) as count, SUM(amount) as revenue
   FROM sales
   GROUP BY category
   ORDER BY revenue DESC
   ```

3. **Tab 3**: Top performers
   ```sql
   SELECT name, SUM(amount) as total
   FROM sales
   GROUP BY name
   ORDER BY total DESC
   LIMIT 10
   ```

4. **Export each result** to different formats (CSV, JSON)

5. **State persists** - reopen shell later to continue work

---

## Data Quality Checking Workflow

### Validating Data

1. **Check for nulls**:
   ```sql
   SELECT COUNT(*) FROM data WHERE important_field IS NULL
   ```

2. **Check for duplicates**:
   ```sql
   SELECT id, COUNT(*) as count
   FROM data
   GROUP BY id
   HAVING count > 1
   ```

3. **Validate ranges**:
   ```sql
   SELECT * FROM data
   WHERE age < 0 OR age > 120
   ```

4. **Use filter** (`Ctrl+F`) to explore problematic records

5. **Export issues** for further investigation

---

## Performance Optimization Workflow

### Optimizing Slow Queries

1. **Run query** with Python backend (default)

2. **Check execution time** in status bar

3. **View query plan** (`F4`) to understand execution

4. **Try Pandas backend** (`F5`):
   - Good for large files, simple queries

5. **Try DuckDB backend** (`F5` again):
   - Best for complex SQL

6. **Compare times** and choose best backend

7. **Refine query** based on plan:
   - Add WHERE filters early
   - Select only needed columns
   - Use appropriate indexes

---

## Iterative Development Workflow

### Developing Complex Queries

1. **Tab 1**: Start simple
   ```sql
   SELECT * FROM data LIMIT 10
   ```

2. **Refine incrementally** using history (`Ctrl+Up`):
   - Add WHERE clause
   - Add GROUP BY
   - Add ORDER BY

3. **Tab 2**: Test subquery separately
   ```sql
   SELECT category, AVG(amount) as avg_amount
   FROM data
   GROUP BY category
   ```

4. **Tab 3**: Combine into final query
   ```sql
   WITH category_avg AS (
       SELECT category, AVG(amount) as avg_amount
       FROM data
       GROUP BY category
   )
   SELECT d.*, c.avg_amount
   FROM data d
   JOIN category_avg c ON d.category = c.category
   WHERE d.amount > c.avg_amount
   ```

5. **Save state** (`Ctrl+S`) at checkpoints

---

## Tips for Efficient Workflows

### Speed Up Your Work

- **Use tabs for context**: Keep different analyses in separate tabs
- **Leverage history**: Press `Ctrl+Up` instead of retyping queries
- **Toggle layout** (`Ctrl+L`): More editor space for complex queries
- **File browser** (`Ctrl+O`): Quickly switch between data files
- **Filter results** (`Ctrl+F`): Narrow down without re-running query
- **Auto-save**: Your work persists between sessions

### Keyboard-First Navigation

- Minimize mouse usage with keyboard shortcuts
- `F2`/`F3` for sidebars, `F4` for plans, `F5` for backends
- `Ctrl+Tab` to switch tabs quickly
- `Ctrl+Enter` to execute without reaching for mouse

---

## See Also

- [Quick Start](quickstart.md) - Get started
- [Interface Guide](interface-guide.md) - Complete UI reference
- [Keyboard Shortcuts](keyboard-shortcuts.md) - All shortcuts
