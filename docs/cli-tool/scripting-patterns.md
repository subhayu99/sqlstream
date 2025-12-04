# CLI Scripting Patterns

Learn how to use SQLStream effectively in shell scripts, automation workflows, and CI/CD pipelines.

---

## Exit Codes

SQLStream follows standard Unix exit code conventions:

- **0**: Success
- **1**: General error (SQL syntax, file not found, etc.)
- **2**: Invalid command-line arguments

### Using Exit Codes in Scripts

```bash
#!/bin/bash

if sqlstream query data.csv "SELECT * FROM data WHERE id = 1" > /dev/null 2>&1; then
    echo "✓ Query succeeded"
else
    echo "✗ Query failed with exit code $?"
    exit 1
fi
```

### Error Handling Pattern

```bash
#!/bin/bash

set -e  # Exit on any error

# Query will fail if file doesn't exist or SQL is invalid
RESULT=$(sqlstream query data.csv "SELECT COUNT(*) FROM data" --format json 2>&1) || {
    echo "ERROR: Query failed"
    echo "$RESULT"
    exit 1
}

echo "Query succeeded: $RESULT"
```

---

## Output Capture & Processing

### Capture JSON Output

```bash
#!/bin/bash

# Capture JSON and process with jq
RESULT=$(sqlstream query employees.csv "SELECT * FROM employees WHERE salary > 80000" --format json)

# Count high earners
COUNT=$(echo "$RESULT" | jq 'length')
echo "High earners: $COUNT"

# Extract names
echo "$RESULT" | jq -r '.[] | .name'
```

### Capture CSV Output

```bash
#!/bin/bash

# Save to file
sqlstream query data.csv "SELECT * FROM data WHERE status = 'pending'" --format csv > pending_items.csv

# Count lines (excluding header)
LINE_COUNT=$(($(wc -l < pending_items.csv) - 1))
echo "Pending items: $LINE_COUNT"
```

### Parse Table Output

```bash
#!/bin/bash

# Get scalar value from table output
TOTAL=$(sqlstream query sales.csv "SELECT SUM(amount) as total FROM sales" --format json | jq -r '.[0].total')

echo "Total sales: \$$TOTAL"
```

---

## Piping & Chaining

### Pipe to Unix Tools

```bash
# Pipe to grep
sqlstream query logs.csv "SELECT * FROM logs" --format csv | grep ERROR

# Pipe to awk
sqlstream query data.csv "SELECT * FROM data" --format csv | awk -F, '$3 > 100 {print $1, $3}'

# Pipe to sort
sqlstream query data.csv "SELECT name, amount FROM data" --format csv | sort -t, -k2 -nr

# Pipe to jq for complex JSON processing
sqlstream query data.csv "SELECT * FROM data" --format json | \
    jq '.[] | select(.status == "active") | {id, name, amount}'
```

### Chain Multiple Queries

```bash
#!/bin/bash

# Extract IDs from first query
IDS=$(sqlstream query employees.csv "SELECT id FROM employees WHERE department = 'Engineering'" --format json | jq -r '.[] | .id')

# Use IDs in second query (construct IN clause)
ID_LIST=$(echo "$IDS" | tr '\n' ',' | sed 's/,$//')
sqlstream query orders.csv "SELECT * FROM orders WHERE employee_id IN ($ID_LIST)" --format json
```

### Multi-Stage Pipeline

```bash
#!/bin/bash

# Stage 1: Extract active users
sqlstream query users.csv "SELECT id, name FROM users WHERE status = 'active'" --format csv > active_users.csv

# Stage 2: Get their orders
sqlstream query "
    SELECT u.name, o.amount
    FROM 'active_users.csv' u
    JOIN 'orders.csv' o ON u.id = o.user_id
" --format csv > active_user_orders.csv

# Stage 3: Aggregate
sqlstream query "
    SELECT name, SUM(amount) as total
    FROM 'active_user_orders.csv'
    GROUP BY name
" --format csv > summary.csv
```

---

## Automation Patterns

### Cron Jobs

```bash
# crontab -e

# Run daily at 2 AM
0 2 * * * /home/user/scripts/daily_report.sh

# Run every hour
0 * * * * sqlstream query /data/logs.csv "SELECT * FROM logs WHERE level = 'ERROR' AND timestamp > datetime('now', '-1 hour')" --format json > /reports/hourly_errors_$(date +\%Y\%m\%d_\%H).json

# Run every Monday at 9 AM
0 9 * * 1 sqlstream query /data/sales.csv "SELECT * FROM sales WHERE date >= date('now', '-7 days')" --format csv > /reports/weekly_sales.csv
```

### Daily Report Script

```bash
#!/bin/bash
# daily_report.sh

DATE=$(date +%Y-%m-%d)
REPORT_DIR="/reports/$DATE"
mkdir -p "$REPORT_DIR"

# Generate multiple reports
echo "Generating reports for $DATE..."

# Sales summary
sqlstream query /data/sales.csv "
    SELECT
        date,
        COUNT(*) as order_count,
        SUM(amount) as total_sales
    FROM sales
    WHERE date = '$DATE'
    GROUP BY date
" --format json > "$REPORT_DIR/sales_summary.json"

# Error log
sqlstream query /data/logs.csv "
    SELECT * FROM logs
    WHERE date = '$DATE' AND level = 'ERROR'
" --format csv > "$REPORT_DIR/errors.csv"

# User activity
sqlstream query /data/users.csv "
    SELECT status, COUNT(*) as count
    FROM users
    GROUP BY status
" --format json > "$REPORT_DIR/user_stats.json"

echo "✓ Reports generated in $REPORT_DIR"
```

### Monitoring Script

```bash
#!/bin/bash
# monitor_data_quality.sh

set -e

# Check for null values
NULL_COUNT=$(sqlstream query data.csv "SELECT COUNT(*) as count FROM data WHERE important_field IS NULL" --format json | jq -r '.[0].count')

if [ "$NULL_COUNT" -gt 0 ]; then
    echo "⚠ WARNING: Found $NULL_COUNT null values in important_field"
    # Send alert
    curl -X POST "https://alerts.example.com/webhook" \
        -H "Content-Type: application/json" \
        -d "{\"message\": \"Data quality issue: $NULL_COUNT null values\"}"
fi

# Check for duplicates
DUP_COUNT=$(sqlstream query data.csv "SELECT id, COUNT(*) as count FROM data GROUP BY id HAVING count > 1" --format json | jq 'length')

if [ "$DUP_COUNT" -gt 0 ]; then
    echo "⚠ WARNING: Found $DUP_COUNT duplicate IDs"
fi

echo "✓ Data quality checks passed"
```

---

## CI/CD Integration

### GitHub Actions

```yaml
# .github/workflows/data-validation.yml
name: Data Validation

on:
  push:
    paths:
      - 'data/**/*.csv'
  schedule:
    - cron: '0 0 * * *'  # Daily at midnight

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install SQLStream
        run: pip install "sqlstream[all]"

      - name: Validate data schema
        run: |
          sqlstream query data/users.csv "SELECT * FROM users LIMIT 1" --format json
          sqlstream query data/orders.csv "SELECT * FROM orders LIMIT 1" --format json

      - name: Check data quality
        run: |
          # No null emails
          COUNT=$(sqlstream query data/users.csv "SELECT COUNT(*) as count FROM users WHERE email IS NULL" --format json | jq -r '.[0].count')
          if [ "$COUNT" -gt 0 ]; then
            echo "ERROR: Found $COUNT users with null emails"
            exit 1
          fi

      - name: Generate test report
        run: |
          sqlstream query data/orders.csv "
            SELECT
              status,
              COUNT(*) as count,
              SUM(amount) as total
            FROM orders
            GROUP BY status
          " --format json > test-report.json

      - name: Upload report
        uses: actions/upload-artifact@v3
        with:
          name: test-report
          path: test-report.json
```

### GitLab CI

```yaml
# .gitlab-ci.yml
stages:
  - validate
  - report

validate_data:
  stage: validate
  image: python:3.11
  before_script:
    - pip install "sqlstream[all]"
  script:
    - sqlstream query data.csv "SELECT COUNT(*) FROM data" --format json
    - |
      # Check for errors
      ERROR_COUNT=$(sqlstream query logs.csv "SELECT COUNT(*) as count FROM logs WHERE level = 'ERROR'" --format json | jq -r '.[0].count')
      if [ "$ERROR_COUNT" -gt 100 ]; then
        echo "Too many errors: $ERROR_COUNT"
        exit 1
      fi

generate_report:
  stage: report
  image: python:3.11
  before_script:
    - pip install "sqlstream[all]"
  script:
    - sqlstream query data.csv "SELECT * FROM data" --format csv > report.csv
  artifacts:
    paths:
      - report.csv
    expire_in: 1 week
```

---

## Environment Variables

Use environment variables for configuration:

```bash
#!/bin/bash

# Configuration
DATA_DIR=${SQLSTREAM_DATA_DIR:-"/data"}
OUTPUT_DIR=${SQLSTREAM_OUTPUT_DIR:-"/output"}
BACKEND=${SQLSTREAM_BACKEND:-"duckdb"}

# Query with environment config
sqlstream query "$DATA_DIR/data.csv" "SELECT * FROM data" \
    --backend "$BACKEND" \
    --format csv > "$OUTPUT_DIR/results.csv"
```

---

## Error Handling Best Practices

### Validate Inputs

```bash
#!/bin/bash

# Check if file exists
if [ ! -f "data.csv" ]; then
    echo "ERROR: data.csv not found"
    exit 1
fi

# Check if file is not empty
if [ ! -s "data.csv" ]; then
    echo "ERROR: data.csv is empty"
    exit 1
fi

# Run query
sqlstream query data.csv "SELECT * FROM data"
```

### Capture and Log Errors

```bash
#!/bin/bash

LOG_FILE="/var/log/sqlstream/queries.log"

# Run query and log
{
    echo "[$(date)] Starting query..."
    if OUTPUT=$(sqlstream query data.csv "SELECT * FROM data" --format json 2>&1); then
        echo "[$(date)] Query succeeded"
        echo "$OUTPUT"
    else
        echo "[$(date)] Query failed"
        echo "$OUTPUT"
        exit 1
    fi
} | tee -a "$LOG_FILE"
```

### Retry Logic

```bash
#!/bin/bash

MAX_RETRIES=3
RETRY_DELAY=5

for i in $(seq 1 $MAX_RETRIES); do
    if sqlstream query data.csv "SELECT * FROM data" > output.json 2>&1; then
        echo "✓ Query succeeded"
        exit 0
    else
        echo "✗ Attempt $i failed"
        if [ $i -lt $MAX_RETRIES ]; then
            echo "Retrying in $RETRY_DELAY seconds..."
            sleep $RETRY_DELAY
        fi
    fi
done

echo "ERROR: Query failed after $MAX_RETRIES attempts"
exit 1
```

---

## Performance Optimization

### Use Appropriate Backend

```bash
# Small files (<100K rows): Python backend (default)
sqlstream query small.csv "SELECT * FROM small"

# Large files (>100K rows): Pandas backend
sqlstream query large.csv "SELECT * FROM large WHERE amount > 1000" --backend pandas

# Complex SQL (CTEs, window functions): DuckDB backend
sqlstream query data.csv "
    WITH ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) as rank
        FROM data
    )
    SELECT * FROM ranked WHERE rank <= 10
" --backend duckdb
```

### Limit Output for Testing

```bash
# Test query with LIMIT before full run
sqlstream query huge_file.csv "SELECT * FROM huge_file WHERE condition LIMIT 10" --format json

# If it works, run full query
sqlstream query huge_file.csv "SELECT * FROM huge_file WHERE condition" --backend pandas > output.json
```

---

## Templates

### Query Template Script

```bash
#!/bin/bash
# query_template.sh - Reusable query script

usage() {
    echo "Usage: $0 <input_file> <output_file> [backend]"
    echo "Example: $0 data.csv output.json duckdb"
    exit 1
}

# Parse arguments
INPUT_FILE=${1:?$(usage)}
OUTPUT_FILE=${2:?$(usage)}
BACKEND=${3:-"auto"}

# Validate input
[ ! -f "$INPUT_FILE" ] && { echo "ERROR: $INPUT_FILE not found"; exit 1; }

# Run query
sqlstream query "$INPUT_FILE" "SELECT * FROM $(basename $INPUT_FILE .csv)" \
    --backend "$BACKEND" \
    --format json > "$OUTPUT_FILE" || {
    echo "ERROR: Query failed"
    exit 1
}

echo "✓ Results saved to $OUTPUT_FILE"
```

---

## Next Steps

- [Query Command Reference](query-command.md) - All CLI options
- [Output Formats](output-formats.md) - Format details
- [Examples](examples/bash-scripts.md) - More script examples
- [Performance Guide](../guides/performance.md) - Optimization tips
