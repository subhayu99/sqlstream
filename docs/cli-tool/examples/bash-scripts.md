# Bash Script Examples

Real-world examples of using SQLStream in bash scripts.

---

## Data Validation Script

```bash
#!/bin/bash
# validate_data.sh - Validate data quality

set -e

DATA_FILE="data.csv"
ERRORS=0

echo "Running data validation..."

# Check for null emails
NULL_COUNT=$(sqlstream query "$DATA_FILE" \
    "SELECT COUNT(*) as count FROM data WHERE email IS NULL" \
    --format json | jq -r '.[0].count')

if [ "$NULL_COUNT" -gt 0 ]; then
    echo "✗ Found $NULL_COUNT rows with null emails"
    ERRORS=$((ERRORS + 1))
else
    echo "✓ No null emails"
fi

# Check for duplicates
DUP_COUNT=$(sqlstream query "$DATA_FILE" \
    "SELECT id, COUNT(*) as count FROM data GROUP BY id HAVING count > 1" \
    --format json | jq 'length')

if [ "$DUP_COUNT" -gt 0 ]; then
    echo "✗ Found $DUP_COUNT duplicate IDs"
    ERRORS=$((ERRORS + 1))
else
    echo "✓ No duplicate IDs"
fi

if [ $ERRORS -eq 0 ]; then
    echo "✓ All validation checks passed"
    exit 0
else
    echo "✗ Validation failed with $ERRORS errors"
    exit 1
fi
```

---

## ETL Pipeline Script

```bash
#!/bin/bash
# etl_pipeline.sh - Extract, Transform, Load pipeline

SOURCE_DIR="/data/raw"
OUTPUT_DIR="/data/processed"
DATE=$(date +%Y-%m-%d)

echo "Starting ETL pipeline for $DATE..."

# Extract: Query source files
sqlstream query "$SOURCE_DIR/sales_$DATE.csv" \
    "SELECT * FROM sales WHERE amount > 0" \
    --format csv > "$OUTPUT_DIR/valid_sales.csv"

# Transform: Aggregate by region
sqlstream query "$OUTPUT_DIR/valid_sales.csv" \
    "SELECT region, SUM(amount) as total, COUNT(*) as count
     FROM valid_sales
     GROUP BY region" \
    --format json > "$OUTPUT_DIR/sales_summary.json"

# Load: Could send to database, API, etc.
echo "✓ ETL pipeline complete"
```

---

## Monitoring Script

```bash
#!/bin/bash
# monitor_errors.sh - Monitor error logs

LOG_FILE="/var/log/app.csv"
ALERT_THRESHOLD=10

# Count errors in last hour
ERROR_COUNT=$(sqlstream query "$LOG_FILE" \
    "SELECT COUNT(*) as count FROM logs
     WHERE level = 'ERROR'
     AND timestamp > datetime('now', '-1 hour')" \
    --format json | jq -r '.[0].count')

echo "Errors in last hour: $ERROR_COUNT"

if [ "$ERROR_COUNT" -gt "$ALERT_THRESHOLD" ]; then
    echo "⚠ Alert: Error threshold exceeded"
    # Send alert
    curl -X POST "https://alerts.example.com/webhook" \
        -d "{\"message\": \"$ERROR_COUNT errors in last hour\"}"
fi
```

---

## Report Generation Script

```bash
#!/bin/bash
# generate_report.sh - Daily report generation

REPORT_DATE=$(date +%Y-%m-%d)
REPORT_DIR="/reports/$REPORT_DATE"
mkdir -p "$REPORT_DIR"

echo "Generating reports for $REPORT_DATE..."

# Sales summary
sqlstream query "sales.csv" \
    "SELECT
        date,
        COUNT(*) as orders,
        SUM(amount) as revenue,
        AVG(amount) as avg_order_value
     FROM sales
     WHERE date = '$REPORT_DATE'
     GROUP BY date" \
    --format json > "$REPORT_DIR/sales_summary.json"

# Top customers
sqlstream query "
    SELECT c.name, SUM(o.amount) as total_spent
    FROM 'customers.csv' c
    JOIN 'orders.csv' o ON c.id = o.customer_id
    WHERE o.date = '$REPORT_DATE'
    GROUP BY c.name
    ORDER BY total_spent DESC
    LIMIT 10
" --format csv > "$REPORT_DIR/top_customers.csv"

echo "✓ Reports generated in $REPORT_DIR"
```

---

## See Also

- [Scripting Patterns](../scripting-patterns.md) - Best practices
- [CLI Quickstart](../quickstart.md) - Get started
- [Query Command](../query-command.md) - All options
