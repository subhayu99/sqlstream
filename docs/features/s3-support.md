# S3 Support

SQLStream can read CSV and Parquet files directly from Amazon S3 buckets, enabling you to query cloud-stored data without downloading files locally.

---

## Installation

S3 support requires the `s3fs` library:

```bash
# Install with S3 support
pip install "sqlstream[s3]"

# Or install all features
pip install "sqlstream[all]"
```

---

## Authentication

SQLStream uses your AWS credentials through `s3fs`. Configure credentials using any of these methods:

### Option 1: AWS Credentials File

```bash
# ~/.aws/credentials
[default]
aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRET_KEY
```

### Option 2: Environment Variables

```bash
export AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
export AWS_DEFAULT_REGION=us-east-1
```

### Option 3: IAM Roles

When running on EC2 or ECS, SQLStream automatically uses IAM role credentials.

---

## Basic Usage

### CLI Usage

Query S3 files using `s3://` URLs:

```bash
# CSV files
sqlstream query "SELECT * FROM 's3://my-bucket/data.csv' WHERE age > 25"

# Parquet files
sqlstream query "SELECT * FROM 's3://my-bucket/data.parquet' LIMIT 100"

# With output formatting
sqlstream query "SELECT * FROM 's3://my-bucket/sales.csv'" --format json

# Using pandas backend for performance
sqlstream query "SELECT * FROM 's3://my-bucket/large.parquet'" --backend pandas
```

### Python API

```python
from sqlstream import query

# Query S3 CSV
results = query("s3://my-bucket/employees.csv").sql("""
    SELECT name, salary
    FROM data
    WHERE department = 'Engineering'
    ORDER BY salary DESC
""")

for row in results:
    print(row)
```

### Interactive Shell

```bash
# Launch shell and query S3
sqlstream shell

# Then run queries
SELECT * FROM 's3://my-bucket/data.csv' WHERE date > '2024-01-01'
```

---

## Advanced Examples

### Example 1: Aggregations on S3 Data

```python
from sqlstream import query

# Sales analysis from S3
results = query("s3://analytics-bucket/sales-2024.parquet").sql("""
    SELECT
        product_category,
        COUNT(*) as num_sales,
        SUM(amount) as total_revenue,
        AVG(amount) as avg_sale
    FROM data
    WHERE sale_date >= '2024-01-01'
    GROUP BY product_category
    ORDER BY total_revenue DESC
""", backend="pandas")

for row in results:
    print(f"{row['product_category']}: ${row['total_revenue']:,.2f}")
```

### Example 2: JOIN S3 and Local Files

```python
from sqlstream.core.query import QueryInline

q = QueryInline()

# Join S3 data with local reference data
results = q.sql("""
    SELECT
        s.customer_id,
        s.order_total,
        c.customer_name,
        c.region
    FROM 's3://orders-bucket/orders.parquet' s
    JOIN 'customers.csv' c ON s.customer_id = c.id
    WHERE s.order_date = '2024-11-30'
""")

for row in results:
    print(row)
```

---

## Performance Tips

### 1. Use Parquet for Large Datasets

Parquet files offer:
- **Faster queries** (columnar format, only read needed columns)
- **Smaller size** (better compression than CSV)
- **Row group pruning** (skip irrelevant data blocks)

### 2. Leverage Column Pruning

```python
# ✅ GOOD: Select specific columns
SELECT name, email FROM data

# ❌ SLOW: Select all columns
SELECT * FROM data
```

### 3. Use Pandas Backend for Aggregations

```python
results = query("s3://bucket/sales.parquet").sql("""
    SELECT region, SUM(revenue) as total
    FROM data
    GROUP BY region
""", backend="pandas")  # 10-100x faster!
```

---

## Troubleshooting

### Missing s3fs Package

```
ImportError: s3fs is required for S3 support. Install with: pip install sqlstream[s3]
```

**Solution**: `pip install "sqlstream[s3]"`

### Access Denied

Ensure your AWS credentials have `s3:GetObject` permission for the bucket.

### No Credentials Configured

Configure AWS credentials using one of the methods described in the Authentication section.

---

## Next Steps

- [Interactive Shell](../interactive-shell/interface-guide.md) - Query S3 interactively
- [Data Sources](data-sources.md) - Learn about other supported formats
- [Performance](../getting-started/core-concepts.md) - Optimize your queries
