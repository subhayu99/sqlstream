# Real-World Use Cases

## Sales Analysis

Analyze sales data spread across daily CSV files.

```sql
SELECT 
    product_id, 
    SUM(amount) as total_sales 
FROM 'sales_*.csv' 
GROUP BY product_id 
ORDER BY total_sales DESC 
LIMIT 5
```
*(Note: Wildcard support depends on shell expansion or specific reader implementation)*

## Log Analysis

Find the top 10 IP addresses with the most 404 errors from a web server log.

```sql
SELECT 
    ip_address, 
    COUNT(*) as error_count 
FROM 'access_logs.csv' 
WHERE status_code = 404 
GROUP BY ip_address 
ORDER BY error_count DESC 
LIMIT 10
```

## Data Quality Check

Identify records with missing critical information.

```sql
SELECT * 
FROM 'users.parquet' 
WHERE email IS NULL OR phone IS NULL
```

## Cross-Reference Data

Check which users in your CSV database have placed orders recorded in an S3 Parquet data lake.

```sql
SELECT 
    u.email 
FROM 'local_users.csv' u 
JOIN 's3://datalake/orders.parquet' o 
ON u.id = o.user_id 
GROUP BY u.email
```
