# Aggregation Examples

## Basic Counting

Count total rows in a file.

```sql
SELECT COUNT(*) FROM 'logs.csv'
```

## Grouping by Category

Calculate average salary by department.

```sql
SELECT 
    department, 
    AVG(salary) as avg_salary 
FROM 'employees.csv' 
GROUP BY department
```

## Multiple Aggregations

Compute multiple statistics at once.

```sql
SELECT 
    category, 
    COUNT(*) as item_count, 
    MIN(price) as min_price, 
    MAX(price) as max_price, 
    AVG(price) as avg_price 
FROM 'products.csv' 
GROUP BY category
```


