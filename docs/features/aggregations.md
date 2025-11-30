# Aggregations

SQLStream supports standard SQL aggregation functions, allowing you to perform calculations on your data.

## Supported Functions

- **COUNT(*)**: Counts the total number of rows.
- **COUNT(column)**: Counts the number of non-null values in a column.
- **SUM(column)**: Calculates the sum of a numeric column.
- **AVG(column)**: Calculates the average value of a numeric column.
- **MIN(column)**: Finds the minimum value.
- **MAX(column)**: Finds the maximum value.

## GROUP BY

You can group results by one or more columns using the `GROUP BY` clause.

```sql
SELECT 
    category, 
    COUNT(*) as count, 
    AVG(price) as avg_price 
FROM 'products.csv' 
GROUP BY category
```

## Backend Differences

- **Python Backend**: Computes aggregations in a streaming fashion where possible, or accumulates state for grouping.
- **Pandas Backend**: Uses optimized Pandas `groupby` and aggregation methods for high performance.
