# JOIN Operations

SQLStream supports joining data from multiple files, allowing you to combine datasets based on common columns. This is particularly powerful as it allows you to treat separate files (CSV, Parquet) as if they were tables in a relational database.

## Syntax

The syntax follows standard SQL conventions. You can specify the files directly in the query using string literals.

```sql
SELECT 
    t1.col1, 
    t2.col2 
FROM 'file1.csv' t1 
JOIN 'file2.csv' t2 
ON t1.id = t2.id
```

## Supported Join Types

SQLStream currently supports:

- **INNER JOIN**: Returns records that have matching values in both tables.
- **LEFT JOIN**: Returns all records from the left table, and the matched records from the right table.

## Cross-Format Joins

You can join files of different formats. For example, you can join a CSV file with a Parquet file:

```sql
SELECT 
    users.name, 
    orders.amount 
FROM 'users.csv' users 
JOIN 'orders.parquet' orders 
ON users.user_id = orders.user_id
```

## Performance Considerations

- **Backend**: Using the `pandas` backend is generally faster for joins on larger datasets as it leverages optimized merge algorithms.
- **Memory**: The Python backend uses a nested-loop or hash join implementation which streams data, making it memory efficient but potentially slower for large datasets compared to Pandas.
