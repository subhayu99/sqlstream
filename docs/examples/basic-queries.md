# Basic Query Examples

Common query patterns and examples.

## Filtering

```sql
SELECT * FROM data WHERE age > 25
SELECT * FROM data WHERE city = 'NYC'
SELECT * FROM data WHERE salary >= 80000
```

## Sorting

```sql
SELECT * FROM data ORDER BY age DESC
SELECT * FROM data ORDER BY salary DESC LIMIT 10
```

## Aggregations

```sql
SELECT COUNT(*) FROM data
SELECT AVG(salary) FROM data
SELECT department, COUNT(*) FROM data GROUP BY department
```

See more: [Join Examples](joins.md) | [Aggregations](aggregations.md)
