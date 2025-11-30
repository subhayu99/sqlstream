# JOIN Examples

## Joining CSV Files

Suppose you have `users.csv` and `orders.csv`.

**users.csv**
```csv
id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
```

**orders.csv**
```csv
order_id,user_id,amount
101,1,50.00
102,1,25.00
103,2,100.00
```

**Query:**
```sql
SELECT 
    u.name, 
    o.amount 
FROM 'users.csv' u 
JOIN 'orders.csv' o 
ON u.id = o.user_id
```

## Joining CSV and Parquet

**Query:**
```sql
SELECT 
    p.product_name, 
    s.quantity 
FROM 'products.csv' p 
JOIN 'sales.parquet' s 
ON p.id = s.product_id
```

## Self Join

**Query:**
```sql
SELECT 
    e1.name as employee, 
    e2.name as manager 
FROM 'employees.csv' e1 
JOIN 'employees.csv' e2 
ON e1.manager_id = e2.id
```
