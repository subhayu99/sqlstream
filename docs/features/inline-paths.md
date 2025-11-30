# Inline File Paths

One of SQLStream's most powerful features is the ability to query files directly by specifying their paths in the SQL query. This eliminates the need to define "tables" or load data beforehand.

## Syntax

Simply enclose the file path in single quotes `'` within the `FROM` clause.

```sql
SELECT * FROM 'path/to/file.csv'
```

## Supported Path Types

### Local Files

Relative or absolute paths to local files.

```sql
SELECT * FROM 'data.csv'
SELECT * FROM '/home/user/datasets/sales.parquet'
```

### HTTP/HTTPS URLs

You can query data directly from the web.

```sql
SELECT * FROM 'https://raw.githubusercontent.com/datasets/covid-19/master/data/countries-aggregated.csv'
```

### S3 Buckets

If you have the `s3fs` library installed (`pip install sqlstream[s3]`), you can query files directly from S3.

```sql
SELECT * FROM 's3://my-bucket/my-data.parquet'
```

## Multi-File Queries

You can join data from different locations and formats in a single query.

```sql
SELECT 
    local.id, 
    remote.value 
FROM 'local_data.csv' local
JOIN 's3://bucket/remote_data.parquet' remote
ON local.id = remote.id
```
