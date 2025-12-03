# Troubleshooting Guide

This guide helps you diagnose and fix common issues when using SQLstream.

## Table of Contents

- [Installation Issues](#installation-issues)
- [SQL Syntax Errors](#sql-syntax-errors)
- [File Access Errors](#file-access-errors)
- [S3 Authentication Errors](#s3-authentication-errors)
- [Type Conversion Errors](#type-conversion-errors)
- [Memory Errors](#memory-errors)
- [Performance Issues](#performance-issues)
- [Interactive Shell Issues](#interactive-shell-issues)
- [Backend Selection Issues](#backend-selection-issues)

---

## Installation Issues

### Problem: `ModuleNotFoundError: No module named 'sqlstream'`

**Cause:** SQLstream is not installed or not in your Python path.

**Solution:**
```bash
# Install from PyPI
pip install sqlstream

# Or install from source
git clone https://github.com/yourusername/sqlstream.git
cd sqlstream
pip install -e .
```

---

### Problem: `ImportError: cannot import name 'Query'`

**Cause:** Circular import or outdated installation.

**Solution:**
```bash
# Uninstall and reinstall
pip uninstall sqlstream
pip install sqlstream --no-cache-dir
```

---

### Problem: Optional dependencies missing (Pandas, DuckDB, S3, etc.)

**Symptom:**
```python
ImportError: pandas is required for this feature. Install with: pip install sqlstream[pandas]
```

**Cause:** Optional dependencies not installed.

**Solution:**
```bash
# Install with specific extras
pip install sqlstream[pandas]      # Pandas backend
pip install sqlstream[duckdb]      # DuckDB backend
pip install sqlstream[parquet]     # Parquet support
pip install sqlstream[s3]          # S3 support
pip install sqlstream[http]        # HTTP sources
pip install sqlstream[html]        # HTML table parsing
pip install sqlstream[cli]         # Interactive shell

# Install all extras
pip install sqlstream[all]
```

---

### Problem: Platform-specific installation errors

#### Windows

**Symptom:** `error: Microsoft Visual C++ 14.0 is required`

**Solution:**
- Install [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
- Or use pre-built wheels: `pip install --only-binary :all: sqlstream[all]`

#### macOS

**Symptom:** `clang: error: unsupported option '-fopenmp'`

**Solution:**
```bash
# Install libomp
brew install libomp

# Then install sqlstream
pip install sqlstream
```

#### Linux

**Symptom:** `error: command 'gcc' failed`

**Solution:**
```bash
# Ubuntu/Debian
sudo apt-get install build-essential python3-dev

# CentOS/RHEL
sudo yum install gcc gcc-c++ python3-devel

# Then install sqlstream
pip install sqlstream
```

---

### Problem: Python version incompatibility

**Symptom:** `ERROR: Package 'sqlstream' requires a different Python: 3.8.0 not in '>=3.9'`

**Solution:**
- SQLstream requires Python 3.9 or higher
- Upgrade Python or use a virtual environment:

```bash
# Using pyenv
pyenv install 3.11
pyenv local 3.11

# Or using conda
conda create -n sqlstream python=3.11
conda activate sqlstream
pip install sqlstream
```

---

## SQL Syntax Errors

### Problem: `ParseError: Expected SELECT but found 'SHOW'`

**Cause:** Unsupported SQL syntax. SQLstream supports a subset of SQL.

**Solution:**
- Use supported SQL features: SELECT, FROM, WHERE, JOIN, GROUP BY, ORDER BY, LIMIT
- Check [SQL Support docs](features/sql-support.md) for full list

**Supported:**
```sql
SELECT name, age FROM 'data.csv' WHERE age > 30
```

**Not Supported (with Python backend):**
```sql
SHOW TABLES  -- Not supported
CREATE TABLE -- Not supported
DESCRIBE users -- Not supported
```

**Workaround:** Use DuckDB backend for advanced SQL features:
```python
from sqlstream import query

result = query().sql("""
    WITH cte AS (SELECT * FROM 'data.csv')
    SELECT * FROM cte
""", backend="duckdb")
```

---

### Problem: `ParseError: Invalid column name`

**Symptom:**
```python
ParseError: Column 'user-name' not found
```

**Cause:** Column names with special characters need quoting.

**Solution:**
```sql
-- Use quotes for column names with special characters
SELECT "user-name", "first.name" FROM 'data.csv'

-- Or rename columns to avoid special characters
SELECT name AS user_name FROM 'data.csv'
```

---

### Problem: `ParseError: Unterminated string literal`

**Symptom:**
```sql
SELECT * FROM 'data.csv' WHERE name = 'O'Brien'
                                       ^ ParseError
```

**Cause:** Single quote inside string not escaped.

**Solution:**
```sql
-- Escape single quotes with another single quote
SELECT * FROM 'data.csv' WHERE name = 'O''Brien'

-- Or use different quoting
SELECT * FROM 'data.csv' WHERE name = "O'Brien"
```

---

### Problem: Reserved keyword conflicts

**Symptom:**
```sql
SELECT from, to, date FROM 'data.csv'  -- ParseError
```

**Cause:** `from`, `to`, `date` are reserved keywords.

**Solution:**
```sql
-- Quote reserved keywords
SELECT "from", "to", "date" FROM 'data.csv'
```

---

### Problem: JOIN syntax errors

**Symptom:**
```sql
SELECT * FROM users, orders WHERE users.id = orders.user_id  -- ParseError
```

**Cause:** Implicit joins (comma syntax) not fully supported.

**Solution:**
```sql
-- Use explicit JOIN syntax
SELECT *
FROM 'users.csv' AS users
INNER JOIN 'orders.csv' AS orders
ON users.id = orders.user_id
```

---

## File Access Errors

### Problem: `FileNotFoundError: CSV file not found: data.csv`

**Cause:** File doesn't exist or path is incorrect.

**Solution:**
```python
# Use absolute path
from pathlib import Path
data_path = Path("/full/path/to/data.csv")
result = query(str(data_path)).sql("SELECT * FROM source")

# Or use relative path from current working directory
import os
print(os.getcwd())  # Check current directory
result = query("./data/data.csv").sql("SELECT * FROM source")
```

---

### Problem: `PermissionError: Permission denied: 'data.csv'`

**Cause:** Insufficient file permissions.

**Solution:**
```bash
# Check file permissions
ls -l data.csv

# Fix permissions
chmod 644 data.csv  # Read/write for owner, read for others

# Or run with appropriate user
sudo chown $USER data.csv
```

---

### Problem: `UnicodeDecodeError: 'utf-8' codec can't decode byte`

**Cause:** File encoding doesn't match specified encoding.

**Solution:**
```python
from sqlstream import query

# Try different encodings
result = query("data.csv", encoding="latin-1").sql("SELECT * FROM source")

# Or detect encoding
import chardet
with open("data.csv", "rb") as f:
    encoding = chardet.detect(f.read())["encoding"]

result = query("data.csv", encoding=encoding).sql("SELECT * FROM source")
```

Common encodings:
- `utf-8` (default, most common)
- `latin-1` (Western European)
- `windows-1252` (Windows default)
- `utf-8-sig` (UTF-8 with BOM)

---

### Problem: `IsADirectoryError: Is a directory: 'data'`

**Cause:** Trying to read a directory instead of a file.

**Solution:**
```python
# Specify the exact file
result = query("data/sales.csv").sql("SELECT * FROM source")

# Or query multiple files
result = query().sql("""
    SELECT * FROM 'data/sales_2023.csv'
    UNION ALL
    SELECT * FROM 'data/sales_2024.csv'
""")
```

---

### Problem: Network paths not working on Windows

**Symptom:**
```python
query("\\\\server\\share\\data.csv")  # Doesn't work
```

**Solution:**
```python
# Use raw strings or forward slashes
query(r"\\server\share\data.csv")  # Raw string
query("//server/share/data.csv")    # Forward slashes (recommended)
```

---

## S3 Authentication Errors

### Problem: `botocore.exceptions.NoCredentialsError: Unable to locate credentials`

**Cause:** AWS credentials not configured.

**Solution:**

**Option 1: Environment variables**
```bash
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
export AWS_DEFAULT_REGION="us-east-1"  # Optional

python your_script.py
```

**Option 2: AWS credentials file**
```bash
# Create ~/.aws/credentials
mkdir -p ~/.aws
cat > ~/.aws/credentials <<EOF
[default]
aws_access_key_id = your_access_key
aws_secret_access_key = your_secret_key
EOF

chmod 600 ~/.aws/credentials
```

**Option 3: IAM role (EC2/ECS)**
- No credentials needed if running on AWS with IAM role attached
- Ensure IAM role has S3 read permissions

---

### Problem: `botocore.exceptions.ClientError: An error occurred (403) Forbidden`

**Cause:** Insufficient S3 permissions.

**Solution:**

1. **Check bucket policy:**
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"AWS": "arn:aws:iam::ACCOUNT:user/USERNAME"},
    "Action": ["s3:GetObject", "s3:ListBucket"],
    "Resource": [
      "arn:aws:s3:::your-bucket",
      "arn:aws:s3:::your-bucket/*"
    ]
  }]
}
```

2. **Check IAM policy:**
```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "s3:GetObject",
      "s3:ListBucket"
    ],
    "Resource": [
      "arn:aws:s3:::your-bucket",
      "arn:aws:s3:::your-bucket/*"
    ]
  }]
}
```

3. **Test access:**
```bash
aws s3 ls s3://your-bucket/
```

---

### Problem: `s3fs.core.S3FileSystemError: Access Denied`

**Cause:** Credentials work but lack specific permissions.

**Solution:**
```python
# Test with public bucket first
result = query("s3://public-bucket/data.csv").sql("SELECT * FROM source")

# Check specific object permissions
import boto3
s3 = boto3.client('s3')
response = s3.head_object(Bucket='your-bucket', Key='data.csv')
print(response)
```

---

### Problem: Cross-region S3 access is slow

**Symptom:** Queries against S3 bucket in different region are very slow.

**Solution:**
```python
# Specify region explicitly
import s3fs
fs = s3fs.S3FileSystem(client_kwargs={'region_name': 'us-west-2'})

# Or use endpoint URL
result = query("s3://bucket/data.csv").sql("SELECT * FROM source")
```

**Better solution:** Copy data to bucket in same region as your application.

---

## Type Conversion Errors

### Problem: `TypeError: '>' not supported between instances of 'str' and 'int'`

**Cause:** Type inference failed or column has mixed types.

**Solution:**

```python
# Type inference happens per-row, may vary
# Manually cast in SQL
result = query("data.csv").sql("""
    SELECT * FROM source
    WHERE CAST(age AS INTEGER) > 30
""")
```

**Or** Pre-process CSV to ensure consistent types:
```python
import pandas as pd

df = pd.read_csv("data.csv")
df['age'] = df['age'].astype(int)  # Force type
df.to_csv("data_clean.csv", index=False)

result = query("data_clean.csv").sql("SELECT * FROM source WHERE age > 30")
```

---

### Problem: Dates not being parsed

**Symptom:**
```python
# Date column remains as string
result = query("events.csv").sql("SELECT date FROM source")
print(type(result.rows[0]['date']))  # <class 'str'>
```

**Cause:** SQLstream's CSV reader doesn't auto-detect dates (yet).

**Solution:**
```python
# Use DuckDB backend for automatic date parsing
result = query("events.csv").sql("""
    SELECT * FROM source
    WHERE date > '2024-01-01'
""", backend="duckdb")

# Or manually parse dates
from datetime import datetime
rows = result.rows
for row in rows:
    row['date'] = datetime.strptime(row['date'], '%Y-%m-%d')
```

---

### Problem: NULL handling inconsistencies

**Symptom:**
```python
# Empty strings vs None
row['field'] == ""   # Sometimes
row['field'] is None  # Other times
```

**Cause:** CSV empty cells become `None`, but type inference varies.

**Solution:**
```python
# Normalize NULL checks
def is_null(value):
    return value is None or value == "" or value == "null"

# Or use SQL NULL handling
result = query("data.csv").sql("""
    SELECT * FROM source
    WHERE field IS NOT NULL
""")
```

---

## Memory Errors

### Problem: `MemoryError` or system runs out of RAM

**Cause:** Loading large files into memory.

**Solution:**

**1. Use Pandas backend (more memory efficient):**
```python
result = query("large_file.csv").sql("""
    SELECT * FROM source WHERE age > 30
""", backend="pandas")
```

**2. Use DuckDB backend (disk-based, handles GB-scale):**
```python
result = query("very_large_file.csv").sql("""
    SELECT * FROM source WHERE age > 30
""", backend="duckdb")
```

**3. Use Parquet instead of CSV:**
```python
# Convert CSV to Parquet first (one-time cost)
import pandas as pd
df = pd.read_csv("large.csv")
df.to_parquet("large.parquet")

# Query Parquet (columnar format, much faster)
result = query("large.parquet").sql("SELECT * FROM source")
```

**4. Filter early with predicate pushdown:**
```python
# GOOD: Filter pushed down to reader
result = query("large.csv").sql("""
    SELECT name FROM source WHERE age > 30
""")

# BAD: Loads all data then filters
result = query("large.csv").sql("SELECT name FROM source")
# Then filter in Python - too late!
```

**5. Select only needed columns:**
```python
# GOOD: Only loads 2 columns
result = query("large.csv").sql("SELECT name, age FROM source")

# BAD: Loads all 50 columns
result = query("large.csv").sql("SELECT * FROM source")
```

---

### Problem: `Cannot allocate memory` on Linux

**Cause:** System swap disabled or insufficient.

**Solution:**
```bash
# Check swap
free -h

# Add swap space (temporary)
sudo fallocate -l 4G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

---

### Problem: Pandas backend still running out of memory

**Symptom:**
```python
result = query("100GB_file.csv").sql("...", backend="pandas")
# MemoryError
```

**Solution:**

Use chunking with Pandas backend:
```python
# Process in chunks
import pandas as pd

chunks = []
for chunk in pd.read_csv("large.csv", chunksize=100000):
    filtered = chunk[chunk['age'] > 30]
    chunks.append(filtered)

result_df = pd.concat(chunks)
```

**Or** switch to DuckDB backend which handles larger-than-RAM data:
```python
result = query("100GB_file.csv").sql("...", backend="duckdb")
```

---

## Performance Issues

### Problem: Queries are very slow

**Symptom:** Simple query takes minutes instead of seconds.

**Diagnostic steps:**

**1. Check backend selection:**
```python
# Python backend is slowest (educational/debugging)
result = query("data.csv").sql("...", backend="python")  # Slow

# Pandas backend is 10-100x faster
result = query("data.csv").sql("...", backend="pandas")  # Fast

# DuckDB backend is 10-1000x faster for complex queries
result = query("data.csv").sql("...", backend="duckdb")  # Fastest
```

**2. Check file format:**
```python
# CSV is slowest (text parsing)
result = query("data.csv").sql("...")  # Slow

# Parquet is 10-100x faster (columnar, compressed)
result = query("data.parquet").sql("...")  # Fast
```

**3. Use EXPLAIN to see query plan:**
```python
result = query("data.csv").sql("SELECT * FROM source WHERE age > 30")
print(result.explain())  # Shows optimizations applied
```

---

### Problem: HTTP sources are slow

**Symptom:** Querying HTTP URL takes a long time every time.

**Cause:** No caching enabled or cache expired.

**Solution:**

HTTP reader automatically caches in `~/.cache/sqlstream/http/`:
```python
# First query: Downloads and caches
result = query("https://example.com/data.csv").sql("SELECT * FROM source")

# Subsequent queries: Uses cache (fast)
result = query("https://example.com/data.csv").sql("SELECT * FROM source")
```

**Check cache:**
```bash
ls -lh ~/.cache/sqlstream/http/
```

**Clear cache if needed:**
```bash
rm -rf ~/.cache/sqlstream/http/
```

---

### Problem: S3 queries are slow

**Symptom:** Queries against S3 are much slower than local files.

**Solutions:**

**1. Use Parquet on S3:**
```python
# CSV on S3: Slow (downloads entire file)
result = query("s3://bucket/large.csv").sql("SELECT * FROM source WHERE age > 30")

# Parquet on S3: Fast (columnar reads, compression)
result = query("s3://bucket/large.parquet").sql("SELECT * FROM source WHERE age > 30")
```

**2. Use partitioned Parquet:**
```
s3://bucket/data/
  year=2023/
    month=01/data.parquet
    month=02/data.parquet
  year=2024/
    month=01/data.parquet
```

```python
# Only reads relevant partitions
result = query("s3://bucket/data/year=2024/").sql("SELECT * FROM source")
```

**3. Ensure bucket is in same region:**
- Cross-region data transfer is slow and expensive
- Use `aws s3 cp` or AWS DataSync to move data to same region

---

### Problem: JOIN queries are slow

**Symptom:** Query with JOIN takes very long.

**Solution:**

**1. Join smaller table on the left:**
```python
# GOOD: Small table LEFT JOIN large table
SELECT * FROM 'small.csv' AS s
LEFT JOIN 'large.csv' AS l ON s.id = l.id

# BAD: Large table LEFT JOIN small table
SELECT * FROM 'large.csv' AS l
LEFT JOIN 'small.csv' AS s ON l.id = s.id
```

**2. Filter before joining:**
```python
# GOOD: Filter first
SELECT * FROM 'users.csv' AS u
INNER JOIN 'orders.csv' AS o ON u.id = o.user_id
WHERE u.active = true AND o.status = 'completed'

# BAD: Join first, filter later (processes all data)
```

**3. Use DuckDB backend for large joins:**
```python
result = query().sql("""
    SELECT * FROM 'large1.csv' AS a
    INNER JOIN 'large2.csv' AS b ON a.id = b.id
""", backend="duckdb")  # Much faster for large joins
```

---

### Problem: GROUP BY queries are slow

**Symptom:** Aggregation queries take a long time.

**Solution:**

```python
# Use DuckDB backend for fast aggregations
result = query("large.csv").sql("""
    SELECT category, COUNT(*), SUM(amount)
    FROM source
    GROUP BY category
""", backend="duckdb")

# Or use Pandas backend
result = query("large.csv").sql("""
    SELECT category, COUNT(*), SUM(amount)
    FROM source
    GROUP BY category
""", backend="pandas")
```

---

## Interactive Shell Issues

### Problem: Shell doesn't start or crashes immediately

**Symptom:**
```bash
sqlstream shell
# ImportError: No module named 'textual'
```

**Cause:** CLI dependencies not installed.

**Solution:**
```bash
pip install sqlstream[cli]
```

---

### Problem: Shell rendering issues (garbled text, wrong colors)

**Cause:** Terminal doesn't support required features.

**Solution:**

**1. Use a modern terminal:**
- **Windows**: Windows Terminal, not CMD.exe
- **macOS**: iTerm2 or built-in Terminal.app
- **Linux**: GNOME Terminal, Konsole, or Alacritty

**2. Set TERM environment variable:**
```bash
export TERM=xterm-256color
sqlstream shell
```

**3. Check terminal color support:**
```bash
tput colors  # Should output 256 or more
```

---

### Problem: Query history not persisting

**Symptom:** Previous queries not available after restarting shell.

**Cause:** History file not writable or corrupted.

**Solution:**
```bash
# Check history file
ls -l ~/.sqlstream/history.txt

# Fix permissions
chmod 644 ~/.sqlstream/history.txt

# Or delete and recreate
rm ~/.sqlstream/history.txt
# Shell will recreate it on next run
```

---

### Problem: Keyboard shortcuts not working

**Symptom:** F2, F4, Ctrl+X don't work in shell.

**Cause:** Terminal intercepts key codes.

**Solution:**

Check terminal preferences to ensure function keys are passed to applications:

- **macOS Terminal:** Preferences → Profiles → Keyboard → "Use Option as Meta key"
- **iTerm2:** Preferences → Keys → Key Mappings
- **Windows Terminal:** Settings → Actions

---

## Backend Selection Issues

### Problem: `ImportError: DuckDB not available`

**Symptom:**
```python
result = query("data.csv").sql("...", backend="duckdb")
# ImportError: DuckDB is not installed
```

**Solution:**
```bash
pip install sqlstream[duckdb]
# Or
pip install duckdb
```

---

### Problem: DuckDB backend doesn't support feature

**Symptom:**
```python
result = query("data.csv").sql("SELECT custom_function(col) FROM source", backend="duckdb")
# Error: Function 'custom_function' not found
```

**Solution:**

DuckDB has extensive built-in functions but may not support custom Python functions.

**Workaround:** Use Python backend:
```python
result = query("data.csv").sql("SELECT * FROM source", backend="python")
# Then process with Python
```

---

### Problem: Backend auto-selection chooses wrong backend

**Symptom:** Query runs slowly because wrong backend was chosen.

**Solution:**

Explicitly specify backend:
```python
# Force DuckDB for complex analytics
result = query("data.csv").sql("""
    SELECT category, AVG(price)
    FROM source
    GROUP BY category
    HAVING AVG(price) > 100
""", backend="duckdb")

# Force Pandas for medium-size data
result = query("data.csv").sql("SELECT * FROM source", backend="pandas")

# Force Python for debugging or custom logic
result = query("data.csv").sql("SELECT * FROM source", backend="python")
```

---

## Still Having Issues?

If your problem isn't covered here:

1. **Check the FAQ:** [docs/faq.md](faq.md)
2. **Check Limitations:** [docs/limitations.md](limitations.md)
3. **Enable verbose logging:**
   ```python
   import logging
   logging.basicConfig(level=logging.DEBUG)
   ```

4. **File a bug report:** [GitHub Issues](https://github.com/yourusername/sqlstream/issues)
   - Include: OS, Python version, SQLstream version, minimal reproducible example
   - Include: Full error traceback
   - Include: Query and data sample (if possible)

5. **Ask on Discussions:** [GitHub Discussions](https://github.com/yourusername/sqlstream/discussions)

---

**Last Updated:** 2025-12-03
