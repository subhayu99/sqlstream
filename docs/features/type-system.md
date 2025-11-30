# Type System & Schema Inference

SQLStream includes a robust type system that automatically infers data types from your files and validates operations.

---

## Supported Types

SQLStream supports six core data types:

| Type | Python Types | Example Values |
|------|-------------|----------------|
| `INTEGER` | `int` | `42`, `-100`, `0` |
| `FLOAT` | `float` | `3.14`, `-2.5`, `0.0` |
| `STRING` | `str` | `"hello"`, `"Alice"` |
| `BOOLEAN` | `bool` | `true`, `false` |
| `DATE` | `date`, `datetime` | `2024-01-15`, `2023-12-31T12:34:56.789` |
| `NULL` | `None` | Empty values |

---

## Automatic Type Inference

SQLStream automatically infers types from your data:

### From Python Values

```python
from sqlstream.core.types import infer_type, DataType

infer_type(42)           # DataType.INTEGER
infer_type(3.14)         # DataType.FLOAT
infer_type("hello")      # DataType.STRING
infer_type(True)         # DataType.BOOLEAN
infer_type(None)         # DataType.NULL
```

### From String Values

When reading CSV files, SQLStream tries to infer the most specific type:

```python
infer_type("42")         # DataType.INTEGER (not STRING)
infer_type("3.14")       # DataType.FLOAT
infer_type("true")       # DataType.BOOLEAN
infer_type("2024-01-15") # DataType.DATE
infer_type("hello")      # DataType.STRING
```

### Handling Mixed Types

When a column has mixed types, SQLStream promotes to the most general compatible type:

```python
from sqlstream.core.types import infer_common_type

# Mixed integers and floats -> FLOAT
infer_common_type([1, 2.5, 3])           # DataType.FLOAT

# Mixed types -> STRING
infer_common_type([1, "hello", 3])       # DataType.STRING

# NULL values are ignored
infer_common_type([1, None, 3])          # DataType.INTEGER
```

---

## Schema Inference

SQLStream automatically infers the schema (column names and types) when reading files.

### Basic Usage

```python
from sqlstream import query

# Create query object
q = query("employees.csv")

# Get inferred schema
schema = q.schema()

# Check column types
print(schema["name"])    # DataType.STRING
print(schema["age"])     # DataType.INTEGER
print(schema["salary"])  # DataType.FLOAT
```

### Schema Object

The `Schema` object provides helpful methods:

```python
# Get all column names
columns = schema.get_column_names()
# ['name', 'age', 'salary', 'hire_date']

# Get type of a column
age_type = schema.get_column_type("age")
# DataType.INTEGER

# Check if column exists
if "email" in schema:
    print("Email column exists")

# Validate column
try:
    schema.validate_column("invalid_column")
except ValueError as e:
    print(e)  # Column 'invalid_column' not found
```

### Sample Size

By default, SQLStream samples 100 rows to infer types. You can adjust this:

```python
from sqlstream.readers.csv_reader import CSVReader

reader = CSVReader("large_file.csv")

# Sample only 10 rows (faster)
schema = reader.get_schema(sample_size=10)

# Sample 1000 rows (more accurate)
schema = reader.get_schema(sample_size=1000)
```

---

## Type Checking

### Numeric Types

Check if a type is numeric:

```python
DataType.INTEGER.is_numeric()  # True
DataType.FLOAT.is_numeric()    # True
DataType.STRING.is_numeric()   # False
```

### Type Compatibility

Check if two types can be compared:

```python
# Same types are compatible
DataType.INTEGER.is_comparable(DataType.INTEGER)  # True

# Numeric types are compatible
DataType.INTEGER.is_comparable(DataType.FLOAT)    # True

# String and number are not compatible
DataType.STRING.is_comparable(DataType.INTEGER)   # False

# NULL is compatible with everything
DataType.NULL.is_comparable(DataType.STRING)      # True
```

### Type Coercion

When mixing types, SQLStream promotes to the more general type:

```python
# INT + FLOAT -> FLOAT
DataType.INTEGER.coerce_to(DataType.FLOAT)  # DataType.FLOAT

# NULL + anything -> that type
DataType.NULL.coerce_to(DataType.INTEGER)   # DataType.INTEGER

# Incompatible types -> STRING
DataType.INTEGER.coerce_to(DataType.STRING) # DataType.STRING
```

---

## Practical Examples

### Example 1: Validate Query Columns

```python
from sqlstream import query

q = query("employees.csv")
schema = q.schema()

# Validate SELECT columns before executing
select_cols = ["name", "age", "salary"]
for col in select_cols:
    try:
        schema.validate_column(col)
    except ValueError:
        print(f"Column '{col}' doesn't exist!")
```

### Example 2: Check Column Types

```python
from sqlstream import query

q = query("sales.csv")
schema = q.schema()

# Find all numeric columns
numeric_cols = [
    col for col in schema.get_column_names()
    if schema[col].is_numeric()
]
print(f"Numeric columns: {numeric_cols}")
```

### Example 3: Type-Safe Filtering

```python
from sqlstream import query
from sqlstream.core.types import DataType

q = query("products.csv")
schema = q.schema()

# Only filter on numeric columns
if schema["price"].is_numeric():
    results = q.sql("SELECT * FROM data WHERE price > 100")
else:
    print("Price column is not numeric!")
```

---

## Schema Merging

When working with multiple files (e.g., in JOINs), SQLStream can merge schemas:

```python
from sqlstream.core.types import Schema, DataType

# Two schemas with overlapping columns
schema1 = Schema({
    "id": DataType.INTEGER,
    "value": DataType.INTEGER
})

schema2 = Schema({
    "id": DataType.INTEGER,
    "value": DataType.FLOAT  # Different type!
})

# Merge schemas
merged = schema1.merge(schema2)

# 'value' column is promoted to FLOAT
print(merged["value"])  # DataType.FLOAT
```

---

## Best Practices

### 1. Check Schema Before Querying

```python
schema = query("data.csv").schema()

# Verify expected columns exist
required = ["id", "name", "amount"]
for col in required:
    schema.validate_column(col)  # Raises error if missing
```

### 2. Use Type Information

```python
schema = query("data.csv").schema()

# Only perform numeric operations on numeric columns
if schema["age"].is_numeric():
    results = query("data.csv").sql("SELECT AVG(age) FROM data")
```

### 3. Handle NULL Values

```python
from sqlstream.core.types import DataType

schema = query("data.csv").schema()

# Check if column might have nulls
if schema["optional_field"] == DataType.NULL:
    print("This column is all nulls!")
```

### 4. Sample Size Tradeoff

```python
from sqlstream.readers.csv_reader import CSVReader

# Small sample (fast, less accurate)
schema = CSVReader("file.csv").get_schema(sample_size=10)

# Large sample (slower, more accurate)
schema = CSVReader("file.csv").get_schema(sample_size=1000)
```

---

## Type System API Reference

### DataType Methods

- `is_numeric()` - Check if type is INTEGER or FLOAT
- `is_comparable(other)` - Check if compatible for comparison
- `coerce_to(other)` - Determine result type of coercion

### Schema Methods

- `get_column_names()` - Get list of column names
- `get_column_type(column)` - Get type of column (or None)
- `validate_column(column)` - Raise error if column doesn't exist
- `merge(other)` - Merge two schemas with type coercion
- `from_row(row)` - Create schema from single row
- `from_rows(rows)` - Create schema from multiple rows (more accurate)

### Type Inference Functions

- `infer_type(value)` - Infer type from Python value
- `infer_common_type(values)` - Infer common type from list of values

---

## Next Steps

- [SQL Support](sql-support.md) - See what SQL features use the type system
- [Data Sources](data-sources.md) - Learn about different file formats
- [Python API](../api/overview.md) - Use the type system programmatically
