# SQLStream Implementation Plan

## Project Overview

Building SQLStream - a lightweight SQL query engine for data exploration with lazy evaluation and intelligent optimizations. Starting from scratch with a full MVP implementation.

## Project Structure

```
SQLstream/
├── sqlstream/                      # Main package
│   ├── __init__.py
│   ├── __main__.py                # Entry point for python -m sqlstream
│   │
│   ├── core/                      # Core query engine
│   │   ├── __init__.py
│   │   ├── query.py               # Main Query class & API
│   │   ├── executor.py            # Pull-based execution engine
│   │   ├── planner.py             # Query planning & optimization
│   │   └── types.py               # Type system & inference
│   │
│   ├── sql/                       # SQL parsing & AST
│   │   ├── __init__.py
│   │   ├── parser.py              # SQL parser (start simple, no external deps)
│   │   ├── ast_nodes.py           # AST node definitions
│   │   └── validator.py           # Query validation
│   │
│   ├── readers/                   # Data source readers
│   │   ├── __init__.py
│   │   ├── base.py                # BaseReader interface
│   │   ├── csv_reader.py          # CSV reader with lazy iteration
│   │   ├── parquet_reader.py      # Parquet reader (requires pyarrow)
│   │   ├── json_reader.py         # JSON/JSONL reader
│   │   └── http_reader.py         # HTTP streaming wrapper
│   │
│   ├── operators/                 # Query operators (Volcano model)
│   │   ├── __init__.py
│   │   ├── scan.py                # Table scan operator
│   │   ├── filter.py              # WHERE clause filter
│   │   ├── project.py             # SELECT projection
│   │   ├── limit.py               # LIMIT operator
│   │   ├── sort.py                # ORDER BY (with external sort)
│   │   ├── aggregate.py           # GROUP BY (incremental aggregation)
│   │   └── join.py                # JOIN (hash join with bloom filter)
│   │
│   ├── optimizers/                # Query optimizers
│   │   ├── __init__.py
│   │   ├── predicate_pushdown.py  # Push filters to readers
│   │   ├── column_pruning.py      # Only read needed columns
│   │   ├── partition_pruning.py   # Skip irrelevant partitions
│   │   └── statistics.py          # Statistics-based optimization
│   │
│   ├── cli/                       # Command-line interface
│   │   ├── __init__.py
│   │   ├── main.py                # CLI entry point (will use Click later)
│   │   ├── commands.py            # Subcommands (query, profile, schema)
│   │   └── formatters.py          # Output formatting (JSON, CSV, table)
│   │
│   └── utils/                     # Utilities
│       ├── __init__.py
│       ├── schema.py              # Schema inference & evolution
│       ├── partitions.py          # Partition discovery
│       └── sampling.py            # Data sampling for statistics
│
├── tests/                         # Test suite
│   ├── __init__.py
│   ├── conftest.py                # Pytest fixtures
│   ├── test_core/
│   ├── test_sql/
│   ├── test_readers/
│   ├── test_operators/
│   ├── test_optimizers/
│   └── integration/               # End-to-end tests
│
├── setup.py                       # Package setup
├── pyproject.toml                 # Modern Python packaging
├── requirements.txt               # Production dependencies
├── requirements-dev.txt           # Development dependencies
├── .gitignore
├── README.md
└── SQLStream - Technical Design Document.md  # Existing design doc
```

## Implementation Phases

### Phase 0: Project Bootstrap (Foundation)

**Goal:** Set up development environment and basic project structure

**Tasks:**
1. Create virtual environment
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

2. Create initial project structure (all directories)

3. Set up packaging files:
   - `setup.py` - Basic package metadata
   - `pyproject.toml` - Modern Python project config
   - `requirements.txt` - Start empty, add as needed
   - `requirements-dev.txt` - pytest, black, mypy

4. Create `.gitignore` for Python projects

5. Initialize git repository

**Deliverables:**
- Working virtual environment
- Importable `sqlstream` package
- Pytest running (even with no tests yet)

**Dependencies:** None yet (pure Python)

---

### Phase 1: Core Foundation (Parser + Basic Operators)

**Goal:** Parse simple SQL and execute basic queries on in-memory data

#### 1.1 SQL Parser (Barebones)

**File:** `sqlstream/sql/parser.py`

**Implementation Strategy:**
- Start with hand-written recursive descent parser (no external dependencies)
- Support minimal SQL subset:
  - `SELECT column1, column2 FROM source`
  - `WHERE column = value` (simple predicates only)
  - `LIMIT n`
- Parse into AST nodes

**AST Nodes:** `sqlstream/sql/ast_nodes.py`
```python
@dataclass
class SelectStatement:
    columns: List[str]  # ['*'] or ['col1', 'col2']
    source: str
    where: Optional[WhereClause]
    limit: Optional[int]

@dataclass
class WhereClause:
    conditions: List[Condition]  # Start with simple AND only

@dataclass
class Condition:
    column: str
    operator: str  # '=', '>', '<', '>=', '<=', '!='
    value: Any
```

**Tests:** `tests/test_sql/test_parser.py`
- Parse SELECT *
- Parse SELECT with column list
- Parse WHERE with single condition
- Parse LIMIT
- Error handling for invalid SQL

#### 1.2 CSV Reader (Lazy Iterator)

**File:** `sqlstream/readers/csv_reader.py`

**Implementation:**
- Use Python's built-in `csv` module
- Yield rows as dictionaries
- Support basic type inference (int, float, string)
- Handle headers automatically

**Interface:** `sqlstream/readers/base.py`
```python
class BaseReader:
    def read_lazy(self) -> Iterator[Dict[str, Any]]:
        """Yield rows as dictionaries"""
        raise NotImplementedError

    def supports_pushdown(self) -> bool:
        return False

    def set_filter(self, conditions: List[Condition]) -> None:
        pass  # Override in subclasses that support pushdown

    def set_columns(self, columns: List[str]) -> None:
        pass  # Override for column pruning
```

**Tests:** `tests/test_readers/test_csv_reader.py`
- Read CSV with headers
- Type inference
- Empty files
- Malformed rows (skip with warning)

#### 1.3 Basic Operators

**Files:** `sqlstream/operators/{scan,filter,project,limit}.py`

**Volcano Model Pattern:**
```python
class Operator:
    def __init__(self, child: Optional[Operator] = None):
        self.child = child

    def __iter__(self):
        """Pull-based iteration"""
        raise NotImplementedError

# Example: Filter operator
class Filter(Operator):
    def __init__(self, child: Operator, condition: Condition):
        super().__init__(child)
        self.condition = condition

    def __iter__(self):
        for row in self.child:
            if self._evaluate(row, self.condition):
                yield row
```

**Operators to implement:**
- **Scan:** Wraps reader, yields all rows
- **Filter:** Evaluates WHERE conditions
- **Project:** Selects specific columns (creates dict views)
- **Limit:** Stops after N rows

**Tests:** `tests/test_operators/`
- Each operator in isolation
- Operator chaining
- Edge cases (empty input, limit=0)

#### 1.4 Query Executor

**File:** `sqlstream/core/executor.py`

**Responsibility:**
- Take AST and build operator tree
- Execute query (pull from root operator)
- Return iterator of results

**Implementation:**
```python
class Executor:
    def execute(self, ast: SelectStatement, reader: BaseReader) -> Iterator[Dict]:
        # Build operator tree bottom-up
        plan = Scan(reader)

        if ast.where:
            plan = Filter(plan, ast.where.conditions)

        if ast.columns != ['*']:
            plan = Project(plan, ast.columns)

        if ast.limit:
            plan = Limit(plan, ast.limit)

        return iter(plan)
```

**Tests:** `tests/test_core/test_executor.py`
- End-to-end: Parse -> Execute -> Verify results
- Multiple operators combined

#### 1.5 Main Query API

**File:** `sqlstream/core/query.py`

**Public API:**
```python
def query(source: str) -> Query:
    """Main entry point"""
    return Query(source)

class Query:
    def __init__(self, source: str):
        self.source = source
        self.reader = self._create_reader(source)

    def sql(self, sql_query: str) -> 'QueryResult':
        ast = parse(sql_query)
        return QueryResult(self.reader, ast)

    def _create_reader(self, source: str):
        # Auto-detect format from extension
        if source.endswith('.csv'):
            return CSVReader(source)
        # Add more formats later

class QueryResult:
    def __iter__(self):
        # Execute and yield results
        executor = Executor()
        return executor.execute(self.ast, self.reader)

    def to_list(self):
        return list(self)
```

**Tests:** `tests/test_core/test_query.py`
- Full API usage
- Format auto-detection
- Error handling

**Phase 1 Deliverable:**
```python
# This should work:
from sqlstream import query

results = query("data.csv").sql("SELECT name, age WHERE age > 25 LIMIT 10")
for row in results:
    print(row)
```

**Dependencies Added:** None (pure Python + stdlib)

---

### Phase 2: Optimization Layer (Pushdown + Pruning)

**Goal:** Add predicate pushdown and column pruning for performance

#### 2.1 Query Planner

**File:** `sqlstream/core/planner.py`

**Responsibility:**
- Analyze AST before execution
- Apply optimization rules
- Generate optimized execution plan

**Implementation:**
```python
class Planner:
    def optimize(self, ast: SelectStatement, reader: BaseReader) -> ExecutionPlan:
        plan = ExecutionPlan()

        # Rule 1: Predicate pushdown
        if ast.where and reader.supports_pushdown():
            pushable = self._extract_pushable_predicates(ast.where)
            reader.set_filter(pushable)
            ast.where = self._remove_pushed_predicates(ast.where)

        # Rule 2: Column pruning
        needed_columns = self._analyze_columns(ast)
        if reader.supports_column_selection():
            reader.set_columns(needed_columns)

        return plan
```

**Tests:** `tests/test_core/test_planner.py`
- Detect pushable predicates
- Column dependency analysis

#### 2.2 Predicate Pushdown

**File:** `sqlstream/optimizers/predicate_pushdown.py`

**Rules for safe pushdown:**
- Simple column comparisons: `column op value`
- AND combinations
- NOT: Complex expressions (UDFs, cross-column)

**CSV Reader Enhancement:**
```python
class CSVReader(BaseReader):
    def supports_pushdown(self) -> bool:
        return True

    def set_filter(self, conditions: List[Condition]):
        self.filter_conditions = conditions

    def read_lazy(self):
        for row in self._read_raw():
            # Apply filter during read
            if self._matches_filter(row):
                yield row
```

**Tests:** `tests/test_optimizers/test_predicate_pushdown.py`
- Verify filters applied at read time
- Measure I/O reduction

#### 2.3 Column Pruning

**File:** `sqlstream/optimizers/column_pruning.py`

**Column Dependency Analysis:**
- Parse SELECT columns
- Parse WHERE columns
- Union = required columns

**CSV Reader Enhancement:**
```python
class CSVReader(BaseReader):
    def set_columns(self, columns: List[str]):
        self.required_columns = columns

    def read_lazy(self):
        reader = csv.DictReader(self.file)
        for row in reader:
            # Only yield required columns
            yield {k: row[k] for k in self.required_columns if k in row}
```

**Tests:** `tests/test_optimizers/test_column_pruning.py`
- Verify only needed columns read
- Nested column references

**Phase 2 Deliverable:**
- Queries run faster with pushdown
- Memory usage reduced with column pruning
- `--explain` flag shows optimization plan (add to CLI)

**Dependencies Added:** None yet

---

### Phase 3: Parquet Support + Statistics

**Goal:** Add Parquet reader with row group statistics optimization

#### 3.1 Add PyArrow Dependency

**Update:** `requirements.txt`
```
pyarrow>=10.0.0
```

**Reason:** Parquet format requires specialized library

#### 3.2 Parquet Reader

**File:** `sqlstream/readers/parquet_reader.py`

**Implementation:**
```python
import pyarrow.parquet as pq

class ParquetReader(BaseReader):
    def __init__(self, path: str):
        self.parquet_file = pq.ParquetFile(path)
        self.filter_conditions = []
        self.required_columns = None

    def supports_pushdown(self) -> bool:
        return True

    def read_lazy(self) -> Iterator[Dict]:
        # Get row groups
        row_groups = self._select_row_groups()

        for rg_idx in row_groups:
            # Read row group with column selection
            table = self.parquet_file.read_row_group(
                rg_idx,
                columns=self.required_columns
            )

            # Convert to dicts and yield
            for batch in table.to_batches():
                for row in batch.to_pydict():
                    if self._matches_filter(row):
                        yield row

    def _select_row_groups(self) -> List[int]:
        """Use statistics to skip row groups"""
        if not self.filter_conditions:
            return range(self.parquet_file.num_row_groups)

        selected = []
        metadata = self.parquet_file.metadata

        for i in range(metadata.num_row_groups):
            rg_meta = metadata.row_group(i)
            if self._row_group_matches(rg_meta):
                selected.append(i)

        return selected

    def _row_group_matches(self, rg_meta) -> bool:
        """Check if row group stats overlap with filter"""
        for condition in self.filter_conditions:
            column_meta = rg_meta.column(condition.column)
            min_val = column_meta.statistics.min
            max_val = column_meta.statistics.max

            # If filter is "age > 60" and max_age = 55, skip this RG
            if condition.operator == '>' and max_val <= condition.value:
                return False
            # Add more operators...

        return True
```

**Tests:** `tests/test_readers/test_parquet_reader.py`
- Read Parquet file
- Row group pruning with statistics
- Column selection
- Compare performance vs reading all data

#### 3.3 Statistics Module

**File:** `sqlstream/optimizers/statistics.py`

**Track query statistics:**
- Row groups scanned vs skipped
- Bytes read
- Time elapsed
- Memory usage

**Tests:** `tests/test_optimizers/test_statistics.py`
- Measure optimization effectiveness

**Phase 3 Deliverable:**
- Parquet files readable
- Row group pruning working
- Significant performance improvement on selective queries

**Dependencies Added:** `pyarrow`

---

### Phase 4: Advanced SQL (GROUP BY + ORDER BY)

**Goal:** Support aggregations and sorting

#### 4.1 Extend SQL Parser

**Update:** `sqlstream/sql/parser.py`

**New AST nodes:**
```python
@dataclass
class SelectStatement:
    # ... existing fields
    group_by: Optional[List[str]]
    order_by: Optional[List[OrderByColumn]]

@dataclass
class OrderByColumn:
    column: str
    direction: str  # 'ASC' or 'DESC'
```

**Parse:**
- `GROUP BY column1, column2`
- `ORDER BY column ASC/DESC`
- Aggregation functions: `COUNT(*)`, `SUM(col)`, `AVG(col)`, `MIN(col)`, `MAX(col)`

#### 4.2 Aggregate Operator

**File:** `sqlstream/operators/aggregate.py`

**Incremental Aggregation:**
```python
class Aggregate(Operator):
    def __init__(self, child: Operator, group_by: List[str], agg_funcs: List[AggFunc]):
        super().__init__(child)
        self.group_by = group_by
        self.agg_funcs = agg_funcs

    def __iter__(self):
        # Streaming aggregation with hash table
        groups = {}  # key: group_key, value: aggregate state

        for row in self.child:
            key = self._make_group_key(row)

            if key not in groups:
                groups[key] = self._init_agg_state()

            # Update aggregates incrementally
            for agg in self.agg_funcs:
                groups[key][agg.name] = agg.update(
                    groups[key][agg.name],
                    row[agg.column]
                )

        # Yield final results
        for key, agg_state in groups.items():
            yield self._make_result_row(key, agg_state)
```

**Aggregation Functions:**
```python
class AggFunc:
    def update(self, current_state, new_value):
        raise NotImplementedError

    def finalize(self, state):
        return state

class Count(AggFunc):
    def update(self, current, value):
        return current + 1

class Sum(AggFunc):
    def update(self, current, value):
        return current + value

class Avg(AggFunc):
    def update(self, state, value):
        # state = (sum, count)
        return (state[0] + value, state[1] + 1)

    def finalize(self, state):
        return state[0] / state[1] if state[1] > 0 else None
```

**Tests:** `tests/test_operators/test_aggregate.py`
- GROUP BY single column
- GROUP BY multiple columns
- All aggregation functions
- Empty groups
- Memory usage (should be O(unique groups))

#### 4.3 Sort Operator

**File:** `sqlstream/operators/sort.py`

**Implementation:**
```python
class Sort(Operator):
    def __init__(self, child: Operator, order_by: List[OrderByColumn]):
        super().__init__(child)
        self.order_by = order_by

    def __iter__(self):
        # Must materialize for sorting
        rows = list(self.child)

        # Sort in-place
        rows.sort(key=self._make_sort_key)

        yield from rows

    def _make_sort_key(self, row):
        # Multi-column sort key
        return tuple(
            row[col.column] * (-1 if col.direction == 'DESC' else 1)
            for col in self.order_by
        )
```

**Future enhancement (Phase 5):** External sort for large datasets

**Tests:** `tests/test_operators/test_sort.py`
- Single column sort
- Multi-column sort
- ASC/DESC
- NULL handling

**Phase 4 Deliverable:**
```python
# This should work:
query("sales.parquet").sql("""
    SELECT category, SUM(revenue) as total
    FROM sales
    WHERE date >= '2024-01-01'
    GROUP BY category
    ORDER BY total DESC
    LIMIT 10
""")
```

**Dependencies Added:** None

---

### Phase 5: JOIN Support

**Goal:** Implement hash join with bloom filter optimization

#### 5.1 Extend SQL Parser

**Parse:** `FROM table1 JOIN table2 ON table1.id = table2.id`

**AST:**
```python
@dataclass
class SelectStatement:
    # ... existing
    join: Optional[JoinClause]

@dataclass
class JoinClause:
    right_source: str
    join_type: str  # 'INNER', 'LEFT', 'RIGHT'
    on_left: str
    on_right: str
```

#### 5.2 Hash Join Operator

**File:** `sqlstream/operators/join.py`

**Implementation:**
```python
class HashJoin(Operator):
    def __init__(self, left: Operator, right: Operator,
                 left_key: str, right_key: str):
        self.left = left
        self.right = right
        self.left_key = left_key
        self.right_key = right_key

    def __iter__(self):
        # Phase 1: Build hash table from smaller side
        # (Assume right is smaller for now; add size estimation later)
        hash_table = self._build_hash_table(self.right, self.right_key)

        # Phase 2: Probe with left side
        for left_row in self.left:
            key = left_row[self.left_key]

            if key in hash_table:
                for right_row in hash_table[key]:
                    # Merge rows
                    yield {**left_row, **right_row}

    def _build_hash_table(self, operator, key_column):
        table = defaultdict(list)
        for row in operator:
            key = row[key_column]
            table[key].append(row)
        return table
```

#### 5.3 Bloom Filter Optimization

**File:** `sqlstream/optimizers/bloom_filter.py`

**When to use:**
- Large left table
- Small right table
- Low join selectivity

**Implementation:**
```python
class BloomFilter:
    def __init__(self, size=10000, hash_count=3):
        self.bits = bitarray(size)
        self.size = size
        self.hash_count = hash_count

    def add(self, item):
        for i in range(self.hash_count):
            idx = hash(f"{item}:{i}") % self.size
            self.bits[idx] = 1

    def contains(self, item):
        for i in range(self.hash_count):
            idx = hash(f"{item}:{i}") % self.size
            if not self.bits[idx]:
                return False
        return True  # Maybe (false positive possible)

class BloomJoin(HashJoin):
    def __iter__(self):
        # Build bloom filter from right side
        bloom = BloomFilter()
        hash_table = defaultdict(list)

        for row in self.right:
            key = row[self.right_key]
            bloom.add(key)
            hash_table[key].append(row)

        # Probe with bloom filter pre-check
        for left_row in self.left:
            key = left_row[self.left_key]

            # Fast rejection with bloom filter
            if not bloom.contains(key):
                continue

            # Actual lookup
            if key in hash_table:
                for right_row in hash_table[key]:
                    yield {**left_row, **right_row}
```

**Tests:** `tests/test_operators/test_join.py`
- Inner join
- Left join
- Multiple matching rows
- No matches
- Performance with bloom filter

**Phase 5 Deliverable:**
```python
# This should work:
query("users.csv").sql("""
    SELECT u.name, o.total
    FROM users u
    JOIN orders o ON u.id = o.user_id
    WHERE o.total > 100
""")
```

**Dependencies Added:** `bitarray` (for bloom filter) - add to requirements.txt

---

### Phase 6: HTTP Streaming + Partition Pruning

**Goal:** Support remote Parquet files and partitioned datasets

#### 6.1 HTTP Reader

**File:** `sqlstream/readers/http_reader.py`

**Implementation:**
```python
import requests

class HTTPReader(BaseReader):
    def __init__(self, url: str):
        self.url = url
        self.reader = self._create_underlying_reader()

    def _create_underlying_reader(self):
        # Download to temp file (simple approach)
        # OR use streaming for CSV/JSON
        # OR use range requests for Parquet

        response = requests.head(self.url)

        if 'Accept-Ranges' in response.headers:
            # Supports range requests
            if self.url.endswith('.parquet'):
                return HTTPParquetReader(self.url)

        # Fallback: stream download
        return self._streaming_reader()
```

#### 6.2 HTTP Parquet with Range Requests

**File:** `sqlstream/readers/http_reader.py` (continued)

**Range Request Strategy:**
```python
class HTTPParquetReader(ParquetReader):
    def __init__(self, url: str):
        self.url = url
        self.footer = self._read_footer()

    def _read_footer(self):
        # Step 1: Get last 8 bytes (footer size)
        response = requests.get(self.url, headers={'Range': 'bytes=-8'})
        footer_size = int.from_bytes(response.content[-4:], 'little')

        # Step 2: Get footer metadata
        response = requests.get(
            self.url,
            headers={'Range': f'bytes=-{footer_size + 8}--8'}
        )

        return parse_parquet_footer(response.content)

    def _read_row_group(self, rg_idx: int):
        # Get row group offset and size from footer
        offset = self.footer.row_groups[rg_idx].offset
        size = self.footer.row_groups[rg_idx].total_byte_size

        # Request only this row group
        response = requests.get(
            self.url,
            headers={'Range': f'bytes={offset}-{offset + size}'}
        )

        return parse_row_group(response.content)
```

**Tests:** `tests/test_readers/test_http_reader.py`
- Download remote CSV
- Range requests for Parquet
- Measure bytes transferred
- Handle no range support

#### 6.3 Partition Discovery

**File:** `sqlstream/utils/partitions.py`

**Hive-style partition parsing:**
```python
def discover_partitions(path: str) -> List[Partition]:
    """
    Parse: /data/year=2024/month=01/data.parquet
    Returns: [Partition(year=2024, month=01, file=...)]
    """
    partitions = []

    for root, dirs, files in os.walk(path):
        # Parse partition values from directory names
        partition_values = parse_partition_path(root)

        for file in files:
            if file.endswith('.parquet'):
                partitions.append(Partition(
                    path=os.path.join(root, file),
                    partition_values=partition_values
                ))

    return partitions

def parse_partition_path(path: str) -> Dict[str, Any]:
    """Parse 'year=2024/month=01' into {'year': 2024, 'month': 1}"""
    parts = {}
    for segment in path.split('/'):
        if '=' in segment:
            key, value = segment.split('=')
            parts[key] = infer_type(value)
    return parts
```

#### 6.4 Partition Pruning

**File:** `sqlstream/optimizers/partition_pruning.py`

**Strategy:**
```python
class PartitionPruner:
    def prune(self, partitions: List[Partition],
              conditions: List[Condition]) -> List[Partition]:
        """Filter partitions based on WHERE conditions"""

        selected = []
        for partition in partitions:
            if self._matches_conditions(partition, conditions):
                selected.append(partition)

        return selected

    def _matches_conditions(self, partition, conditions):
        for condition in conditions:
            if condition.column in partition.partition_values:
                # This is a partition column
                value = partition.partition_values[condition.column]
                if not evaluate_condition(value, condition):
                    return False  # Skip this partition

        return True
```

**Tests:** `tests/test_optimizers/test_partition_pruning.py`
- Discover partitions
- Prune by partition predicates
- Multi-level partitions

**Phase 6 Deliverable:**
```python
# This should work:
query("https://example.com/data.parquet").sql("SELECT * WHERE age > 30")

# And this (partitioned dataset):
query("s3://bucket/data/year=2024/*/").sql("""
    SELECT * FROM data WHERE year = 2024 AND month = 1
""")
```

**Dependencies Added:** `requests`

---

### Phase 7: CLI + Output Formatting

**Goal:** Complete command-line interface with multiple output formats

#### 7.1 Add Click Framework

**Update:** `requirements.txt`
```
click>=8.0.0
```

#### 7.2 CLI Implementation

**File:** `sqlstream/cli/main.py`

**Implementation:**
```python
import click
from sqlstream import query

@click.group()
def cli():
    """SQLStream - SQL queries on local and remote files"""
    pass

@cli.command()
@click.argument('source')
@click.argument('sql_query')
@click.option('--format', default='json', type=click.Choice(['json', 'csv', 'table']))
@click.option('--output', default='-', type=click.Path())
@click.option('--explain', is_flag=True, help='Show query plan')
@click.option('--verbose', is_flag=True, help='Verbose output')
def query_cmd(source, sql_query, format, output, explain, verbose):
    """Execute SQL query on data source"""

    result = query(source).sql(sql_query)

    if explain:
        click.echo(result.explain_plan())
        return

    # Format and output results
    formatter = get_formatter(format)

    if output == '-':
        # Output to stdout
        for row in result:
            click.echo(formatter.format_row(row))
    else:
        # Write to file
        with open(output, 'w') as f:
            for row in result:
                f.write(formatter.format_row(row) + '\n')

@cli.command()
@click.argument('source')
def schema(source):
    """Show schema of data source"""
    reader = create_reader(source)
    schema = infer_schema(reader)
    click.echo(format_schema(schema))

@cli.command()
@click.argument('source')
def profile(source):
    """Profile data source (row count, column types, statistics)"""
    # Implement data profiling
    pass
```

#### 7.3 Output Formatters

**File:** `sqlstream/cli/formatters.py`

**Implementations:**
```python
class JSONFormatter:
    def format_row(self, row: Dict) -> str:
        return json.dumps(row)

class CSVFormatter:
    def __init__(self):
        self.headers_written = False
        self.headers = None

    def format_row(self, row: Dict) -> str:
        if not self.headers_written:
            self.headers = list(row.keys())
            # Return header + row
            return ','.join(self.headers) + '\n' + ','.join(str(row[k]) for k in self.headers)
        return ','.join(str(row[k]) for k in self.headers)

class TableFormatter:
    """Pretty table output using tabulate or custom implementation"""
    def format_rows(self, rows: List[Dict]) -> str:
        # Use tabulate library or implement simple table
        pass
```

**Tests:** `tests/test_cli/test_formatters.py`
- JSON output
- CSV output
- Table output

#### 7.4 Entry Point

**File:** `sqlstream/__main__.py`
```python
from sqlstream.cli.main import cli

if __name__ == '__main__':
    cli()
```

**Update:** `setup.py`
```python
setup(
    name='sqlstream',
    entry_points={
        'console_scripts': [
            'sqlstream=sqlstream.cli.main:cli',
        ],
    },
)
```

**Phase 7 Deliverable:**
```bash
# These should work:
sqlstream query data.csv "SELECT * WHERE age > 25" --format table
sqlstream schema data.parquet
sqlstream query https://example.com/data.parquet "SELECT COUNT(*) FROM data"
```

**Dependencies Added:** `click`, `tabulate` (optional, for table formatting)

---

### Phase 8: Type System + Schema Inference

**Goal:** Robust type inference and schema evolution

#### 8.1 Type System

**File:** `sqlstream/core/types.py`

**Type definitions:**
```python
class DataType(Enum):
    INTEGER = 'int64'
    FLOAT = 'float64'
    STRING = 'string'
    BOOLEAN = 'bool'
    DATE = 'date'
    DATETIME = 'datetime'
    NULL = 'null'

class TypeInferrer:
    def infer(self, value: Any) -> DataType:
        if value is None:
            return DataType.NULL

        if isinstance(value, bool):
            return DataType.BOOLEAN

        if isinstance(value, int):
            return DataType.INTEGER

        if isinstance(value, float):
            return DataType.FLOAT

        if isinstance(value, str):
            # Pattern matching
            if self._is_date(value):
                return DataType.DATE
            if self._is_datetime(value):
                return DataType.DATETIME
            return DataType.STRING

        return DataType.STRING  # Fallback

    def _is_date(self, value: str) -> bool:
        # Try parsing as date
        patterns = [
            r'\d{4}-\d{2}-\d{2}',
            r'\d{2}/\d{2}/\d{4}',
        ]
        return any(re.match(p, value) for p in patterns)
```

#### 8.2 Schema Inference

**File:** `sqlstream/utils/schema.py`

**Implementation:**
```python
class Schema:
    def __init__(self):
        self.columns: Dict[str, DataType] = {}

    def infer_from_sample(self, rows: Iterator[Dict], sample_size=1000):
        """Infer schema from first N rows"""
        type_inferrer = TypeInferrer()
        column_types = defaultdict(Counter)

        for i, row in enumerate(rows):
            if i >= sample_size:
                break

            for column, value in row.items():
                dtype = type_inferrer.infer(value)
                column_types[column][dtype] += 1

        # Determine final type for each column
        for column, type_counts in column_types.items():
            self.columns[column] = self._resolve_type(type_counts)

    def _resolve_type(self, type_counts: Counter) -> DataType:
        # Type promotion rules
        # If mixed int/float -> float
        # If mixed date/string -> date (if >90% are dates)
        # If mixed anything else -> string

        if DataType.FLOAT in type_counts:
            return DataType.FLOAT
        if DataType.INTEGER in type_counts:
            return DataType.INTEGER
        # ... more rules
        return DataType.STRING
```

**Tests:** `tests/test_utils/test_schema.py`
- Infer from homogeneous data
- Handle mixed types
- Type promotion
- Null handling

**Phase 8 Deliverable:**
- Automatic type inference for all readers
- Type-aware filtering (numeric comparisons work correctly)
- Schema evolution handling

**Dependencies Added:** None

---

### Phase 9: Error Handling + User Feedback

**Goal:** Clear error messages and query plan explanation

#### 9.1 Custom Exceptions

**File:** `sqlstream/core/exceptions.py`

**Exception hierarchy:**
```python
class SQLStreamError(Exception):
    """Base exception"""
    pass

class ParseError(SQLStreamError):
    """SQL parsing failed"""
    def __init__(self, message, position, query):
        super().__init__(f"{message} at position {position}\nQuery: {query}")

class FileNotFoundError(SQLStreamError):
    """Data source not found"""
    def __init__(self, path):
        suggestions = self._find_similar_files(path)
        msg = f"File not found: {path}"
        if suggestions:
            msg += f"\nDid you mean: {', '.join(suggestions)}?"
        super().__init__(msg)

class TypeMismatchError(SQLStreamError):
    """Type incompatibility"""
    def __init__(self, column, expected, actual):
        super().__init__(
            f"Type mismatch for column '{column}': "
            f"expected {expected}, got {actual}"
        )
```

#### 9.2 Query Plan Explanation

**File:** `sqlstream/core/planner.py` (enhancement)

**Add explain functionality:**
```python
class ExecutionPlan:
    def __init__(self):
        self.steps = []
        self.optimizations = []

    def explain(self) -> str:
        output = ["Query Execution Plan:", ""]

        for i, step in enumerate(self.steps, 1):
            output.append(f"{i}. {step.describe()}")
            if step.metadata:
                for key, value in step.metadata.items():
                    output.append(f"   - {key}: {value}")

        output.append("\nOptimizations Applied:")
        for opt in self.optimizations:
            output.append(f"  ✓ {opt}")

        return '\n'.join(output)
```

**Example output:**
```
Query Execution Plan:

1. Scan: data.parquet
   - Row groups: 10 total, 2 selected (8 pruned)
   - Columns: 15 total, 3 selected (12 pruned)
   - Estimated rows: 2000

2. Filter: WHERE age > 25
   - Predicate pushed to reader

3. Project: SELECT name, age
   - Column pruning applied

4. Limit: 10
   - Early termination enabled

Optimizations Applied:
  ✓ Predicate pushdown (reduced row groups from 10 to 2)
  ✓ Column pruning (reading 3 of 15 columns)
  ✓ Statistics-based row group selection
```

**Tests:** `tests/test_core/test_error_handling.py`
- Parse errors with helpful messages
- File not found suggestions
- Type mismatch detection

**Phase 9 Deliverable:**
- User-friendly error messages
- `--explain` flag shows optimization plan
- Helpful suggestions in error messages

---

### Phase 10: Testing + Documentation

**Goal:** Comprehensive test coverage and documentation

#### 10.1 Integration Tests

**File:** `tests/integration/test_end_to_end.py`

**Test scenarios:**
```python
def test_csv_complete_query():
    """Full query with all operations"""
    result = query("test_data.csv").sql("""
        SELECT category, AVG(price) as avg_price
        FROM data
        WHERE price > 10
        GROUP BY category
        ORDER BY avg_price DESC
        LIMIT 5
    """)
    assert len(list(result)) == 5

def test_parquet_optimization():
    """Verify optimizations work"""
    result = query("large.parquet").sql(
        "SELECT * WHERE age > 60"
    )

    # Check that row groups were pruned
    stats = result.get_statistics()
    assert stats['row_groups_scanned'] < stats['row_groups_total']

def test_remote_parquet():
    """HTTP streaming"""
    result = query("https://example.com/data.parquet").sql(
        "SELECT * LIMIT 10"
    )
    assert len(list(result)) == 10
```

#### 10.2 Performance Tests

**File:** `tests/performance/test_benchmarks.py`

**Benchmark scenarios:**
```python
def test_1m_row_csv_performance():
    """Query 1M rows CSV in < 5 seconds"""
    start = time.time()
    result = query("1m_rows.csv").sql(
        "SELECT * WHERE value > 500000"
    )
    list(result)
    elapsed = time.time() - start

    assert elapsed < 5.0

def test_memory_usage():
    """Memory < 100MB for 1GB Parquet"""
    import tracemalloc
    tracemalloc.start()

    result = query("1gb.parquet").sql("SELECT * WHERE condition")
    list(result)

    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert peak < 100 * 1024 * 1024  # 100MB
```

#### 10.3 Documentation

**README.md:**
- Quick start guide
- Installation instructions
- Basic examples
- Link to full documentation

**TUTORIAL.md:**
- Step-by-step examples
- Common use cases
- Best practices

**API.md:**
- Full API reference
- All parameters documented

**Contributing guide:**
- Code structure overview
- How to add new features
- Testing guidelines

**Phase 10 Deliverable:**
- > 80% test coverage
- Complete documentation
- Performance benchmarks passing
- Ready for release

---

## Development Workflow

### For Each Phase:

1. **Implement** core functionality
2. **Write tests** alongside code
3. **Run tests** continuously
4. **Verify** integration with previous phases
5. **Document** new features
6. **Commit** working code

### Testing Commands:

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_core/test_query.py

# Run with coverage
pytest --cov=sqlstream --cov-report=html

# Performance tests
pytest tests/performance/ -v
```

### Code Quality:

```bash
# Format code
black sqlstream/

# Type checking
mypy sqlstream/

# Linting
pylint sqlstream/
```

---

## Dependency Timeline

**Phase 0-2:** Pure Python (no dependencies)
**Phase 3:** Add `pyarrow` (Parquet support)
**Phase 5:** Add `bitarray` (Bloom filter)
**Phase 6:** Add `requests` (HTTP)
**Phase 7:** Add `click`, `tabulate` (CLI)

**Final requirements.txt:**
```
pyarrow>=10.0.0
requests>=2.28.0
click>=8.0.0
bitarray>=2.6.0
tabulate>=0.9.0  # optional
```

**requirements-dev.txt:**
```
pytest>=7.0.0
pytest-cov>=4.0.0
black>=22.0.0
mypy>=0.990
pylint>=2.15.0
```

---

## Critical Implementation Details

### 1. Lazy Evaluation Pattern

All readers and operators must use generators:
```python
# GOOD - Lazy
def read_lazy(self):
    for row in self.source:
        yield process(row)

# BAD - Eager
def read_eager(self):
    return [process(row) for row in self.source]
```

### 2. Zero-Copy Projections

Don't copy dicts for SELECT:
```python
# GOOD - View
class ProjectedDict:
    def __init__(self, source, columns):
        self._source = source
        self._columns = columns

    def __getitem__(self, key):
        if key in self._columns:
            return self._source[key]
        raise KeyError(key)

# BAD - Copy
def project(row, columns):
    return {k: row[k] for k in columns}
```

### 3. Statistics-Driven Decisions

Always check statistics before reading data:
```python
# Read Parquet metadata first
metadata = parquet_file.metadata

# Use stats to skip row groups
for rg in range(metadata.num_row_groups):
    if should_skip_based_on_stats(rg, filters):
        continue
    read_row_group(rg)
```

### 4. Error Recovery

Handle malformed data gracefully:
```python
try:
    value = parse_value(raw)
except ValueError:
    warnings.warn(f"Malformed value: {raw}, using NULL")
    value = None
```

---

## Potential Challenges & Mitigations

### Challenge 1: SQL Parser Complexity

**Risk:** Hand-written parser becomes unwieldy

**Mitigation:**
- Start with minimal SQL subset
- Add features incrementally
- Consider using `sqlparse` library if complexity grows
- Focus on common use cases (90% rule)

### Challenge 2: Memory Management

**Risk:** Large datasets cause OOM

**Mitigation:**
- Test with large files early
- Use generators everywhere
- Monitor memory in tests
- Implement external sort if needed (Phase 11)

### Challenge 3: HTTP Range Request Support

**Risk:** Not all servers support ranges

**Mitigation:**
- Check `Accept-Ranges` header
- Fallback to streaming download
- Cache locally for repeated queries
- Clear error message when unsupported

### Challenge 4: Type Inference Ambiguity

**Risk:** Wrong type inferred (e.g., "123" as string vs number)

**Mitigation:**
- Consistent heuristics documented
- Allow user schema override
- Sample multiple rows (not just first)
- Provide `--schema` flag for explicit types

### Challenge 5: Performance vs Correctness

**Risk:** Optimizations introduce bugs

**Mitigation:**
- Test correctness first, optimize second
- Compare optimized vs naive results
- Property-based testing (same result with/without optimization)
- Thorough integration tests

---

## Success Criteria

### Technical Metrics:
- ✓ Package size < 250KB (check with: `pip install --no-deps .`)
- ✓ Query 1M rows CSV in < 5s
- ✓ Memory < 100MB for 1GB Parquet
- ✓ Row group pruning reduces I/O by > 50% on selective queries
- ✓ HTTP range requests download < 10% of file for selective queries

### User Experience:
- ✓ Install to first query < 2 minutes
- ✓ `--help` explains all features clearly
- ✓ Error messages actionable (user knows what to fix)
- ✓ Works on Python 3.8-3.12 without issues

### Code Quality:
- ✓ Test coverage > 80%
- ✓ All tests passing
- ✓ No linter errors
- ✓ Type hints on public APIs
- ✓ Documentation complete

---

## Post-MVP Enhancements (Phase 11+)

These are NOT part of the initial implementation but good to keep in mind:

### Phase 11: Advanced Features
- External sorting (for ORDER BY on huge datasets)
- Approximate algorithms (HyperLogLog, Count-Min Sketch)
- Query result caching
- Parallel execution

### Phase 12: Extended Format Support
- JSON Lines (JSONL)
- Apache Avro
- Apache ORC
- Excel files

### Phase 13: Database Sources
- PostgreSQL reader
- MySQL reader
- SQLite reader
- DuckDB integration

### Phase 14: Write Operations
- `.to_parquet()`
- `.to_csv()`
- Partitioned writes
- Compression options

---

## Implementation Start Checklist

Before starting Phase 0:

- [ ] Understand the technical design document
- [ ] Python 3.8+ installed
- [ ] Git installed
- [ ] Editor/IDE configured
- [ ] Clear on MVP scope (Phases 0-10)
- [ ] Questions resolved

Ready to build! 🚀
