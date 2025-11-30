# Readers API

Readers are responsible for abstracting data access from different file formats and storage systems.

## `BaseReader`

The abstract base class for all readers.

### `read_lazy() -> Iterator[Dict[str, Any]]`
Yields rows one by one from the source.

### `get_schema() -> Optional[Schema]`
Returns the schema of the data source, if available.

## `CSVReader`

Reads Comma-Separated Values files.

- **Source**: Local files, URLs, S3.
- **Features**: Type inference, header detection.

## `ParquetReader`

Reads Apache Parquet files.

- **Source**: Local files, URLs, S3.
- **Features**: Column pruning (reads only requested columns), predicate pushdown (filters data at the storage level).

## `HTTPReader`

Reads data from HTTP/HTTPS URLs. Usually wraps another reader (like CSVReader) to handle the content format.
