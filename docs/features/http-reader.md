# HTTP Reader

The HTTP Reader allows SQLStream to query data directly from HTTP and HTTPS URLs. It supports intelligent caching, streaming downloads, and automatic format detection.

## Features

- **Direct Querying**: Query data from any public URL.
- **Caching**: Downloads are cached locally to speed up subsequent queries.
- **Format Detection**: Automatically detects format from file extension, URL fragments, or content.
- **Streaming**: Streams large files efficiently.

## Usage

### Basic Usage

Simply use a URL as the table source in your SQL query:

```sql
SELECT * FROM "https://raw.githubusercontent.com/datasets/population/master/data/population.csv";
```

### Handling Extension-less URLs

For URLs that don't end in a standard file extension (like Pastebin or API endpoints), use the [URL Fragment Syntax](url-fragments.md) to specify the format:

```sql
-- Read CSV data from Pastebin
SELECT * FROM "https://pastebin.com/raw/xxxxx#csv";

-- Read Markdown table from a raw URL
SELECT * FROM "https://pastebin.com/raw/cnkgQp1t#markdown";
```

### Caching

Downloaded files are cached in the system's temporary directory (e.g., `/tmp/sqlstream_cache` on Linux). The cache key is based on the URL hash.

- **Persistent Cache**: Files remain in cache until cleared.
- **Force Download**: Currently, the CLI doesn't expose a flag to force download, but you can manually clear the cache directory.

### Supported Formats

The HTTP Reader supports all formats supported by SQLStream, including:
- CSV
- Parquet
- JSON
- HTML (tables)
- Markdown (tables)

## Advanced Usage

### Reading Specific Tables

You can combine format specification with table selection for multi-table formats like HTML and Markdown:

```sql
-- Read the second table from a remote Markdown file
SELECT * FROM "https://example.com/data.md#markdown:1";
```

### Content-Type Detection

The reader attempts to detect the format from the file content if the extension is missing or unknown. It checks for:
- **Parquet**: Magic bytes `PAR1`.
- **HTML**: Tags like `<html>`, `<table>`.
- **Markdown**: Table syntax `| ... |` and `|---`.
