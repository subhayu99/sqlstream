# URL Fragment Syntax

SQLStream supports a powerful URL fragment syntax that allows you to specify the data format and table index directly in the source URL. This is particularly useful for URLs that don't have file extensions (like Pastebin) or files that contain multiple tables (like HTML or Markdown).

## Syntax

The general syntax is:

```
source_url#format:table
```

- **source_url**: The path to the file or HTTP URL.
- **format**: (Optional) The data format (e.g., `csv`, `json`, `parquet`, `html`, `markdown`).
- **table**: (Optional) The index of the table to read (0-indexed).

### Components

| Component | Description | Example |
|-----------|-------------|---------|
| `format` | Explicitly sets the parser to use. Overrides file extension detection. | `#csv`, `#html` |
| `table` | Selects a specific table from a multi-table file. | `:0`, `:1`, `:-1` |
| Separator | The `:` character separates format and table. | `#html:1` |

## Examples

### Specifying Format Only

Use this when the URL doesn't have a file extension or you want to override the detected format.

```sql
-- Read raw text from Pastebin as CSV
SELECT * FROM "https://pastebin.com/raw/xxxxx#csv";

-- Force CSV parsing for a .txt file
SELECT * FROM "data.txt#csv";
```

### Specifying Table Only

Use this when the format is correctly detected (e.g., by extension) but you want to read a specific table.

```sql
-- Read the second table from an HTML file (index 1)
SELECT * FROM "data.html#:1";

-- Read the last table from a Markdown file
SELECT * FROM "README.md#:-1";
```

### Specifying Format and Table

Use this for full control, especially for extension-less URLs containing multi-table formats.

```sql
-- Read the first table from a raw HTML URL
SELECT * FROM "https://example.com/raw/data#html:0";

-- Read the second table from a raw Markdown URL
SELECT * FROM "https://pastebin.com/raw/cnkgQp1t#markdown:1";
```

## Supported Formats

The following formats support the fragment syntax:

- **html**: Supports table selection.
- **markdown**: Supports table selection.
- **csv**: Format specification only (table index ignored).
- **parquet**: Format specification only.
- **json**: Format specification only.

## Table Indexing

- **Positive Index**: `0` is the first table, `1` is the second, etc.
- **Negative Index**: `-1` is the last table, `-2` is the second to last, etc.

## Error Handling

- If the specified format is invalid, an error will be raised.
- If the table index is out of range (e.g., requesting table 5 when only 2 exist), a `ValueError` will be raised with a helpful message indicating the number of available tables.
