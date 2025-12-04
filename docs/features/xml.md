# XML File Support

SQLStream can extract and query structured data from XML files, making it easy to work with XML data using SQL.

---

## Installation

```bash
# XML support is included in the "all" package
pip install "sqlstream[all]"

# Or install just XML dependencies
pip install sqlstream lxml
```

---

## Basic Usage

### Simple XML Structure

Given an XML file `users.xml`:

```xml
<?xml version="1.0"?>
<users>
    <user id="1">
        <name>Alice</name>
        <email>alice@example.com</email>
        <age>30</age>
    </user>
    <user id="2">
        <name>Bob</name>
        <email>bob@example.com</email>
        <age>25</age>
    </user>
</users>
```

Query it with SQLStream:

=== "Python"

    ```python
    from sqlstream import query

    # Auto-detect repeating elements
    results = query("users.xml").sql("SELECT * FROM users")

    for row in results:
        print(f"{row['name']}: {row['email']}")
    ```

=== "CLI"

    ```bash
    $ sqlstream query users.xml "SELECT name, email FROM users WHERE age > 25"
    ```

=== "Output"

    ```
    ┌────────┬────────────────────┐
    │ name   │ email              │
    ├────────┼────────────────────┤
    │ Alice  │ alice@example.com  │
    └────────┴────────────────────┘
    ```

---

## Element Path Syntax

Use the `#xml:element_path` syntax to specify which XML elements to query:

```python
# Explicit element path
results = query("data.xml#xml:user").sql("SELECT * FROM data")

# Nested path (dot notation)
results = query("data.xml#xml:root.users.user").sql("SELECT * FROM data")
```

### Path Examples

Given this XML structure:

```xml
<root>
    <company>
        <department name="Engineering">
            <employee id="1">
                <name>Alice</name>
                <title>Engineer</title>
            </employee>
        </department>
    </company>
</root>
```

Query paths:

```python
# Query all employees
query("org.xml#xml:employee")

# Full path
query("org.xml#xml:root.company.department.employee")
```

---

## Attributes

XML attributes are accessible with the `@` prefix:

### Example

```xml
<products>
    <product id="101" status="active">
        <name>Widget</name>
        <price>29.99</price>
    </product>
    <product id="102" status="inactive">
        <name>Gadget</name>
        <price>49.99</price>
    </product>
</products>
```

Query attributes:

```python
from sqlstream import query

results = query("products.xml#xml:product").sql("""
    SELECT @id as product_id, @status, name, price
    FROM products
    WHERE @status = 'active'
""")
```

Output:
```python
{'product_id': '101', 'status': 'active', 'name': 'Widget', 'price': '29.99'}
```

---

## Nested Elements

Access nested elements using dot notation in column names:

### Example

```xml
<employees>
    <employee>
        <name>Alice</name>
        <contact>
            <email>alice@example.com</email>
            <phone>555-1234</phone>
        </contact>
        <address>
            <city>New York</city>
            <state>NY</state>
        </address>
    </employee>
</employees>
```

Query nested data:

```python
results = query("employees.xml#xml:employee").sql("""
    SELECT
        name,
        contact.email as email,
        contact.phone as phone,
        address.city as city,
        address.state as state
    FROM employees
""")
```

---

## Complex Structures

### Mixed Content

When XML has both attributes and nested elements:

```xml
<books>
    <book isbn="978-0-123456-78-9" year="2024">
        <title>SQL for Data</title>
        <author>
            <name>John Doe</name>
            <email>john@example.com</email>
        </author>
        <price currency="USD">39.99</price>
    </book>
</books>
```

Query:

```python
results = query("books.xml#xml:book").sql("""
    SELECT
        @isbn as isbn,
        @year as year,
        title,
        author.name as author_name,
        author.email as author_email,
        price as price,
        price.@currency as currency
    FROM books
""")
```

### Multiple Record Types

When XML has different element types:

```xml
<data>
    <users>
        <user id="1">
            <name>Alice</name>
        </user>
        <user id="2">
            <name>Bob</name>
        </user>
    </users>
    <orders>
        <order id="101">
            <user_id>1</user_id>
            <amount>100.00</amount>
        </order>
    </orders>
</data>
```

Query each type separately:

```python
# Query users
users = query("data.xml#xml:users.user").sql("SELECT @id, name FROM users")

# Query orders
orders = query("data.xml#xml:orders.order").sql("SELECT @id, user_id, amount FROM orders")

# JOIN them
results = query().sql("""
    SELECT u.name, o.amount
    FROM 'data.xml#xml:users.user' u
    JOIN 'data.xml#xml:orders.order' o ON u.@id = o.user_id
""", backend="duckdb")
```

---

## Namespaces

SQLStream handles XML namespaces automatically:

```xml
<root xmlns:ns="http://example.com/ns">
    <ns:record>
        <ns:name>Alice</ns:name>
        <ns:value>100</ns:value>
    </ns:record>
</root>
```

The namespace prefix is stripped in column names:

```python
results = query("data.xml#xml:record").sql("SELECT name, value FROM data")
# No need to specify ns:name or ns:value
```

---

## Type Inference

SQLStream automatically infers data types from XML:

```xml
<data>
    <record>
        <id>1</id>              <!-- INTEGER -->
        <price>29.99</price>    <!-- FLOAT -->
        <active>true</active>   <!-- BOOLEAN -->
        <name>Product</name>    <!-- STRING -->
    </record>
</data>
```

```python
results = query("data.xml").sql("SELECT * FROM data WHERE id > 0 AND price < 50.00")
# Types are automatically inferred
```

---

## Performance Considerations

### Large XML Files

For large XML files:

1. **Use DuckDB backend** for better performance:
   ```python
   results = query("large.xml#xml:record", backend="duckdb").sql("""
       SELECT * FROM large WHERE condition
   """)
   ```

2. **Filter early** with WHERE clauses:
   ```python
   # Good - filters during parsing
   results = query("large.xml").sql("SELECT * FROM large WHERE date > '2024-01-01'")

   # Less efficient - filters after loading
   results = query("large.xml").sql("SELECT * FROM large")
   filtered = [r for r in results if r['date'] > '2024-01-01']
   ```

3. **Use LIMIT** for testing:
   ```python
   # Test query first
   results = query("large.xml").sql("SELECT * FROM large LIMIT 10")
   ```

### Memory Usage

XML files are parsed into memory before querying. For very large files:

- Consider splitting into smaller files
- Use streaming XML parsers (not currently supported)
- Use a database designed for XML (BaseX, eXist-db)

---

## Common Patterns

### Configuration Files

Parse XML configuration files:

```xml
<config>
    <database host="localhost" port="5432">
        <name>mydb</name>
        <user>admin</user>
    </database>
    <features>
        <feature name="caching" enabled="true"/>
        <feature name="logging" enabled="false"/>
    </features>
</config>
```

```python
# Get database config
db_config = query("config.xml#xml:database").sql("SELECT @host, @port, name, user FROM database").to_list()[0]

# Get enabled features
features = query("config.xml#xml:feature").sql("""
    SELECT @name as feature
    FROM features
    WHERE @enabled = 'true'
""")
```

### API Responses

Query XML API responses:

```python
import requests
from sqlstream import query

# Fetch XML from API
response = requests.get("https://api.example.com/data.xml")
with open("temp.xml", "w") as f:
    f.write(response.text)

# Query it
results = query("temp.xml#xml:item").sql("SELECT * FROM items WHERE category = 'electronics'")
```

### RSS/Atom Feeds

Parse RSS feeds:

```xml
<rss version="2.0">
    <channel>
        <item>
            <title>Article 1</title>
            <link>https://example.com/1</link>
            <pubDate>2024-01-15</pubDate>
        </item>
    </channel>
</rss>
```

```python
results = query("feed.xml#xml:item").sql("""
    SELECT title, link, pubDate
    FROM items
    ORDER BY pubDate DESC
    LIMIT 10
""")
```

---

## Limitations

1. **No XPath**: SQLStream uses element paths, not full XPath expressions
2. **No CDATA extraction**: CDATA sections are treated as text
3. **In-memory parsing**: Entire file is loaded into memory
4. **No schema validation**: No DTD or XSD validation
5. **No modification**: Read-only, cannot modify XML

For advanced XML processing, consider:
- **lxml** for XPath and complex queries
- **xmltodict** for converting XML to dict/JSON
- **BaseX** or **eXist-db** for XML databases

---

## Examples

See [XML Examples](../examples/xml-data.md) for more real-world use cases.

---

## Troubleshooting

### "No repeating elements found"

SQLStream looks for repeating XML elements to treat as rows. If your XML has only one instance:

```xml
<root>
    <record>  <!-- Only one record -->
        <name>Alice</name>
    </record>
</root>
```

Specify the element explicitly:
```python
query("data.xml#xml:record")
```

### "Column not found"

Check attribute vs element:
- Elements: `name`
- Attributes: `@name`

### "Invalid XML"

Ensure your XML is well-formed:
- Properly closed tags
- Valid encoding declaration
- No unescaped special characters

---

## Next Steps

- [Data Sources Overview](data-sources.md) - All supported formats
- [Type System](type-system.md) - How types are inferred
- [XML Examples](../examples/xml-data.md) - Real-world examples
