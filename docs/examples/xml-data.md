# Querying XML Data

SQLStream can extract and query structured data from XML files using SQL.

**Data Source**: [sample_data.xml](https://github.com/subhayu99/sqlstream/raw/main/examples/demo_data.xml)

---

## Example 1: Basic XML Query

Query a simple XML structure:

```xml
<?xml version="1.0"?>
<employees>
    <employee id="1">
        <name>Alice</name>
        <department>Engineering</department>
        <salary>95000</salary>
    </employee>
    <employee id="2">
        <name>Bob</name>
        <department>Sales</department>
        <salary>75000</salary>
    </employee>
</employees>
```

Query it:

```python
from sqlstream import query

# Auto-detect repeating elements
results = query("employees.xml").sql("""
    SELECT @id as employee_id, name, department, salary
    FROM employees
    WHERE salary > 80000
""")

for row in results:
    print(f"Employee {row['employee_id']}: {row['name']} - ${row['salary']:,}")
```

---

## Example 2: Nested XML Elements

Query nested data using dot notation:

```xml
<company>
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
</company>
```

```python
results = query("company.xml#xml:employee").sql("""
    SELECT
        name,
        contact.email as email,
        address.city as city,
        address.state as state
    FROM company
""")
```

---

## Example 3: XML with Attributes and Elements

```xml
<products>
    <product id="101" status="active">
        <name>Widget</name>
        <price currency="USD">29.99</price>
        <stock>150</stock>
    </product>
    <product id="102" status="inactive">
        <name>Gadget</name>
        <price currency="USD">49.99</price>
        <stock>0</stock>
    </product>
</products>
```

```bash
$ sqlstream query "products.xml#xml:product" "
    SELECT
        @id as product_id,
        @status as status,
        name,
        price,
        stock
    FROM products
    WHERE @status = 'active' AND stock > 0
"
```

---

## Example 4: Querying XML from URLs

```python
from sqlstream import query

# Query XML data from a URL
url = "https://example.com/data.xml"

results = query(f"{url}#xml:item").sql("""
    SELECT * FROM items WHERE category = 'electronics'
""")
```

---

## See Also

- [XML Feature Guide](../features/xml.md) - Complete XML reference
- [URL Fragments](../reference/url-fragments.md) - Path syntax
- [Data Sources](../features/data-sources.md) - All supported formats
