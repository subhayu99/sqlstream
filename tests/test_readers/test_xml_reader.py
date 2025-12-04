"""
Test XMLReader - parsing and querying XML files
"""
import tempfile
from pathlib import Path


class TestXMLReaderBasic:
    """Test basic XML parsing and reading"""

    def test_read_simple_xml(self):
        """Test reading a simple XML file with repeating elements"""
        from sqlstream.readers.xml_reader import XMLReader

        xml_content = """<?xml version="1.0"?>
<data>
    <record>
        <name>Alice</name>
        <age>30</age>
        <city>New York</city>
    </record>
    <record>
        <name>Bob</name>
        <age>25</age>
        <city>San Francisco</city>
    </record>
</data>"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            temp_path = f.name
            f.write(xml_content)

        try:
            reader = XMLReader(temp_path, element="record")
            rows = list(reader.read_lazy())

            assert len(rows) == 2
            assert rows[0]['name'] == 'Alice'
            assert rows[0]['age'] == 30
            assert rows[0]['city'] == 'New York'
            assert rows[1]['name'] == 'Bob'
            assert rows[1]['age'] == 25
        finally:
            Path(temp_path).unlink()

    def test_auto_detect_repeating_elements(self):
        """Test auto-detection of repeating elements"""
        from sqlstream.readers.xml_reader import XMLReader

        xml_content = """<?xml version="1.0"?>
<data>
    <item>
        <id>1</id>
        <value>foo</value>
    </item>
    <item>
        <id>2</id>
        <value>bar</value>
    </item>
</data>"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            temp_path = f.name
            f.write(xml_content)

        try:
            # Don't specify element, let it auto-detect
            reader = XMLReader(temp_path)
            rows = list(reader.read_lazy())

            assert len(rows) == 2
            assert rows[0]['id'] == 1
            assert rows[0]['value'] == 'foo'
        finally:
            Path(temp_path).unlink()

    def test_xml_with_attributes(self):
        """Test reading XML with attributes"""
        from sqlstream.readers.xml_reader import XMLReader

        xml_content = """<?xml version="1.0"?>
<data>
    <record id="1" status="active">
        <name>Alice</name>
        <age>30</age>
    </record>
    <record id="2" status="inactive">
        <name>Bob</name>
        <age>25</age>
    </record>
</data>"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            temp_path = f.name
            f.write(xml_content)

        try:
            reader = XMLReader(temp_path, element="record")
            rows = list(reader.read_lazy())

            assert len(rows) == 2
            # Attributes are prefixed with @
            assert rows[0]['@id'] == 1
            assert rows[0]['@status'] == 'active'
            assert rows[0]['name'] == 'Alice'
            assert rows[1]['@id'] == 2
            assert rows[1]['@status'] == 'inactive'
        finally:
            Path(temp_path).unlink()

    def test_xml_with_nested_elements(self):
        """Test reading XML with nested elements"""
        from sqlstream.readers.xml_reader import XMLReader

        xml_content = """<?xml version="1.0"?>
<data>
    <person>
        <name>Alice</name>
        <address>
            <city>New York</city>
            <zip>10001</zip>
        </address>
    </person>
    <person>
        <name>Bob</name>
        <address>
            <city>San Francisco</city>
            <zip>94102</zip>
        </address>
    </person>
</data>"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            temp_path = f.name
            f.write(xml_content)

        try:
            reader = XMLReader(temp_path, element="person")
            rows = list(reader.read_lazy())

            assert len(rows) == 2
            assert rows[0]['name'] == 'Alice'
            # Nested elements use dot notation
            assert rows[0]['address.city'] == 'New York'
            assert rows[0]['address.zip'] == 10001
            assert rows[1]['address.city'] == 'San Francisco'
        finally:
            Path(temp_path).unlink()

    def test_xml_type_inference(self):
        """Test that types are inferred correctly"""
        from sqlstream.core.types import DataType
        from sqlstream.readers.xml_reader import XMLReader

        xml_content = """<?xml version="1.0"?>
<data>
    <record>
        <id>1</id>
        <price>19.99</price>
        <active>true</active>
        <name>Product A</name>
    </record>
    <record>
        <id>2</id>
        <price>29.99</price>
        <active>false</active>
        <name>Product B</name>
    </record>
</data>"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            temp_path = f.name
            f.write(xml_content)

        try:
            reader = XMLReader(temp_path, element="record")
            schema = reader.get_schema()

            assert schema['id'] == DataType.INTEGER
            assert schema['price'] == DataType.FLOAT
            assert schema['active'] == DataType.BOOLEAN
            assert schema['name'] == DataType.STRING
        finally:
            Path(temp_path).unlink()


class TestXMLReaderFiltering:
    """Test filtering capabilities"""

    def test_filter_pushdown(self):
        """Test that filters are applied correctly"""
        from sqlstream.readers.xml_reader import XMLReader
        from sqlstream.sql.ast_nodes import Condition

        xml_content = """<?xml version="1.0"?>
<data>
    <record>
        <name>Alice</name>
        <age>30</age>
    </record>
    <record>
        <name>Bob</name>
        <age>25</age>
    </record>
    <record>
        <name>Charlie</name>
        <age>35</age>
    </record>
</data>"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            temp_path = f.name
            f.write(xml_content)

        try:
            reader = XMLReader(temp_path, element="record")

            # Filter for age > 25
            reader.set_filter([Condition("age", ">", 25)])
            rows = list(reader.read_lazy())

            assert len(rows) == 2
            assert rows[0]['name'] == 'Alice'
            assert rows[1]['name'] == 'Charlie'
        finally:
            Path(temp_path).unlink()

    def test_column_selection(self):
        """Test column selection"""
        from sqlstream.readers.xml_reader import XMLReader

        xml_content = """<?xml version="1.0"?>
<data>
    <record>
        <name>Alice</name>
        <age>30</age>
        <city>New York</city>
    </record>
</data>"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            temp_path = f.name
            f.write(xml_content)

        try:
            reader = XMLReader(temp_path, element="record")
            reader.set_columns(['name', 'age'])
            rows = list(reader.read_lazy())

            assert len(rows) == 1
            assert 'name' in rows[0]
            assert 'age' in rows[0]
            assert 'city' not in rows[0]
        finally:
            Path(temp_path).unlink()


class TestXMLReaderErrors:
    """Test error handling"""

    def test_file_not_found(self):
        """Test handling of missing file"""
        import pytest

        from sqlstream.readers.xml_reader import XMLReader

        with pytest.raises(IOError, match="not found"):
            XMLReader("/nonexistent/file.xml")

    def test_invalid_xml(self):
        """Test handling of malformed XML"""
        import pytest

        from sqlstream.readers.xml_reader import XMLReader

        xml_content = """<?xml version="1.0"?>
<data>
    <record>
        <name>Alice
    </record>
</data>"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            temp_path = f.name
            f.write(xml_content)

        try:
            with pytest.raises(IOError, match="Failed to parse"):
                XMLReader(temp_path)
        finally:
            Path(temp_path).unlink()

    def test_element_not_found(self):
        """Test error when specified element doesn't exist"""
        import pytest

        from sqlstream.readers.xml_reader import XMLReader

        xml_content = """<?xml version="1.0"?>
<data>
    <record>
        <name>Alice</name>
    </record>
</data>"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            temp_path = f.name
            f.write(xml_content)

        try:
            with pytest.raises(ValueError, match="No elements found"):
                XMLReader(temp_path, element="nonexistent")
        finally:
            Path(temp_path).unlink()

    def test_no_repeating_elements(self):
        """Test error when no repeating elements found"""
        import pytest

        from sqlstream.readers.xml_reader import XMLReader

        xml_content = """<?xml version="1.0"?>
<data>
    <name>Alice</name>
    <age>30</age>
</data>"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            temp_path = f.name
            f.write(xml_content)

        try:
            with pytest.raises(ValueError, match="No repeating elements"):
                XMLReader(temp_path)
        finally:
            Path(temp_path).unlink()


class TestXMLReaderIntegration:
    """Test integration with query API"""

    def test_xml_with_query_api(self):
        """Test using XML reader through query API"""
        from sqlstream import query

        xml_content = """<?xml version="1.0"?>
<data>
    <record>
        <name>Alice</name>
        <age>30</age>
    </record>
    <record>
        <name>Bob</name>
        <age>25</age>
    </record>
</data>"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            temp_path = f.name
            f.write(xml_content)

        try:
            # Test with fragment syntax - force pandas/python backend
            results = query(f"{temp_path}#xml:record").sql(
                "SELECT name, age FROM data WHERE age > 25",
                backend="python"
            )
            rows = list(results)

            assert len(rows) == 1
            assert rows[0]['name'] == 'Alice'
            assert rows[0]['age'] == 30
        finally:
            Path(temp_path).unlink()

    def test_xml_to_dataframe(self):
        """Test converting XML reader to DataFrame"""
        import pytest

        from sqlstream.readers.xml_reader import XMLReader

        xml_content = """<?xml version="1.0"?>
<data>
    <record>
        <name>Alice</name>
        <age>30</age>
    </record>
    <record>
        <name>Bob</name>
        <age>25</age>
    </record>
</data>"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            temp_path = f.name
            f.write(xml_content)

        try:
            reader = XMLReader(temp_path, element="record")

            try:
                import pandas as pd
                df = reader.to_dataframe()
                assert len(df) == 2
                assert sorted(df.columns) == ['age', 'name']
                assert df['name'].tolist() == ['Alice', 'Bob']
            except ImportError:
                # Pandas not available, should raise ImportError
                with pytest.raises(ImportError):
                    reader.to_dataframe()
        finally:
            Path(temp_path).unlink()
