"""
Test MarkdownReader - parsing and querying markdown tables
"""

import tempfile
import os


class TestMarkdownReaderBasic:
    """Test basic markdown table parsing"""

    def test_simple_table(self):
        """Test parsing a simple GFM table"""
        from sqlstream.readers.markdown_reader import MarkdownReader

        md = """
# Test Data

| Name   | Age | City     |
|:-------|----:|---------:|
| Alice  | 30  | New York |
| Bob    | 25  | Boston   |
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(md)
            temp_path = f.name

        try:
            reader = MarkdownReader(temp_path)
            rows = list(reader.read_lazy())

            assert len(rows) == 2
            assert rows[0]['Name'] == 'Alice'
            assert rows[0]['Age'] == 30
            assert rows[1]['Name'] == 'Bob'
            assert rows[1]['City'] == 'Boston'
        finally:
            os.unlink(temp_path)

    def test_type_inference(self):
        """Test type inference from markdown table values"""
        from sqlstream.readers.markdown_reader import MarkdownReader

        md = """
| Name  | Count | Price | Active |
|:------|------:|------:|:------:|
| Item1 | 10    | 19.99 | true   |
| Item2 | 5     | 9.50  | false  |
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(md)
            temp_path = f.name

        try:
            reader = MarkdownReader(temp_path)
            rows = list(reader.read_lazy())

            # Check types were inferred correctly
            assert isinstance(rows[0]['Name'], str)
            assert isinstance(rows[0]['Count'], int)
            assert isinstance(rows[0]['Price'], float)
            assert isinstance(rows[0]['Active'], bool)
            assert rows[0]['Active'] is True
            assert rows[1]['Active'] is False
        finally:
            os.unlink(temp_path)

    def test_null_values(self):
        """Test NULL value recognition"""
        from sqlstream.readers.markdown_reader import MarkdownReader

        md = """
| Name  | Value |
|:------|------:|
| A     | 10    |
| B     | null  |
| C     | N/A   |
| D     | -     |
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(md)
            temp_path = f.name

        try:
            reader = MarkdownReader(temp_path)
            rows = list(reader.read_lazy())

            assert rows[0]['Value'] == 10
            assert rows[1]['Value'] is None
            assert rows[2]['Value'] is None
            assert rows[3]['Value'] is None
        finally:
            os.unlink(temp_path)

    def test_multiple_tables(self):
        """Test parsing multiple tables in one markdown file"""
        from sqlstream.readers.markdown_reader import MarkdownReader

        md = """
# First Table

| A | B |
|---|---|
| 1 | 2 |

# Second Table

| X | Y | Z |
|---|---|---|
| a | b | c |
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(md)
            temp_path = f.name

        try:
            # Read first table
            reader = MarkdownReader(temp_path, table=0)
            rows = list(reader.read_lazy())
            assert 'A' in rows[0]
            assert rows[0]['A'] == 1

            # Read second table
            reader = MarkdownReader(temp_path, table=1)
            rows = list(reader.read_lazy())
            assert 'X' in rows[0]
            assert rows[0]['X'] == 'a'
        finally:
            os.unlink(temp_path)

    def test_list_tables(self):
        """Test listing all tables in markdown"""
        from sqlstream.readers.markdown_reader import MarkdownReader

        md = """
| A | B |
|---|---|
| 1 | 2 |
| 3 | 4 |

Some text

| X | Y | Z |
|---|---|---|
| a | b | c |
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(md)
            temp_path = f.name

        try:
            reader = MarkdownReader(temp_path)
            tables = reader.list_tables()

            assert len(tables) == 2
            assert 'Table 0' in tables[0]
            assert 'A, B' in tables[0]
            assert '2 rows' in tables[0]
            assert 'Table 1' in tables[1]
        finally:
            os.unlink(temp_path)

    def test_schema_inference(self):
        """Test schema inference from markdown table"""
        from sqlstream.readers.markdown_reader import MarkdownReader
        from sqlstream.core.types import DataType

        md = """
| Name   | Age | Score | Active |
|:-------|----:|------:|:------:|
| Alice  | 30  | 95.5  | true   |
| Bob    | 25  | 87.3  | false  |
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(md)
            temp_path = f.name

        try:
            reader = MarkdownReader(temp_path)
            schema = reader.get_schema()

            assert schema['Name'] == DataType.STRING
            assert schema['Age'] == DataType.INTEGER
            assert schema['Score'] == DataType.FLOAT
            assert schema['Active'] == DataType.BOOLEAN
        finally:
            os.unlink(temp_path)

    def test_escaped_pipes(self):
        """Test handling of escaped pipe characters"""
        from sqlstream.readers.markdown_reader import MarkdownReader

        md = """
| Text        | Count |
|:------------|------:|
| A \\| B      | 1     |
| Normal      | 2     |
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(md)
            temp_path = f.name

        try:
            reader = MarkdownReader(temp_path)
            rows = list(reader.read_lazy())

            # Escaped pipe should be preserved
            assert '|' in rows[0]['Text']
        finally:
            os.unlink(temp_path)

    def test_empty_markdown_error(self):
        """Test error when markdown has no tables"""
        from sqlstream.readers.markdown_reader import MarkdownReader

        md = "# Just a heading\n\nNo tables here!"

        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(md)
            temp_path = f.name

        try:
            try:
                reader = MarkdownReader(temp_path)
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert "No tables found" in str(e)
        finally:
            os.unlink(temp_path)

    def test_table_index_out_of_range(self):
        """Test error when table index is out of range"""
        from sqlstream.readers.markdown_reader import MarkdownReader

        md = """
| A |
|---|
| 1 |
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(md)
            temp_path = f.name

        try:
            try:
                reader = MarkdownReader(temp_path, table=5)
                assert False, "Should have raised ValueError"
            except ValueError as e:
                assert "out of range" in str(e)
        finally:
            os.unlink(temp_path)


class TestMarkdownReaderIntegration:
    """Test markdown reader integration with query engine"""

    def test_query_markdown_file(self):
        """Test querying markdown table with SQLstream"""
        from sqlstream import query

        md = """
# Products

| Product | Price | Stock |
|:--------|------:|------:|
| Apple   | 1.50  | 100   |
| Banana  | 0.75  | 150   |
| Cherry  | 2.25  | 50    |
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(md)
            temp_path = f.name

        try:
            result = query(temp_path).sql("SELECT Product, Price FROM data WHERE Stock < 100")
            rows = result.to_list()

            assert len(rows) == 1
            assert rows[0]['Product'] == 'Cherry'
            assert rows[0]['Price'] == 2.25
        finally:
            os.unlink(temp_path)

    def test_query_markdown_with_fragment(self):
        """Test querying markdown with URL fragment"""
        from sqlstream import query

        md = """
| A |
|---|
| 1 |

| Product | Quantity |
|:--------|:--------:|
| Widget  | 42       |
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as f:
            f.write(md)
            temp_path = f.name

        try:
            # Query second table using fragment
            result = query(f"{temp_path}#markdown:1").sql("SELECT * FROM data")
            rows = result.to_list()

            assert len(rows) == 1
            assert rows[0]['Product'] == 'Widget'
            assert rows[0]['Quantity'] == 42
        finally:
            os.unlink(temp_path)
