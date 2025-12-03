"""
Tests for CSV reader
"""

import tempfile
from pathlib import Path

import pytest

from sqlstream.readers.csv_reader import CSVReader
from sqlstream.sql.ast_nodes import Condition
from sqlstream.core.types import DataType


@pytest.fixture
def sample_csv_file(tmp_path):
    """Create a sample CSV file for testing"""
    csv_content = """name,age,city,salary
Alice,30,NYC,75000
Bob,25,LA,65000
Charlie,35,SF,85000
Diana,28,NYC,70000
Eve,32,LA,80000"""

    csv_file = tmp_path / "test_data.csv"
    csv_file.write_text(csv_content)
    return csv_file


@pytest.fixture
def mixed_types_csv(tmp_path):
    """CSV with mixed types for testing type inference"""
    csv_content = """name,age,price,active
Alice,30,19.99,true
Bob,25,25.50,false
Charlie,35,30.00,true"""

    csv_file = tmp_path / "mixed.csv"
    csv_file.write_text(csv_content)
    return csv_file


@pytest.fixture
def empty_csv(tmp_path):
    """Empty CSV file"""
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("name,age,city\n")
    return csv_file


class TestBasicReading:
    """Test basic CSV reading"""

    def test_read_csv(self, sample_csv_file):
        """Test reading CSV file"""
        reader = CSVReader(str(sample_csv_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 5
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 30  # Should be int
        assert rows[0]["city"] == "NYC"

    def test_type_inference(self, mixed_types_csv):
        """Test that types are inferred correctly"""
        reader = CSVReader(str(mixed_types_csv))
        rows = list(reader.read_lazy())

        row = rows[0]

        # String
        assert isinstance(row["name"], str)
        assert row["name"] == "Alice"

        # Integer
        assert isinstance(row["age"], int)
        assert row["age"] == 30

        # Float
        assert isinstance(row["price"], float)
        assert row["price"] == 19.99

        # Boolean (now inferred by enhanced type system)
        assert isinstance(row["active"], bool)

    def test_empty_csv(self, empty_csv):
        """Test reading empty CSV"""
        reader = CSVReader(str(empty_csv))
        rows = list(reader.read_lazy())

        assert len(rows) == 0

    def test_file_not_found(self):
        """Test error when file doesn't exist"""
        with pytest.raises(FileNotFoundError):
            CSVReader("nonexistent.csv")


class TestLazyIteration:
    """Test lazy iteration (generator behavior)"""

    def test_lazy_evaluation(self, sample_csv_file):
        """Test that data is not loaded all at once"""
        reader = CSVReader(str(sample_csv_file))
        iterator = reader.read_lazy()

        # Iterator should be a generator
        assert hasattr(iterator, "__iter__")
        assert hasattr(iterator, "__next__")

        # Get first row
        first_row = next(iterator)
        assert first_row["name"] == "Alice"

        # Get second row
        second_row = next(iterator)
        assert second_row["name"] == "Bob"

    def test_iterate_with_for_loop(self, sample_csv_file):
        """Test using reader in for loop"""
        reader = CSVReader(str(sample_csv_file))
        names = []

        for row in reader:
            names.append(row["name"])

        assert names == ["Alice", "Bob", "Charlie", "Diana", "Eve"]


class TestPredicatePushdown:
    """Test predicate pushdown optimization"""

    def test_filter_equals(self, sample_csv_file):
        """Test filter with equals"""
        reader = CSVReader(str(sample_csv_file))
        reader.set_filter([Condition("city", "=", "NYC")])

        rows = list(reader.read_lazy())

        assert len(rows) == 2
        assert all(row["city"] == "NYC" for row in rows)

    def test_filter_greater_than(self, sample_csv_file):
        """Test filter with greater than"""
        reader = CSVReader(str(sample_csv_file))
        reader.set_filter([Condition("age", ">", 30)])

        rows = list(reader.read_lazy())

        assert len(rows) == 2
        assert all(row["age"] > 30 for row in rows)

    def test_filter_multiple_conditions(self, sample_csv_file):
        """Test filter with multiple AND conditions"""
        reader = CSVReader(str(sample_csv_file))
        reader.set_filter([Condition("age", ">", 25), Condition("city", "=", "NYC")])

        rows = list(reader.read_lazy())

        # Only Alice (30, NYC) and Diana (28, NYC) match
        assert len(rows) == 2
        assert all(row["age"] > 25 and row["city"] == "NYC" for row in rows)

    def test_filter_no_matches(self, sample_csv_file):
        """Test filter that matches nothing"""
        reader = CSVReader(str(sample_csv_file))
        reader.set_filter([Condition("age", ">", 100)])

        rows = list(reader.read_lazy())

        assert len(rows) == 0

    def test_supports_pushdown(self, sample_csv_file):
        """Test that CSV reader supports pushdown"""
        reader = CSVReader(str(sample_csv_file))
        assert reader.supports_pushdown() is True


class TestColumnPruning:
    """Test column pruning optimization"""

    def test_select_specific_columns(self, sample_csv_file):
        """Test selecting only specific columns"""
        reader = CSVReader(str(sample_csv_file))
        reader.set_columns(["name", "age"])

        rows = list(reader.read_lazy())

        assert len(rows) == 5
        # Should only have name and age
        assert set(rows[0].keys()) == {"name", "age"}
        assert "city" not in rows[0]
        assert "salary" not in rows[0]

    def test_select_single_column(self, sample_csv_file):
        """Test selecting single column"""
        reader = CSVReader(str(sample_csv_file))
        reader.set_columns(["name"])

        rows = list(reader.read_lazy())

        assert len(rows) == 5
        assert set(rows[0].keys()) == {"name"}

    def test_supports_column_selection(self, sample_csv_file):
        """Test that CSV reader supports column selection"""
        reader = CSVReader(str(sample_csv_file))
        assert reader.supports_column_selection() is True


class TestCombinedOptimizations:
    """Test combining predicate pushdown and column pruning"""

    def test_filter_and_project(self, sample_csv_file):
        """Test applying both filter and column selection"""
        reader = CSVReader(str(sample_csv_file))
        reader.set_filter([Condition("age", ">", 30)])
        reader.set_columns(["name", "age"])

        rows = list(reader.read_lazy())

        # Only rows with age > 30
        assert len(rows) == 2

        # Only name and age columns
        assert set(rows[0].keys()) == {"name", "age"}

        # Verify values
        assert all(row["age"] > 30 for row in rows)


class TestSchemaInference:
    """Test schema inference"""

    def test_get_schema(self, mixed_types_csv):
        """Test schema inference from first row"""
        reader = CSVReader(str(mixed_types_csv))
        schema = reader.get_schema()

        assert schema.get_column_type("name") == DataType.STRING
        assert schema.get_column_type("age") == DataType.INTEGER
        assert schema.get_column_type("price") == DataType.FLOAT

    def test_schema_empty_file(self, empty_csv):
        """Test schema on empty file"""
        reader = CSVReader(str(empty_csv))
        schema = reader.get_schema()

        assert len(schema or []) == 0


class TestMalformedData:
    """Test handling of malformed data"""

    def test_null_values(self, tmp_path):
        """Test handling of NULL/empty values"""
        csv_content = """name,age,city
Alice,30,NYC
Bob,,LA
Charlie,35,"""

        csv_file = tmp_path / "nulls.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(str(csv_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 3
        assert rows[1]["age"] is None
        assert rows[2]["city"] is None

    def test_null_value_variations(self, tmp_path):
        """Test different NULL representations"""
        csv_content = """name,age,city,notes
Alice,30,NYC,
Bob,,,null
Charlie,35,SF,N/A
Diana,28,LA,-"""

        csv_file = tmp_path / "null_variations.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(str(csv_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 4
        # Empty string should be None
        assert rows[0]["notes"] is None
        # Empty age should be None
        assert rows[1]["age"] is None
        # "null" string remains as string (not automatically converted)
        assert rows[1]["notes"] == "null"
        # "N/A" and "-" remain as strings
        assert rows[2]["notes"] == "N/A"
        assert rows[3]["notes"] == "-"

    def test_inconsistent_row_lengths(self, tmp_path):
        """Test handling of rows with different column counts"""
        csv_content = """name,age,city
Alice,30,NYC
Bob,25
Charlie,35,SF,extra_column
Diana,28,LA"""

        csv_file = tmp_path / "inconsistent.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(str(csv_file))
        # Should handle inconsistent rows gracefully
        # Rows with missing columns are OK, but extra columns may cause issues
        import warnings
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            rows = list(reader.read_lazy())

            # Charlie's row with extra column is skipped (malformed)
            # Only Alice, Bob, Diana are returned
            assert len(rows) == 3

            # Should have warning about skipped row
            assert len(w) >= 1
            assert "Skipping malformed row" in str(w[0].message)

        # Bob's row is missing city - this is handled gracefully
        assert rows[1]["name"] == "Bob"
        assert rows[1]["city"] is None


class TestEncodingHandling:
    """Test different file encodings"""

    def test_utf8_encoding(self, tmp_path):
        """Test UTF-8 encoding (default)"""
        csv_content = """name,city,country
José,São Paulo,Brasil
François,Montréal,Canada
北京,中国,China"""

        csv_file = tmp_path / "utf8.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        reader = CSVReader(str(csv_file), encoding="utf-8")
        rows = list(reader.read_lazy())

        assert len(rows) == 3
        assert rows[0]["name"] == "José"
        assert rows[0]["city"] == "São Paulo"
        assert rows[1]["name"] == "François"
        assert rows[2]["name"] == "北京"

    def test_latin1_encoding(self, tmp_path):
        """Test Latin-1 encoding"""
        csv_content = """name,city
José,São Paulo
François,Montréal"""

        csv_file = tmp_path / "latin1.csv"
        # Write with latin-1 encoding
        csv_file.write_text(csv_content, encoding="latin-1")

        # Read with latin-1 encoding specified
        reader = CSVReader(str(csv_file), encoding="latin-1")
        rows = list(reader.read_lazy())

        assert len(rows) == 2
        assert rows[0]["name"] == "José"
        assert rows[1]["name"] == "François"

    def test_utf8_with_bom(self, tmp_path):
        """Test UTF-8 with BOM (Byte Order Mark)"""
        csv_content = """name,age
Alice,30
Bob,25"""

        csv_file = tmp_path / "utf8_bom.csv"
        # Write with UTF-8 BOM
        csv_file.write_text(csv_content, encoding="utf-8-sig")

        # Should handle BOM gracefully
        reader = CSVReader(str(csv_file), encoding="utf-8-sig")
        rows = list(reader.read_lazy())

        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"


class TestDelimiterHandling:
    """Test different CSV delimiters"""

    def test_tab_delimiter(self, tmp_path):
        """Test tab-separated values (TSV)"""
        tsv_content = """name\tage\tcity
Alice\t30\tNYC
Bob\t25\tLA
Charlie\t35\tSF"""

        tsv_file = tmp_path / "data.tsv"
        tsv_file.write_text(tsv_content)

        reader = CSVReader(str(tsv_file), delimiter="\t")
        rows = list(reader.read_lazy())

        assert len(rows) == 3
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 30
        assert rows[0]["city"] == "NYC"

    def test_pipe_delimiter(self, tmp_path):
        """Test pipe-separated values"""
        psv_content = """name|age|city
Alice|30|NYC
Bob|25|LA
Charlie|35|SF"""

        psv_file = tmp_path / "data.psv"
        psv_file.write_text(psv_content)

        reader = CSVReader(str(psv_file), delimiter="|")
        rows = list(reader.read_lazy())

        assert len(rows) == 3
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 30

    def test_semicolon_delimiter(self, tmp_path):
        """Test semicolon-separated values"""
        ssv_content = """name;age;city
Alice;30;NYC
Bob;25;LA
Charlie;35;SF"""

        ssv_file = tmp_path / "data.ssv"
        ssv_file.write_text(ssv_content)

        reader = CSVReader(str(ssv_file), delimiter=";")
        rows = list(reader.read_lazy())

        assert len(rows) == 3
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 30


class TestEdgeCases:
    """Test edge cases and boundary conditions"""

    def test_completely_empty_file(self, tmp_path):
        """Test file with no content at all"""
        csv_file = tmp_path / "completely_empty.csv"
        csv_file.write_text("")

        reader = CSVReader(str(csv_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 0

    def test_only_header_no_data(self, tmp_path):
        """Test file with only header row"""
        csv_file = tmp_path / "header_only.csv"
        csv_file.write_text("name,age,city\n")

        reader = CSVReader(str(csv_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 0

    def test_single_row(self, tmp_path):
        """Test file with single data row"""
        csv_content = """name,age,city
Alice,30,NYC"""

        csv_file = tmp_path / "single_row.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(str(csv_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"

    def test_very_long_field(self, tmp_path):
        """Test field with very long content"""
        long_text = "A" * 10000  # 10KB of text
        csv_content = f"""name,description
Alice,{long_text}
Bob,Short text"""

        csv_file = tmp_path / "long_field.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(str(csv_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 2
        assert len(rows[0]["description"]) == 10000
        assert rows[0]["description"] == long_text

    def test_special_characters_in_data(self, tmp_path):
        """Test handling of special characters"""
        csv_content = """name,emoji,symbols
Alice,😀🎉,@#$%
Bob,🚀💻,<>&|
Charlie,❤️✨,{}[]"""

        csv_file = tmp_path / "special_chars.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        reader = CSVReader(str(csv_file), encoding="utf-8")
        rows = list(reader.read_lazy())

        assert len(rows) == 3
        assert rows[0]["emoji"] == "😀🎉"
        assert rows[0]["symbols"] == "@#$%"
        assert rows[1]["emoji"] == "🚀💻"

    def test_quoted_fields_with_commas(self, tmp_path):
        """Test quoted fields containing commas"""
        csv_content = """name,address,city
Alice,"123 Main St, Apt 4",NYC
Bob,"456 Oak Ave, Suite 200",LA"""

        csv_file = tmp_path / "quoted.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(str(csv_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 2
        assert rows[0]["address"] == "123 Main St, Apt 4"
        assert rows[1]["address"] == "456 Oak Ave, Suite 200"

    def test_quoted_fields_with_newlines(self, tmp_path):
        """Test quoted fields containing newlines"""
        csv_content = '''name,bio,city
Alice,"Software
Engineer",NYC
Bob,"Data Scientist
ML Expert",LA'''

        csv_file = tmp_path / "multiline.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(str(csv_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 2
        assert "Software\nEngineer" in rows[0]["bio"]
        assert "Data Scientist\nML Expert" in rows[1]["bio"]

    def test_escape_sequences(self, tmp_path):
        """Test handling of escape sequences"""
        csv_content = r'''name,path,description
Alice,"C:\Users\Alice\Documents","User's folder"
Bob,"D:\Projects\Code","Bob's workspace"'''

        csv_file = tmp_path / "escapes.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(str(csv_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 2
        # CSV module should handle escaped quotes
        assert rows[0]["description"] == "User's folder"


class TestTypeInferenceEdgeCases:
    """Test edge cases in type inference"""

    def test_mixed_int_float_column(self, tmp_path):
        """Test column with mixed integers and floats"""
        csv_content = """name,value
Alice,100
Bob,25.5
Charlie,30
Diana,42.0"""

        csv_file = tmp_path / "mixed_numbers.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(str(csv_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 4
        # Each value inferred independently
        assert rows[0]["value"] == 100  # int
        assert rows[1]["value"] == 25.5  # float
        assert rows[2]["value"] == 30  # int
        assert rows[3]["value"] == 42.0  # float

    def test_scientific_notation(self, tmp_path):
        """Test scientific notation numbers"""
        csv_content = """name,value
Alice,1.5e10
Bob,2.3e-5
Charlie,1e6"""

        csv_file = tmp_path / "scientific.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(str(csv_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 3
        assert rows[0]["value"] == 1.5e10
        assert rows[1]["value"] == 2.3e-5
        assert rows[2]["value"] == 1e6

    def test_leading_zeros(self, tmp_path):
        """Test numbers with leading zeros (should remain as strings if not parseable)"""
        csv_content = """id,zip_code,value
1,00123,456
2,01234,789
3,00000,100"""

        csv_file = tmp_path / "leading_zeros.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(str(csv_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 3
        # Leading zeros get stripped when parsed as int
        # zip_code "00123" becomes 123 (int)
        assert rows[0]["zip_code"] == 123
        # value is normal int
        assert rows[0]["value"] == 456

    def test_whitespace_handling(self, tmp_path):
        """Test handling of whitespace in values"""
        csv_content = """name,age,city
  Alice  ,  30  ,  NYC
Bob,25,LA
  Charlie,35,SF"""

        csv_file = tmp_path / "whitespace.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(str(csv_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 3
        # _infer_value_type does NOT strip whitespace for strings (preserves fidelity)
        # But it does strip for numbers (int/float)
        assert rows[0]["name"] == "  Alice  "
        assert rows[0]["age"] == 30
        assert rows[0]["city"] == "  NYC"

    def test_boolean_like_strings(self, tmp_path):
        """Test that boolean-like strings remain as strings"""
        csv_content = """name,active,verified
Alice,true,yes
Bob,false,no
Charlie,TRUE,YES
Diana,1,0"""

        csv_file = tmp_path / "booleans.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(str(csv_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 4
        # Boolean strings ARE converted now (case-insensitive)
        assert rows[0]["active"] is True
        assert rows[0]["verified"] == "yes"  # 'yes' is not boolean in our inference

        # Case-insensitive boolean parsing: TRUE -> True
        assert rows[2]["active"] is True

        # Numbers 1 and 0 are parsed as int, not bool
        assert rows[3]["active"] == 1
        assert rows[3]["verified"] == 0

    def test_negative_numbers(self, tmp_path):
        """Test negative numbers"""
        csv_content = """name,balance,temperature
Alice,-1000,-5.5
Bob,500,98.6
Charlie,-250,-10"""

        csv_file = tmp_path / "negatives.csv"
        csv_file.write_text(csv_content)

        reader = CSVReader(str(csv_file))
        rows = list(reader.read_lazy())

        assert len(rows) == 3
        assert rows[0]["balance"] == -1000
        assert rows[0]["temperature"] == -5.5
        assert rows[2]["temperature"] == -10
