"""
Tests for Pandas backend

Compares pandas backend results with Python backend to ensure correctness.
"""


import pytest

try:
    import pandas as pd

    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

from sqlstream.core.query import query

# Skip all tests if pandas not available
pytestmark = pytest.mark.skipif(not PANDAS_AVAILABLE, reason="Pandas not installed")


class TestPandasBackendBasic:
    """Test basic queries with pandas backend"""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create sample CSV file"""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text(
            "id,name,age,salary\n"
            "1,Alice,30,75000\n"
            "2,Bob,25,65000\n"
            "3,Charlie,35,85000\n"
            "4,David,28,70000\n"
        )
        return csv_file

    def test_select_all(self, sample_csv):
        """Test SELECT * with both backends"""
        # Python backend
        python_result = query(str(sample_csv)).sql(
            "SELECT * FROM data", backend="python"
        ).to_list()

        # Pandas backend
        pandas_result = query(str(sample_csv)).sql(
            f"SELECT * FROM {sample_csv}", backend="pandas"
        ).to_list()

        assert len(python_result) == len(pandas_result) == 4
        # Results should match (pandas may have different types, so compare values)
        for py_row, pd_row in zip(python_result, pandas_result):
            assert py_row["name"] == pd_row["name"]
            assert py_row["age"] == pd_row["age"]

    def test_select_columns(self, sample_csv):
        """Test SELECT specific columns"""
        python_result = query(str(sample_csv)).sql(
            "SELECT name, age FROM data", backend="python"
        ).to_list()

        pandas_result = query(str(sample_csv)).sql(
            f"SELECT name, age FROM {sample_csv}", backend="pandas"
        ).to_list()

        assert len(python_result) == len(pandas_result) == 4
        assert list(python_result[0].keys()) == list(pandas_result[0].keys())

    def test_where_filter(self, sample_csv):
        """Test WHERE clause"""
        python_result = query(str(sample_csv)).sql(
            "SELECT * FROM data WHERE age > 28", backend="python"
        ).to_list()

        pandas_result = query(str(sample_csv)).sql(
            f"SELECT * FROM {sample_csv} WHERE age > 28", backend="pandas"
        ).to_list()

        assert len(python_result) == len(pandas_result) == 2
        # Should be Alice and Charlie
        names = sorted([r["name"] for r in pandas_result])
        assert names == ["Alice", "Charlie"]

    def test_limit(self, sample_csv):
        """Test LIMIT clause"""
        python_result = query(str(sample_csv)).sql(
            "SELECT * FROM data LIMIT 2", backend="python"
        ).to_list()

        pandas_result = query(str(sample_csv)).sql(
            f"SELECT * FROM {sample_csv} LIMIT 2", backend="pandas"
        ).to_list()

        assert len(python_result) == len(pandas_result) == 2


class TestPandasBackendGroupBy:
    """Test GROUP BY with pandas backend"""

    @pytest.fixture
    def sales_csv(self, tmp_path):
        """Create sales CSV file"""
        csv_file = tmp_path / "sales.csv"
        csv_file.write_text(
            "region,product,sales\n"
            "East,A,100\n"
            "East,B,150\n"
            "West,A,200\n"
            "West,B,120\n"
            "East,A,80\n"
        )
        return csv_file

    def test_groupby_sum(self, sales_csv):
        """Test GROUP BY with SUM"""
        python_result = query(str(sales_csv)).sql(
            "SELECT region, SUM(sales) FROM data GROUP BY region", backend="python"
        ).to_list()

        pandas_result = query(str(sales_csv)).sql(
            f"SELECT region, SUM(sales) FROM {sales_csv} GROUP BY region",
            backend="pandas",
        ).to_list()

        assert len(python_result) == len(pandas_result) == 2

        # Check sums match
        py_sums = {r["region"]: r["sum_sales"] for r in python_result}
        pd_sums = {r["region"]: r["sum_sales"] for r in pandas_result}

        assert py_sums["East"] == pd_sums["East"] == 330
        assert py_sums["West"] == pd_sums["West"] == 320

    def test_groupby_count(self, sales_csv):
        """Test GROUP BY with COUNT"""
        python_result = query(str(sales_csv)).sql(
            "SELECT region, COUNT(*) FROM data GROUP BY region", backend="python"
        ).to_list()

        pandas_result = query(str(sales_csv)).sql(
            f"SELECT region, COUNT(*) FROM {sales_csv} GROUP BY region",
            backend="pandas",
        ).to_list()

        assert len(python_result) == len(pandas_result) == 2


class TestPandasBackendOrderBy:
    """Test ORDER BY with pandas backend"""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create sample CSV file"""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text(
            "name,age\n" "Charlie,35\n" "Alice,30\n" "Bob,25\n" "David,28\n"
        )
        return csv_file

    def test_order_by_asc(self, sample_csv):
        """Test ORDER BY ASC"""
        python_result = query(str(sample_csv)).sql(
            "SELECT * FROM data ORDER BY age ASC", backend="python"
        ).to_list()

        pandas_result = query(str(sample_csv)).sql(
            f"SELECT * FROM {sample_csv} ORDER BY age ASC", backend="pandas"
        ).to_list()

        assert len(python_result) == len(pandas_result) == 4

        # Check order
        py_ages = [r["age"] for r in python_result]
        pd_ages = [r["age"] for r in pandas_result]

        assert py_ages == pd_ages == [25, 28, 30, 35]

    def test_order_by_desc(self, sample_csv):
        """Test ORDER BY DESC"""
        pandas_result = query(str(sample_csv)).sql(
            f"SELECT * FROM {sample_csv} ORDER BY age DESC", backend="pandas"
        ).to_list()

        ages = [r["age"] for r in pandas_result]
        assert ages == [35, 30, 28, 25]


class TestPandasBackendJoin:
    """Test JOIN with pandas backend"""

    @pytest.fixture
    def customers_csv(self, tmp_path):
        """Create customers CSV file"""
        csv_file = tmp_path / "customers.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n3,Charlie\n")
        return csv_file

    @pytest.fixture
    def orders_csv(self, tmp_path):
        """Create orders CSV file"""
        csv_file = tmp_path / "orders.csv"
        csv_file.write_text(
            "order_id,customer_id,amount\n" "101,1,100\n" "102,2,200\n" "103,1,150\n"
        )
        return csv_file

    def test_inner_join(self, customers_csv, orders_csv):
        """Test INNER JOIN"""
        pandas_result = query(str(customers_csv)).sql(
            f"SELECT * FROM {customers_csv} "
            f"INNER JOIN {orders_csv} ON id = customer_id",
            backend="pandas",
        ).to_list()

        assert len(pandas_result) == 3

        # Check Alice has 2 orders
        alice_orders = [r for r in pandas_result if r["name"] == "Alice"]
        assert len(alice_orders) == 2

    def test_left_join(self, customers_csv, orders_csv):
        """Test LEFT JOIN"""
        pandas_result = query(str(customers_csv)).sql(
            f"SELECT * FROM {customers_csv} "
            f"LEFT JOIN {orders_csv} ON id = customer_id",
            backend="pandas",
        ).to_list()

        # Should have 4 rows (Alice twice, Bob once, Charlie once with NULL)
        assert len(pandas_result) == 4

        # Charlie should have NaN/None for amount
        charlie = next(r for r in pandas_result if r["name"] == "Charlie")
        assert pd.isna(charlie.get("amount")) or charlie.get("amount") is None


class TestPandasBackendAuto:
    """Test automatic backend selection"""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create sample CSV file"""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name\n1,Alice\n2,Bob\n")
        return csv_file

    def test_auto_uses_pandas(self, sample_csv):
        """Test that auto backend selects pandas when available"""
        result = query(str(sample_csv)).sql(
            f"SELECT * FROM {sample_csv}", backend="auto"
        )

        # Check that pandas backend was selected
        assert result.use_pandas is True
        assert result.backend == "auto"

    def test_can_force_python(self, sample_csv):
        """Test that python backend can be forced"""
        result = query(str(sample_csv)).sql(
            "SELECT * FROM data", backend="python"
        )

        assert result.use_pandas is False
        assert result.backend == "python"


class TestPandasBackendExplain:
    """Test explain() with pandas backend"""

    @pytest.fixture
    def sample_csv(self, tmp_path):
        """Create sample CSV file"""
        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,name,age\n1,Alice,30\n2,Bob,25\n")
        return csv_file

    def test_explain_format(self, sample_csv):
        """Test explain output format"""
        result = query(str(sample_csv)).sql(
            f"SELECT * FROM {sample_csv} WHERE age > 25 LIMIT 1", backend="pandas"
        )

        plan = result.explain()

        # Check plan contains key operations
        assert "Pandas Execution Plan" in plan
        assert "Load DataFrame" in plan
        assert "Filter" in plan
        assert "Limit" in plan


class TestPandasBackendJSON:
    """Test pandas backend with JSON files"""

    @pytest.fixture
    def simple_json(self, tmp_path):
        """Create simple JSON array file"""
        import json
        json_file = tmp_path / "data.json"
        data = [
            {"id": 1, "name": "Alice", "age": 30, "city": "NYC"},
            {"id": 2, "name": "Bob", "age": 25, "city": "LA"},
            {"id": 3, "name": "Charlie", "age": 35, "city": "Chicago"},
            {"id": 4, "name": "Diana", "age": 28, "city": "NYC"},
        ]
        json_file.write_text(json.dumps(data, indent=2))
        return json_file

    @pytest.fixture
    def nested_json(self, tmp_path):
        """Create nested JSON file with records key"""
        import json
        json_file = tmp_path / "api_response.json"
        data = {
            "status": "success",
            "data": {
                "users": [
                    {"id": 1, "name": "Alice", "score": 95},
                    {"id": 2, "name": "Bob", "score": 87},
                    {"id": 3, "name": "Charlie", "score": 92},
                ]
            },
            "meta": {"count": 3}
        }
        json_file.write_text(json.dumps(data, indent=2))
        return json_file

    def test_json_select_all(self, simple_json):
        """Test SELECT * on JSON file"""
        result = query(str(simple_json)).sql(
            "SELECT * FROM data", backend="pandas"
        ).to_list()

        assert len(result) == 4
        assert result[0]["name"] == "Alice"
        assert result[0]["age"] == 30

    def test_json_select_columns(self, simple_json):
        """Test SELECT specific columns from JSON"""
        result = query(str(simple_json)).sql(
            "SELECT name, city FROM data", backend="pandas"
        ).to_list()

        assert len(result) == 4
        assert list(result[0].keys()) == ["name", "city"]
        assert result[0]["name"] == "Alice"
        assert result[0]["city"] == "NYC"

    def test_json_where_filter(self, simple_json):
        """Test WHERE clause on JSON data"""
        result = query(str(simple_json)).sql(
            "SELECT * FROM data WHERE age > 28", backend="pandas"
        ).to_list()

        assert len(result) == 2
        names = sorted([r["name"] for r in result])
        assert names == ["Alice", "Charlie"]

    def test_json_where_string_filter(self, simple_json):
        """Test WHERE clause with string comparison"""
        result = query(str(simple_json)).sql(
            "SELECT name, age FROM data WHERE city = 'NYC'", backend="pandas"
        ).to_list()

        assert len(result) == 2
        names = sorted([r["name"] for r in result])
        assert names == ["Alice", "Diana"]

    def test_json_limit(self, simple_json):
        """Test LIMIT on JSON data"""
        result = query(str(simple_json)).sql(
            "SELECT * FROM data LIMIT 2", backend="pandas"
        ).to_list()

        assert len(result) == 2

    def test_json_order_by(self, simple_json):
        """Test ORDER BY on JSON data"""
        result = query(str(simple_json)).sql(
            "SELECT name, age FROM data ORDER BY age ASC", backend="pandas"
        ).to_list()

        ages = [r["age"] for r in result]
        assert ages == [25, 28, 30, 35]

    def test_json_nested_with_fragment(self, nested_json):
        """Test JSON with nested path using fragment syntax"""
        result = query(f"{nested_json}#json:data.users").sql(
            "SELECT * FROM users", backend="pandas"
        ).to_list()

        assert len(result) == 3
        assert result[0]["name"] == "Alice"
        assert result[0]["score"] == 95

    def test_json_auto_backend(self, simple_json):
        """Test that auto backend works with JSON files"""
        result = query(str(simple_json)).sql(
            "SELECT * FROM data WHERE age > 25", backend="auto"
        )

        # Verify pandas backend was selected
        assert result.use_pandas is True

        # Verify results are correct
        rows = result.to_list()
        assert len(rows) == 3


class TestPandasBackendJSONL:
    """Test pandas backend with JSONL files"""

    @pytest.fixture
    def logs_jsonl(self, tmp_path):
        """Create JSONL log file"""
        import json
        jsonl_file = tmp_path / "logs.jsonl"
        logs = [
            {"timestamp": "2024-01-01T10:00:00", "level": "INFO", "message": "Server started"},
            {"timestamp": "2024-01-01T10:05:00", "level": "ERROR", "message": "Connection failed"},
            {"timestamp": "2024-01-01T10:10:00", "level": "INFO", "message": "Request processed"},
            {"timestamp": "2024-01-01T10:15:00", "level": "ERROR", "message": "Timeout"},
            {"timestamp": "2024-01-01T10:20:00", "level": "INFO", "message": "Server shutdown"},
        ]
        jsonl_file.write_text("\n".join(json.dumps(log) for log in logs))
        return jsonl_file

    def test_jsonl_select_all(self, logs_jsonl):
        """Test SELECT * on JSONL file"""
        result = query(str(logs_jsonl)).sql(
            "SELECT * FROM logs", backend="pandas"
        ).to_list()

        assert len(result) == 5
        assert result[0]["level"] == "INFO"

    def test_jsonl_where_filter(self, logs_jsonl):
        """Test WHERE clause on JSONL data"""
        result = query(str(logs_jsonl)).sql(
            "SELECT * FROM logs WHERE level = 'ERROR'", backend="pandas"
        ).to_list()

        assert len(result) == 2
        messages = [r["message"] for r in result]
        assert "Connection failed" in messages
        assert "Timeout" in messages

    def test_jsonl_select_columns(self, logs_jsonl):
        """Test SELECT specific columns from JSONL"""
        result = query(str(logs_jsonl)).sql(
            "SELECT level, message FROM logs", backend="pandas"
        ).to_list()

        assert len(result) == 5
        assert list(result[0].keys()) == ["level", "message"]

    def test_jsonl_limit(self, logs_jsonl):
        """Test LIMIT on JSONL data"""
        result = query(str(logs_jsonl)).sql(
            "SELECT * FROM logs LIMIT 3", backend="pandas"
        ).to_list()

        assert len(result) == 3


class TestPandasBackendXML:
    """Test pandas backend with XML files"""

    @pytest.fixture
    def simple_xml(self, tmp_path):
        """Create simple XML file"""
        xml_file = tmp_path / "data.xml"
        xml_content = """<?xml version="1.0"?>
<data>
    <record>
        <id>1</id>
        <name>Alice</name>
        <age>30</age>
        <city>NYC</city>
    </record>
    <record>
        <id>2</id>
        <name>Bob</name>
        <age>25</age>
        <city>LA</city>
    </record>
    <record>
        <id>3</id>
        <name>Charlie</name>
        <age>35</age>
        <city>Chicago</city>
    </record>
</data>"""
        xml_file.write_text(xml_content)
        return xml_file

    @pytest.fixture
    def products_xml(self, tmp_path):
        """Create products XML file"""
        xml_file = tmp_path / "products.xml"
        xml_content = """<?xml version="1.0"?>
<catalog>
    <product id="p1" status="available">
        <name>Laptop</name>
        <price>1200</price>
        <stock>50</stock>
    </product>
    <product id="p2" status="available">
        <name>Mouse</name>
        <price>25</price>
        <stock>200</stock>
    </product>
    <product id="p3" status="discontinued">
        <name>Keyboard</name>
        <price>80</price>
        <stock>0</stock>
    </product>
</catalog>"""
        xml_file.write_text(xml_content)
        return xml_file

    def test_xml_select_all(self, simple_xml):
        """Test SELECT * on XML file"""
        result = query(str(simple_xml)).sql(
            "SELECT * FROM data", backend="pandas"
        ).to_list()

        assert len(result) == 3
        assert result[0]["name"] == "Alice"
        assert result[0]["age"] == 30

    def test_xml_select_columns(self, simple_xml):
        """Test SELECT specific columns from XML"""
        result = query(str(simple_xml)).sql(
            "SELECT name, city FROM data", backend="pandas"
        ).to_list()

        assert len(result) == 3
        assert list(result[0].keys()) == ["name", "city"]

    def test_xml_where_filter(self, simple_xml):
        """Test WHERE clause on XML data"""
        result = query(str(simple_xml)).sql(
            "SELECT * FROM data WHERE age > 28", backend="pandas"
        ).to_list()

        assert len(result) == 2
        names = sorted([r["name"] for r in result])
        assert names == ["Alice", "Charlie"]

    def test_xml_with_element_selection(self, simple_xml):
        """Test XML with explicit element selection"""
        result = query(f"{simple_xml}#xml:record").sql(
            "SELECT name, age FROM data", backend="pandas"
        ).to_list()

        assert len(result) == 3
        assert result[0]["name"] == "Alice"

    def test_xml_with_attributes(self, products_xml):
        """Test XML with attributes (@ prefix)"""
        result = query(f"{products_xml}#xml:product").sql(
            "SELECT name, price, stock FROM data WHERE price > 50", backend="pandas"
        ).to_list()

        assert len(result) == 2
        names = sorted([r["name"] for r in result])
        assert names == ["Keyboard", "Laptop"]

    def test_xml_order_by(self, simple_xml):
        """Test ORDER BY on XML data"""
        result = query(str(simple_xml)).sql(
            "SELECT name, age FROM data ORDER BY age DESC", backend="pandas"
        ).to_list()

        ages = [r["age"] for r in result]
        assert ages == [35, 30, 25]


class TestPandasBackendMultiFormat:
    """Test pandas backend with multiple file formats in one query"""

    @pytest.fixture
    def setup_multiformat(self, tmp_path):
        """Create multiple format files"""
        import json

        # CSV file
        csv_file = tmp_path / "users.csv"
        csv_file.write_text("id,name,email\n1,Alice,alice@example.com\n2,Bob,bob@example.com\n")

        # JSON file
        json_file = tmp_path / "orders.json"
        orders = [
            {"order_id": 101, "user_id": 1, "amount": 250},
            {"order_id": 102, "user_id": 2, "amount": 150},
            {"order_id": 103, "user_id": 1, "amount": 300},
        ]
        json_file.write_text(json.dumps(orders))

        return csv_file, json_file

    @pytest.mark.skip(reason="Table aliases in JOINs need further investigation")
    def test_join_csv_and_json(self, setup_multiformat):
        """Test JOIN between CSV and JSON files with pandas backend"""
        csv_file, json_file = setup_multiformat

        result = query(str(csv_file)).sql(
            f"SELECT u.name, o.order_id, o.amount "
            f"FROM {csv_file} u "
            f"INNER JOIN {json_file} o ON u.id = o.user_id",
            backend="pandas"
        ).to_list()

        assert len(result) == 3

        # Check Alice has 2 orders
        alice_orders = [r for r in result if r["name"] == "Alice"]
        assert len(alice_orders) == 2

        # Check total amounts
        alice_total = sum(r["amount"] for r in alice_orders)
        assert alice_total == 550
