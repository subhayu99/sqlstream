"""
Tests for GroupBy and OrderBy operators
"""

import pytest

from sqlstream.operators.groupby import GroupByOperator
from sqlstream.operators.orderby import OrderByOperator
from sqlstream.operators.scan import Scan
from sqlstream.readers.csv_reader import CSVReader
from sqlstream.sql.ast_nodes import AggregateFunction, OrderByColumn


class TestGroupByOperator:
    """Test GroupBy operator"""

    @pytest.fixture
    def sales_csv(self, tmp_path):
        """Create a sales CSV file for testing"""
        csv_file = tmp_path / "sales.csv"
        csv_file.write_text(
            "city,product,amount\n"
            "NYC,Widget,100\n"
            "NYC,Gadget,200\n"
            "LA,Widget,150\n"
            "LA,Gadget,250\n"
            "NYC,Widget,120\n"
        )
        return csv_file

    def test_group_by_single_column_count(self, sales_csv):
        """Test GROUP BY city with COUNT(*)"""
        reader = CSVReader(str(sales_csv))
        scan = Scan(reader)

        # GROUP BY city
        agg = [AggregateFunction("COUNT", "*", None)]
        groupby = GroupByOperator(scan, ["city"], agg, ["city", "count_*"])

        rows = list(groupby)

        # Should have 2 groups: NYC and LA
        assert len(rows) == 2

        # Check results (order may vary)
        results = {row["city"]: row["count_*"] for row in rows}
        assert results["NYC"] == 3
        assert results["LA"] == 2

    def test_group_by_with_sum(self, sales_csv):
        """Test GROUP BY with SUM aggregation"""
        reader = CSVReader(str(sales_csv))
        scan = Scan(reader)

        # GROUP BY city, SUM(amount)
        agg = [AggregateFunction("SUM", "amount", "total")]
        groupby = GroupByOperator(scan, ["city"], agg, ["city", "total"])

        rows = list(groupby)

        results = {row["city"]: row["total"] for row in rows}
        assert results["NYC"] == 420  # 100 + 200 + 120
        assert results["LA"] == 400  # 150 + 250

    def test_group_by_with_avg(self, sales_csv):
        """Test GROUP BY with AVG aggregation"""
        reader = CSVReader(str(sales_csv))
        scan = Scan(reader)

        # GROUP BY city, AVG(amount)
        agg = [AggregateFunction("AVG", "amount", "avg_amount")]
        groupby = GroupByOperator(scan, ["city"], agg, ["city", "avg_amount"])

        rows = list(groupby)

        results = {row["city"]: row["avg_amount"] for row in rows}
        assert results["NYC"] == pytest.approx(140.0)  # (100+200+120)/3
        assert results["LA"] == pytest.approx(200.0)  # (150+250)/2

    def test_group_by_multiple_columns(self, sales_csv):
        """Test GROUP BY with multiple columns"""
        reader = CSVReader(str(sales_csv))
        scan = Scan(reader)

        # GROUP BY city, product
        agg = [AggregateFunction("SUM", "amount", "total")]
        groupby = GroupByOperator(
            scan, ["city", "product"], agg, ["city", "product", "total"]
        )

        rows = list(groupby)

        # Should have 4 groups: (NYC, Widget), (NYC, Gadget), (LA, Widget), (LA, Gadget)
        assert len(rows) == 4

        # Create lookup dict
        results = {(r["city"], r["product"]): r["total"] for r in rows}
        assert results[("NYC", "Widget")] == 220  # 100 + 120
        assert results[("NYC", "Gadget")] == 200
        assert results[("LA", "Widget")] == 150
        assert results[("LA", "Gadget")] == 250

    def test_group_by_multiple_aggregates(self, sales_csv):
        """Test GROUP BY with multiple aggregate functions"""
        reader = CSVReader(str(sales_csv))
        scan = Scan(reader)

        # GROUP BY city with COUNT, SUM, AVG
        agg = [
            AggregateFunction("COUNT", "*", "count"),
            AggregateFunction("SUM", "amount", "total"),
            AggregateFunction("AVG", "amount", "average"),
        ]
        groupby = GroupByOperator(
            scan, ["city"], agg, ["city", "count", "total", "average"]
        )

        rows = list(groupby)

        nyc = next(r for r in rows if r["city"] == "NYC")
        assert nyc["count"] == 3
        assert nyc["total"] == 420
        assert nyc["average"] == pytest.approx(140.0)

    def test_group_by_with_min_max(self, sales_csv):
        """Test GROUP BY with MIN and MAX"""
        reader = CSVReader(str(sales_csv))
        scan = Scan(reader)

        # GROUP BY city with MIN and MAX
        agg = [
            AggregateFunction("MIN", "amount", "min_amount"),
            AggregateFunction("MAX", "amount", "max_amount"),
        ]
        groupby = GroupByOperator(
            scan, ["city"], agg, ["city", "min_amount", "max_amount"]
        )

        rows = list(groupby)

        results = {row["city"]: (row["min_amount"], row["max_amount"]) for row in rows}
        assert results["NYC"] == (100, 200)
        assert results["LA"] == (150, 250)


class TestOrderByOperator:
    """Test OrderBy operator"""

    @pytest.fixture
    def people_csv(self, tmp_path):
        """Create a people CSV file for testing"""
        csv_file = tmp_path / "people.csv"
        csv_file.write_text(
            "name,age,city\n"
            "Alice,30,NYC\n"
            "Bob,25,LA\n"
            "Charlie,35,NYC\n"
            "David,25,SF\n"
            "Eve,28,LA\n"
        )
        return csv_file

    def test_order_by_single_asc(self, people_csv):
        """Test ORDER BY single column ASC"""
        reader = CSVReader(str(people_csv))
        scan = Scan(reader)

        # ORDER BY age ASC
        orderby = OrderByOperator(scan, [OrderByColumn("age", "ASC")])

        rows = list(orderby)

        # Check ages are in ascending order
        ages = [row["age"] for row in rows]
        assert ages == [25, 25, 28, 30, 35]

    def test_order_by_single_desc(self, people_csv):
        """Test ORDER BY single column DESC"""
        reader = CSVReader(str(people_csv))
        scan = Scan(reader)

        # ORDER BY age DESC
        orderby = OrderByOperator(scan, [OrderByColumn("age", "DESC")])

        rows = list(orderby)

        # Check ages are in descending order
        ages = [row["age"] for row in rows]
        assert ages == [35, 30, 28, 25, 25]

    def test_order_by_multiple_columns(self, people_csv):
        """Test ORDER BY multiple columns"""
        reader = CSVReader(str(people_csv))
        scan = Scan(reader)

        # ORDER BY city ASC, age DESC
        orderby = OrderByOperator(
            scan, [OrderByColumn("city", "ASC"), OrderByColumn("age", "DESC")]
        )

        rows = list(orderby)

        # Check order
        result = [(row["city"], row["age"]) for row in rows]
        assert result == [
            ("LA", 28),  # Eve
            ("LA", 25),  # Bob
            ("NYC", 35),  # Charlie
            ("NYC", 30),  # Alice
            ("SF", 25),  # David
        ]

    def test_order_by_string_column(self, people_csv):
        """Test ORDER BY string column"""
        reader = CSVReader(str(people_csv))
        scan = Scan(reader)

        # ORDER BY name ASC
        orderby = OrderByOperator(scan, [OrderByColumn("name", "ASC")])

        rows = list(orderby)

        names = [row["name"] for row in rows]
        assert names == ["Alice", "Bob", "Charlie", "David", "Eve"]

    def test_order_by_string_desc(self, people_csv):
        """Test ORDER BY string column DESC"""
        reader = CSVReader(str(people_csv))
        scan = Scan(reader)

        # ORDER BY name DESC
        orderby = OrderByOperator(scan, [OrderByColumn("name", "DESC")])

        rows = list(orderby)

        names = [row["name"] for row in rows]
        assert names == ["Eve", "David", "Charlie", "Bob", "Alice"]

    def test_order_by_preserves_all_columns(self, people_csv):
        """Test that ORDER BY preserves all columns"""
        reader = CSVReader(str(people_csv))
        scan = Scan(reader)

        orderby = OrderByOperator(scan, [OrderByColumn("age", "ASC")])

        rows = list(orderby)

        # Check first row has all columns
        first_row = rows[0]
        assert "name" in first_row
        assert "age" in first_row
        assert "city" in first_row


class TestCombinedGroupByOrderBy:
    """Test combining GroupBy and OrderBy"""

    @pytest.fixture
    def sales_csv(self, tmp_path):
        """Create sales CSV for testing"""
        csv_file = tmp_path / "sales.csv"
        csv_file.write_text(
            "city,amount\n"
            "NYC,100\n"
            "LA,200\n"
            "NYC,150\n"
            "SF,180\n"
            "LA,120\n"
        )
        return csv_file

    def test_group_by_then_order_by(self, sales_csv):
        """Test GROUP BY followed by ORDER BY"""
        reader = CSVReader(str(sales_csv))
        scan = Scan(reader)

        # GROUP BY city, SUM(amount)
        agg = [AggregateFunction("SUM", "amount", "total")]
        groupby = GroupByOperator(scan, ["city"], agg, ["city", "total"])

        # ORDER BY total DESC
        orderby = OrderByOperator(groupby, [OrderByColumn("total", "DESC")])

        rows = list(orderby)

        # Check results are sorted by total descending
        totals = [row["total"] for row in rows]
        assert totals == [320, 250, 180]  # LA, NYC, SF

        cities = [row["city"] for row in rows]
        assert cities == ["LA", "NYC", "SF"]
