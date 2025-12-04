"""
Tests for JOIN operator
"""


import pytest

from sqlstream.core.query import query


class TestInnerJoin:
    """Test INNER JOIN functionality"""

    @pytest.fixture
    def customers_csv(self, tmp_path):
        """Create customers CSV file"""
        csv_file = tmp_path / "customers.csv"
        csv_file.write_text(
            "id,name,city\n"
            "1,Alice,NYC\n"
            "2,Bob,LA\n"
            "3,Charlie,SF\n"
            "4,David,NYC\n"
        )
        return csv_file

    @pytest.fixture
    def orders_csv(self, tmp_path):
        """Create orders CSV file"""
        csv_file = tmp_path / "orders.csv"
        csv_file.write_text(
            "order_id,customer_id,amount\n"
            "101,1,100\n"
            "102,1,200\n"
            "103,2,150\n"
            "104,3,300\n"
            "105,1,50\n"
        )
        return csv_file

    def test_inner_join_basic(self, customers_csv, orders_csv):
        """Test basic INNER JOIN"""
        sql = f"""
            SELECT *
            FROM {customers_csv}
            INNER JOIN {orders_csv} ON id = customer_id
        """

        results = query(str(customers_csv)).sql(sql).to_list()

        # Should have 5 rows (all orders have matching customers)
        assert len(results) == 5

        # Check first row
        first_row = results[0]
        assert first_row["name"] == "Alice"
        assert first_row["amount"] == 100

    def test_inner_join_with_where(self, customers_csv, orders_csv):
        """Test INNER JOIN with WHERE clause"""
        sql = f"""
            SELECT name, amount
            FROM {customers_csv}
            INNER JOIN {orders_csv} ON id = customer_id
            WHERE amount > 100
        """

        results = query(str(customers_csv)).sql(sql).to_list()

        # Should have 3 rows (Alice-200, Bob-150, Charlie-300)
        assert len(results) == 3

        # All amounts should be > 100
        for row in results:
            assert row["amount"] > 100

    def test_inner_join_with_columns(self, customers_csv, orders_csv):
        """Test INNER JOIN with specific columns"""
        sql = f"""
            SELECT name, city, amount
            FROM {customers_csv}
            INNER JOIN {orders_csv} ON id = customer_id
        """

        results = query(str(customers_csv)).sql(sql).to_list()

        assert len(results) == 5

        # Check columns exist
        first_row = results[0]
        assert "name" in first_row
        assert "city" in first_row
        assert "amount" in first_row

    def test_inner_join_with_order_by(self, customers_csv, orders_csv):
        """Test INNER JOIN with ORDER BY"""
        sql = f"""
            SELECT name, amount
            FROM {customers_csv}
            INNER JOIN {orders_csv} ON id = customer_id
            ORDER BY amount DESC
        """

        results = query(str(customers_csv)).sql(sql).to_list()

        assert len(results) == 5

        # Check descending order
        amounts = [row["amount"] for row in results]
        assert amounts == sorted(amounts, reverse=True)

    def test_inner_join_with_limit(self, customers_csv, orders_csv):
        """Test INNER JOIN with LIMIT"""
        sql = f"""
            SELECT name, amount
            FROM {customers_csv}
            INNER JOIN {orders_csv} ON id = customer_id
            LIMIT 3
        """

        results = query(str(customers_csv)).sql(sql).to_list()

        assert len(results) == 3

    def test_inner_join_no_matches(self, tmp_path):
        """Test INNER JOIN with no matching rows"""
        customers = tmp_path / "customers.csv"
        customers.write_text("id,name\n1,Alice\n2,Bob\n")

        orders = tmp_path / "orders.csv"
        orders.write_text("order_id,customer_id,amount\n101,99,100\n")

        sql = f"""
            SELECT *
            FROM {customers}
            INNER JOIN {orders} ON id = customer_id
        """

        results = query(str(customers)).sql(sql).to_list()

        # No matching rows
        assert len(results) == 0

    def test_join_keyword_defaults_to_inner(self, customers_csv, orders_csv):
        """Test that JOIN defaults to INNER JOIN"""
        sql = f"""
            SELECT name, amount
            FROM {customers_csv}
            JOIN {orders_csv} ON id = customer_id
        """

        results = query(str(customers_csv)).sql(sql).to_list()

        # Should behave like INNER JOIN
        assert len(results) == 5


class TestLeftJoin:
    """Test LEFT JOIN functionality"""

    @pytest.fixture
    def customers_csv(self, tmp_path):
        """Create customers CSV file"""
        csv_file = tmp_path / "customers.csv"
        csv_file.write_text(
            "id,name\n"
            "1,Alice\n"
            "2,Bob\n"
            "3,Charlie\n"
        )
        return csv_file

    @pytest.fixture
    def orders_csv(self, tmp_path):
        """Create orders CSV file"""
        csv_file = tmp_path / "orders.csv"
        csv_file.write_text(
            "order_id,customer_id,amount\n"
            "101,1,100\n"
            "102,2,200\n"
        )
        return csv_file

    def test_left_join_with_unmatched(self, customers_csv, orders_csv):
        """Test LEFT JOIN includes unmatched left rows"""
        sql = f"""
            SELECT *
            FROM {customers_csv}
            LEFT JOIN {orders_csv} ON id = customer_id
        """

        results = query(str(customers_csv)).sql(sql, backend="python").to_list()

        # Should have 3 rows (all customers, even Charlie with no orders)
        assert len(results) == 3

        # Find Charlie's row
        charlie_row = next(r for r in results if r["name"] == "Charlie")

        # Charlie should have NULL/missing order fields
        assert charlie_row.get("amount") is None


class TestRightJoin:
    """Test RIGHT JOIN functionality"""

    @pytest.fixture
    def customers_csv(self, tmp_path):
        """Create customers CSV file"""
        csv_file = tmp_path / "customers.csv"
        csv_file.write_text(
            "id,name\n"
            "1,Alice\n"
            "2,Bob\n"
        )
        return csv_file

    @pytest.fixture
    def orders_csv(self, tmp_path):
        """Create orders CSV file"""
        csv_file = tmp_path / "orders.csv"
        csv_file.write_text(
            "order_id,customer_id,amount\n"
            "101,1,100\n"
            "102,2,200\n"
            "103,99,300\n"  # No matching customer
        )
        return csv_file

    def test_right_join_with_unmatched(self, customers_csv, orders_csv):
        """Test RIGHT JOIN includes unmatched right rows"""
        sql = f"""
            SELECT *
            FROM {customers_csv}
            RIGHT JOIN {orders_csv} ON id = customer_id
        """

        results = query(str(customers_csv)).sql(sql, backend="python").to_list()

        # Should have 3 rows (all orders)
        assert len(results) == 3

        # Find orphaned order (customer_id=99)
        orphaned = next(r for r in results if r.get("customer_id") == 99)

        # Should have NULL for customer name
        assert orphaned.get("name") is None
        assert orphaned["amount"] == 300


class TestJoinEdgeCases:
    """Test edge cases and special scenarios"""

    def test_join_with_null_keys(self, tmp_path):
        """Test JOIN with NULL join keys"""
        left = tmp_path / "left.csv"
        left.write_text("id,name\n1,Alice\n,Bob\n")  # Bob has NULL id

        right = tmp_path / "right.csv"
        right.write_text("user_id,amount\n1,100\n")

        sql = f"""
            SELECT *
            FROM {left}
            INNER JOIN {right} ON id = user_id
        """

        results = query(str(left)).sql(sql).to_list()

        # NULL keys should not match
        assert len(results) == 1
        assert results[0]["name"] == "Alice"

    def test_join_multiple_matches(self, tmp_path):
        """Test JOIN with multiple matching rows"""
        customers = tmp_path / "customers.csv"
        customers.write_text("id,name\n1,Alice\n")

        orders = tmp_path / "orders.csv"
        orders.write_text(
            "order_id,customer_id,amount\n"
            "101,1,100\n"
            "102,1,200\n"
            "103,1,300\n"
        )

        sql = f"""
            SELECT name, amount
            FROM {customers}
            INNER JOIN {orders} ON id = customer_id
        """

        results = query(str(customers)).sql(sql).to_list()

        # One customer with 3 orders = 3 output rows
        assert len(results) == 3

        # All should be Alice
        assert all(r["name"] == "Alice" for r in results)

        # Check amounts
        amounts = sorted([r["amount"] for r in results])
        assert amounts == [100, 200, 300]

    def test_join_complex_query(self, tmp_path):
        """Test JOIN with WHERE, ORDER BY, and LIMIT"""
        customers = tmp_path / "customers.csv"
        customers.write_text(
            "id,name,city\n"
            "1,Alice,NYC\n"
            "2,Bob,LA\n"
            "3,Charlie,NYC\n"
        )

        orders = tmp_path / "orders.csv"
        orders.write_text(
            "order_id,customer_id,amount\n"
            "101,1,100\n"
            "102,2,250\n"
            "103,3,150\n"
            "104,1,300\n"
        )

        sql = f"""
            SELECT name, city, amount
            FROM {customers}
            INNER JOIN {orders} ON id = customer_id
            WHERE city = 'NYC'
            ORDER BY amount DESC
            LIMIT 2
        """

        results = query(str(customers)).sql(sql).to_list()

        # Should have 2 rows (top 2 NYC orders)
        assert len(results) == 2

        # Check order and content
        assert results[0]["amount"] == 300  # Alice's large order
        assert results[1]["amount"] == 150  # Charlie's order

        # Both should be NYC
        assert all(r["city"] == "NYC" for r in results)
