import json

import pytest

from sqlstream.readers.json_reader import JSONReader


def test_nested_object_access(tmp_path):
    """Test accessing nested objects with dot notation"""
    data = {
        "result": {
            "orders": [
                {"id": 1, "total": 100},
                {"id": 2, "total": 200}
            ]
        }
    }

    file_path = tmp_path / "nested.json"
    with open(file_path, "w") as f:
        json.dump(data, f)

    reader = JSONReader(str(file_path), records_key="result.orders")
    rows = list(reader.read_lazy())

    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[1]["total"] == 200

def test_array_index_access(tmp_path):
    """Test accessing specific array index"""
    data = {
        "users": [
            {
                "name": "Alice",
                "transactions": [
                    {"id": 1, "amount": 50},
                    {"id": 2, "amount": 75}
                ]
            },
            {
                "name": "Bob",
                "transactions": [
                    {"id": 3, "amount": 100}
                ]
            }
        ]
    }

    file_path = tmp_path / "indexed.json"
    with open(file_path, "w") as f:
        json.dump(data, f)

    # Get first user's transactions
    reader = JSONReader(str(file_path), records_key="users[0].transactions")
    rows = list(reader.read_lazy())

    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[1]["amount"] == 75

def test_array_flatten(tmp_path):
    """Test flattening arrays with [] operator"""
    data = {
        "users": [
            {
                "name": "Alice",
                "transactions": [
                    {"id": 1, "amount": 50},
                    {"id": 2, "amount": 75}
                ]
            },
            {
                "name": "Bob",
                "transactions": [
                    {"id": 3, "amount": 100},
                    {"id": 4, "amount": 125}
                ]
            }
        ]
    }

    file_path = tmp_path / "flatten.json"
    with open(file_path, "w") as f:
        json.dump(data, f)

    # Flatten transactions from all users
    reader = JSONReader(str(file_path), records_key="users[].transactions")
    rows = list(reader.read_lazy())

    assert len(rows) == 4
    assert rows[0]["id"] == 1
    assert rows[2]["id"] == 3
    assert rows[3]["amount"] == 125

def test_simple_array_flatten(tmp_path):
    """Test flattening a simple array without nested access"""
    data = {
        "items": [
            {"id": 1},
            {"id": 2},
            {"id": 3}
        ]
    }

    file_path = tmp_path / "simple_flatten.json"
    with open(file_path, "w") as f:
        json.dump(data, f)

    reader = JSONReader(str(file_path), records_key="items[]")
    rows = list(reader.read_lazy())

    assert len(rows) == 3
    assert rows[0]["id"] == 1

def test_deep_nested_path(tmp_path):
    """Test deeply nested path"""
    data = {
        "api": {
            "v1": {
                "users": {
                    "list": [
                        {"id": 1, "name": "Alice"},
                        {"id": 2, "name": "Bob"}
                    ]
                }
            }
        }
    }

    file_path = tmp_path / "deep.json"
    with open(file_path, "w") as f:
        json.dump(data, f)

    reader = JSONReader(str(file_path), records_key="api.v1.users.list")
    rows = list(reader.read_lazy())

    assert len(rows) == 2
    assert rows[0]["name"] == "Alice"

def test_invalid_path_key_not_found(tmp_path):
    """Test error when key doesn't exist"""
    data = {"result": {"items": []}}

    file_path = tmp_path / "invalid.json"
    with open(file_path, "w") as f:
        json.dump(data, f)

    reader = JSONReader(str(file_path), records_key="result.missing")

    with pytest.raises(ValueError, match="Key 'missing' not found"):
        list(reader.read_lazy())

def test_invalid_array_index(tmp_path):
    """Test error when array index is out of range"""
    data = {"users": [{"id": 1}]}

    file_path = tmp_path / "invalid_index.json"
    with open(file_path, "w") as f:
        json.dump(data, f)

    reader = JSONReader(str(file_path), records_key="users[5]")

    with pytest.raises(ValueError, match="Index 5 out of range"):
        list(reader.read_lazy())

def test_flatten_with_missing_nested_keys(tmp_path):
    """Test that flattening skips items without the nested key"""
    data = {
        "users": [
            {"name": "Alice", "transactions": [{"id": 1}]},
            {"name": "Bob"},  # No transactions
            {"name": "Charlie", "transactions": [{"id": 2}, {"id": 3}]}
        ]
    }

    file_path = tmp_path / "partial.json"
    with open(file_path, "w") as f:
        json.dump(data, f)

    reader = JSONReader(str(file_path), records_key="users[].transactions")
    rows = list(reader.read_lazy())

    # Should only get transactions from Alice and Charlie
    assert len(rows) == 3
    assert rows[0]["id"] == 1
    assert rows[2]["id"] == 3

def test_integration_with_query(tmp_path):
    """Test that nested paths work with the full query API"""
    from sqlstream.core.query import Query

    data = {
        "response": {
            "data": {
                "orders": [
                    {"id": 1, "status": "completed"},
                    {"id": 2, "status": "pending"}
                ]
            }
        }
    }

    file_path = tmp_path / "query_test.json"
    with open(file_path, "w") as f:
        json.dump(data, f)

    # Use the fragment syntax with nested path
    source = f"{file_path}#json:response.data.orders"
    query = Query(source)

    rows = list(query.reader.read_lazy())
    assert len(rows) == 2
    assert rows[0]["status"] == "completed"
