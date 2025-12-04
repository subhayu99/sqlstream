import json

import pytest

from sqlstream.readers.json_reader import JSONReader
from sqlstream.sql.ast_nodes import Condition


def test_json_reader_array(tmp_path):
    data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35},
    ]

    file_path = tmp_path / "test.json"
    with open(file_path, "w") as f:
        json.dump(data, f)

    reader = JSONReader(str(file_path))
    rows = list(reader.read_lazy())

    assert len(rows) == 3
    assert rows[0]["name"] == "Alice"
    assert rows[1]["name"] == "Bob"


def test_json_reader_records_key(tmp_path):
    data = {"meta": {"version": 1}, "data": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}

    file_path = tmp_path / "test_key.json"
    with open(file_path, "w") as f:
        json.dump(data, f)

    # Test with explicit key
    reader = JSONReader(str(file_path), records_key="data")
    rows = list(reader.read_lazy())
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice"

    # Test with auto-detect
    reader_auto = JSONReader(str(file_path))
    rows_auto = list(reader_auto.read_lazy())
    assert len(rows_auto) == 2
    assert rows_auto[0]["name"] == "Alice"


def test_json_reader_filter(tmp_path):
    data = [{"id": 1, "val": 10}, {"id": 2, "val": 20}, {"id": 3, "val": 30}]

    file_path = tmp_path / "test_filter.json"
    with open(file_path, "w") as f:
        json.dump(data, f)

    reader = JSONReader(str(file_path))
    reader.set_filter([Condition("val", ">", 15)])

    rows = list(reader.read_lazy())
    assert len(rows) == 2
    assert rows[0]["id"] == 2
    assert rows[1]["id"] == 3


def test_json_reader_columns(tmp_path):
    data = [
        {"id": 1, "name": "Alice", "extra": "ignore"},
        {"id": 2, "name": "Bob", "extra": "ignore"},
    ]

    file_path = tmp_path / "test_cols.json"
    with open(file_path, "w") as f:
        json.dump(data, f)

    reader = JSONReader(str(file_path))
    reader.set_columns(["id", "name"])

    rows = list(reader.read_lazy())
    assert len(rows) == 2
    assert "extra" not in rows[0]
    assert "id" in rows[0]
    assert "name" in rows[0]


def test_json_reader_limit(tmp_path):
    data = [{"id": i} for i in range(10)]

    file_path = tmp_path / "test_limit.json"
    with open(file_path, "w") as f:
        json.dump(data, f)

    reader = JSONReader(str(file_path))
    reader.set_limit(5)

    rows = list(reader.read_lazy())
    assert len(rows) == 5
    assert rows[-1]["id"] == 4


def test_json_reader_invalid_file(tmp_path):
    file_path = tmp_path / "invalid.json"
    with open(file_path, "w") as f:
        f.write("{invalid json")

    reader = JSONReader(str(file_path))
    with pytest.raises(ValueError, match="Invalid JSON"):
        list(reader.read_lazy())


def test_json_reader_no_list(tmp_path):
    file_path = tmp_path / "nolist.json"
    with open(file_path, "w") as f:
        json.dump({"a": 1}, f)

    # Should treat as single row
    reader = JSONReader(str(file_path))
    rows = list(reader.read_lazy())
    assert len(rows) == 1
    assert rows[0]["a"] == 1
