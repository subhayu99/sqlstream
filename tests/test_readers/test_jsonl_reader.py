import json
import pytest
from sqlstream.readers.jsonl_reader import JSONLReader
from sqlstream.sql.ast_nodes import Condition

def test_jsonl_reader_basic(tmp_path):
    lines = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 3, "name": "Charlie"}
    ]
    
    file_path = tmp_path / "test.jsonl"
    with open(file_path, "w") as f:
        for line in lines:
            f.write(json.dumps(line) + "\n")
            
    reader = JSONLReader(str(file_path))
    rows = list(reader.read_lazy())
    
    assert len(rows) == 3
    assert rows[0]["name"] == "Alice"
    assert rows[2]["name"] == "Charlie"

def test_jsonl_reader_filter(tmp_path):
    lines = [
        {"id": 1, "val": 10},
        {"id": 2, "val": 20},
        {"id": 3, "val": 30}
    ]
    
    file_path = tmp_path / "test_filter.jsonl"
    with open(file_path, "w") as f:
        for line in lines:
            f.write(json.dumps(line) + "\n")
            
    reader = JSONLReader(str(file_path))
    reader.set_filter([Condition("val", ">", 15)])
    
    rows = list(reader.read_lazy())
    assert len(rows) == 2
    assert rows[0]["id"] == 2

def test_jsonl_reader_malformed(tmp_path):
    file_path = tmp_path / "malformed.jsonl"
    with open(file_path, "w") as f:
        f.write('{"id": 1}\n')
        f.write('invalid json\n')
        f.write('{"id": 2}\n')
        
    reader = JSONLReader(str(file_path))
    
    # Should warn but continue
    with pytest.warns(UserWarning, match="Skipping invalid JSON"):
        rows = list(reader.read_lazy())
        
    assert len(rows) == 2
    assert rows[0]["id"] == 1
    assert rows[1]["id"] == 2

def test_jsonl_reader_columns(tmp_path):
    lines = [
        {"id": 1, "name": "Alice", "extra": "ignore"},
        {"id": 2, "name": "Bob", "extra": "ignore"}
    ]
    
    file_path = tmp_path / "test_cols.jsonl"
    with open(file_path, "w") as f:
        for line in lines:
            f.write(json.dumps(line) + "\n")
            
    reader = JSONLReader(str(file_path))
    reader.set_columns(["id", "name"])
    
    rows = list(reader.read_lazy())
    assert len(rows) == 2
    assert "extra" not in rows[0]
    assert "id" in rows[0]

def test_jsonl_reader_limit(tmp_path):
    file_path = tmp_path / "test_limit.jsonl"
    with open(file_path, "w") as f:
        for i in range(10):
            f.write(json.dumps({"id": i}) + "\n")
            
    reader = JSONLReader(str(file_path))
    reader.set_limit(5)
    
    rows = list(reader.read_lazy())
    assert len(rows) == 5
    assert rows[-1]["id"] == 4
