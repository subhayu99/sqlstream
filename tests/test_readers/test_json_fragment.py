
def test_json_reader_fragment_integration(tmp_path):
    """Test that we can specify the key via fragment in the query"""
    import json
    from sqlstream.core.query import Query
    
    data = {
        "custom_key": [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ],
        "other_key": []
    }
    
    file_path = tmp_path / "test_fragment.json"
    with open(file_path, "w") as f:
        json.dump(data, f)
        
    # Use the fragment syntax
    source = f"{file_path}#json:custom_key"
    query = Query(source)
    
    # Verify reader was created with correct key
    assert query.reader.records_key == "custom_key"
    
    # Verify reading works
    rows = list(query.reader.read_lazy())
    assert len(rows) == 2
    assert rows[0]["name"] == "Alice"

def test_json_reader_numeric_string_key(tmp_path):
    """Test that a numeric string key works (e.g. "2024")"""
    import json
    from sqlstream.core.query import Query
    
    data = {
        "2024": [
            {"id": 1, "year": 2024}
        ]
    }
    
    file_path = tmp_path / "test_numeric_key.json"
    with open(file_path, "w") as f:
        json.dump(data, f)
        
    # Fragment with numeric key - parser might see as int, but query should cast to str
    source = f"{file_path}#json:2024"
    query = Query(source)
    
    # Verify reader was created with correct key (as string)
    assert query.reader.records_key == "2024"
    
    rows = list(query.reader.read_lazy())
    assert len(rows) == 1
    assert rows[0]["year"] == 2024
