"""
Pytest configuration and shared fixtures
"""

import pytest


@pytest.fixture
def sample_data():
    """Sample data for testing"""
    return [
        {"name": "Alice", "age": 30, "city": "NYC"},
        {"name": "Bob", "age": 25, "city": "LA"},
        {"name": "Charlie", "age": 35, "city": "SF"},
    ]


@pytest.fixture
def sample_csv_content():
    """Sample CSV content"""
    return """name,age,city
Alice,30,NYC
Bob,25,LA
Charlie,35,SF"""
