#!/usr/bin/env python3
"""
Example demonstrating JSON nested path syntax in SQLStream
"""

import json
import tempfile
from pathlib import Path
from sqlstream import query

# Create example data
data = {
    "api_version": "1.0",
    "result": {
        "users": [
            {
                "id": 1,
                "name": "Alice",
                "transactions": [
                    {"id": "t1", "amount": 50, "status": "completed"},
                    {"id": "t2", "amount": 75, "status": "pending"}
                ]
            },
            {
                "id": 2,
                "name": "Bob",
                "transactions": [
                    {"id": "t3", "amount": 100, "status": "completed"},
                    {"id": "t4", "amount": 125, "status": "completed"}
                ]
            },
            {
                "id": 3,
                "name": "Charlie",
                "transactions": [
                    {"id": "t5", "amount": 200, "status": "failed"}
                ]
            }
        ]
    }
}

# Save to temporary file
with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
    json.dump(data, f)
    json_file = f.name

print("=" * 60)
print("JSON Nested Path Examples")
print("=" * 60)

# Example 1: Simple nested access
print("\n1. Get all users: result.users")
print("   Query: SELECT * FROM users")
for row in query(f"{json_file}#json:result.users").sql("SELECT * FROM users"):
    print(f"   {row}")

# Example 2: Array indexing
print("\n2. Get first user's transactions: result.users[0].transactions")
print("   Query: SELECT * FROM transactions")
for row in query(f"{json_file}#json:result.users[0].transactions").sql("SELECT * FROM transactions"):
    print(f"   {row}")

# Example 3: Array flattening
print("\n3. Flatten all transactions from all users: result.users[].transactions")
print("   Query: SELECT * FROM transactions WHERE status = 'completed'")
for row in query(f"{json_file}#json:result.users[].transactions").sql(
    "SELECT * FROM transactions WHERE status = 'completed'"
):
    print(f"   {row}")

# Example 4: Aggregation on flattened data
print("\n4. Total amount of completed transactions")
print("   Query: SELECT amount FROM transactions WHERE status = 'completed'")
completed = list(query(f"{json_file}#json:result.users[].transactions").sql(
    "SELECT amount FROM transactions WHERE status = 'completed'"
))
total = sum(row['amount'] for row in completed)
print(f"   Total: ${total} from {len(completed)} transactions")

# Clean up
Path(json_file).unlink()

print("\n" + "=" * 60)
print("Supported Path Syntax:")
print("=" * 60)
print("  key              - Simple key access")
print("  key.nested       - Nested object access")
print("  key[0]           - Array index (0-based)")
print("  key[]            - Flatten array")
print("  key[].nested     - Flatten and access nested")
print("  a.b[0].c.d[].e   - Any combination")
print("=" * 60)
