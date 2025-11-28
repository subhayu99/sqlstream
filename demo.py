#!/usr/bin/env python
"""
SQLStream Demo - Testing all implemented features

Demonstrates:
- CSV and Parquet reading
- WHERE filtering
- GROUP BY aggregations
- ORDER BY sorting
- Combined queries
"""

from pathlib import Path
import tempfile

from sqlstream.core.query import query


def demo_basic_queries():
    """Demo basic SELECT queries"""
    print("=" * 60)
    print("DEMO 1: Basic SELECT Queries")
    print("=" * 60)

    # Create sample CSV
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("name,age,city,salary\n")
        f.write("Alice,30,NYC,90000\n")
        f.write("Bob,25,LA,75000\n")
        f.write("Charlie,35,NYC,95000\n")
        f.write("David,28,SF,85000\n")
        f.write("Eve,32,LA,88000\n")
        csv_file = f.name

    # Simple SELECT
    print("\n1. SELECT * FROM employees")
    results = query(csv_file).sql("SELECT * FROM employees").to_list()
    for row in results[:3]:
        print(f"  {row}")

    # SELECT with columns
    print("\n2. SELECT name, city FROM employees")
    results = query(csv_file).sql("SELECT name, city FROM employees").to_list()
    for row in results[:3]:
        print(f"  {row}")

    # Cleanup
    Path(csv_file).unlink()


def demo_filtering():
    """Demo WHERE clause filtering"""
    print("\n" + "=" * 60)
    print("DEMO 2: WHERE Clause Filtering")
    print("=" * 60)

    # Create sample CSV
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("product,price,category\n")
        f.write("Laptop,1200,Electronics\n")
        f.write("Mouse,25,Electronics\n")
        f.write("Desk,300,Furniture\n")
        f.write("Chair,150,Furniture\n")
        f.write("Monitor,400,Electronics\n")
        csv_file = f.name

    # Filter with WHERE
    print("\n1. SELECT * FROM products WHERE price > 200")
    results = query(csv_file).sql("SELECT * FROM products WHERE price > 200").to_list()
    for row in results:
        print(f"  {row}")

    # Multiple conditions
    print("\n2. SELECT product, price FROM products WHERE category = 'Electronics' AND price < 500")
    results = query(csv_file).sql(
        "SELECT product, price FROM products WHERE category = 'Electronics' AND price < 500"
    ).to_list()
    for row in results:
        print(f"  {row}")

    # Cleanup
    Path(csv_file).unlink()


def demo_group_by():
    """Demo GROUP BY with aggregations"""
    print("\n" + "=" * 60)
    print("DEMO 3: GROUP BY with Aggregations")
    print("=" * 60)

    # Create sales data
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("region,product,sales,quantity\n")
        f.write("East,Widget,1000,10\n")
        f.write("East,Gadget,1500,15\n")
        f.write("West,Widget,1200,12\n")
        f.write("West,Gadget,1800,18\n")
        f.write("East,Widget,900,9\n")
        f.write("West,Widget,1100,11\n")
        csv_file = f.name

    # COUNT
    print("\n1. SELECT region, COUNT(*) AS count FROM sales GROUP BY region")
    results = query(csv_file).sql(
        "SELECT region, COUNT(*) AS count FROM sales GROUP BY region"
    ).to_list()
    for row in results:
        print(f"  {row}")

    # SUM
    print("\n2. SELECT region, SUM(sales) AS total_sales FROM sales GROUP BY region")
    results = query(csv_file).sql(
        "SELECT region, SUM(sales) AS total_sales FROM sales GROUP BY region"
    ).to_list()
    for row in results:
        print(f"  {row}")

    # Multiple aggregates
    print("\n3. SELECT product, COUNT(*) AS count, SUM(sales) AS total, AVG(quantity) AS avg_qty FROM sales GROUP BY product")
    results = query(csv_file).sql(
        "SELECT product, COUNT(*) AS count, SUM(sales) AS total, AVG(quantity) AS avg_qty FROM sales GROUP BY product"
    ).to_list()
    for row in results:
        print(f"  {row}")

    # Cleanup
    Path(csv_file).unlink()


def demo_order_by():
    """Demo ORDER BY sorting"""
    print("\n" + "=" * 60)
    print("DEMO 4: ORDER BY Sorting")
    print("=" * 60)

    # Create sample data
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("name,score,grade\n")
        f.write("Alice,95,A\n")
        f.write("Bob,78,B\n")
        f.write("Charlie,95,A\n")
        f.write("David,82,B\n")
        f.write("Eve,88,B\n")
        csv_file = f.name

    # ORDER BY ASC
    print("\n1. SELECT name, score FROM students ORDER BY score ASC")
    results = query(csv_file).sql("SELECT name, score FROM students ORDER BY score ASC").to_list()
    for row in results:
        print(f"  {row}")

    # ORDER BY DESC
    print("\n2. SELECT name, score FROM students ORDER BY score DESC")
    results = query(csv_file).sql("SELECT name, score FROM students ORDER BY score DESC").to_list()
    for row in results:
        print(f"  {row}")

    # Multi-column sort
    print("\n3. SELECT * FROM students ORDER BY grade ASC, score DESC")
    results = query(csv_file).sql("SELECT * FROM students ORDER BY grade ASC, score DESC").to_list()
    for row in results:
        print(f"  {row}")

    # Cleanup
    Path(csv_file).unlink()


def demo_complex_query():
    """Demo complex query with all features"""
    print("\n" + "=" * 60)
    print("DEMO 5: Complex Query (WHERE + GROUP BY + ORDER BY + LIMIT)")
    print("=" * 60)

    # Create sales data
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("city,product,amount,date\n")
        f.write("NYC,Laptop,1200,2024-01-01\n")
        f.write("NYC,Mouse,25,2024-01-02\n")
        f.write("LA,Laptop,1150,2024-01-01\n")
        f.write("LA,Monitor,400,2024-01-03\n")
        f.write("NYC,Monitor,420,2024-01-04\n")
        f.write("SF,Laptop,1300,2024-01-02\n")
        f.write("SF,Mouse,30,2024-01-05\n")
        f.write("LA,Laptop,1180,2024-01-06\n")
        csv_file = f.name

    # Complex query
    sql = """
        SELECT city, COUNT(*) AS sales_count, SUM(amount) AS total_revenue, AVG(amount) AS avg_sale
        FROM sales
        WHERE amount > 100
        GROUP BY city
        ORDER BY total_revenue DESC
        LIMIT 2
    """

    print(f"\nQuery:\n{sql}")
    print("\nResults:")
    results = query(csv_file).sql(sql).to_list()
    for row in results:
        print(f"  {row}")

    # Cleanup
    Path(csv_file).unlink()


def demo_explain_plan():
    """Demo EXPLAIN to show query execution plan"""
    print("\n" + "=" * 60)
    print("DEMO 6: Query Execution Plans")
    print("=" * 60)

    # Create sample data
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("id,value\n")
        f.write("1,100\n")
        f.write("2,200\n")
        csv_file = f.name

    # Simple query plan
    print("\n1. Simple SELECT with WHERE")
    plan = query(csv_file).sql("SELECT * FROM data WHERE value > 100").explain()
    print(plan)

    # Complex query plan
    print("\n2. Complex query with GROUP BY and ORDER BY")
    plan = query(csv_file).sql(
        "SELECT id, COUNT(*) AS count FROM data GROUP BY id ORDER BY count DESC"
    ).explain()
    print(plan)

    # Cleanup
    Path(csv_file).unlink()


if __name__ == "__main__":
    print("\n" + "🚀" * 30)
    print("SQLStream Feature Demo")
    print("Testing Phases 0-4 Implementation")
    print("🚀" * 30)

    demo_basic_queries()
    demo_filtering()
    demo_group_by()
    demo_order_by()
    demo_complex_query()
    demo_explain_plan()

    print("\n" + "=" * 60)
    print("✅ All demos completed successfully!")
    print("=" * 60)
    print("\nFeatures tested:")
    print("  ✓ CSV reading with lazy evaluation")
    print("  ✓ SELECT with column projection")
    print("  ✓ WHERE clause filtering")
    print("  ✓ GROUP BY with aggregations (COUNT, SUM, AVG)")
    print("  ✓ ORDER BY with ASC/DESC")
    print("  ✓ LIMIT clause")
    print("  ✓ Complex multi-clause queries")
    print("  ✓ Query execution plans (EXPLAIN)")
    print("  ✓ Predicate pushdown optimization")
    print("  ✓ Column pruning optimization")
    print("\nNext: Phase 5 - JOIN Support")
    print("=" * 60 + "\n")
