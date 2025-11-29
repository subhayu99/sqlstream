"""
Test script for Interactive SQL Shell

Validates that shell components work correctly without
running the full TUI (to avoid terminal keybinding conflicts).
"""

import sys
import tempfile
from pathlib import Path

# Test imports
print("Testing imports...")
try:
    from sqlstream.cli.shell import SQLShellApp, QueryEditor, ResultsViewer, StatusBar
    
    # Verify they accept kwargs (like id)
    rv = ResultsViewer(id="test-viewer")
    sb = StatusBar(id="test-status")
    print("✅ All shell components imported and instantiated successfully")
except ImportError as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

# Test query execution engine
print("\nTesting query execution...")
try:
    from sqlstream.core.query import QueryInline
    
    # Create test data
    test_csv = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    test_csv.write("name,age,city,salary\n")
    test_csv.write("Alice,30,NYC,90000\n")
    test_csv.write("Bob,25,LA,75000\n")
    test_csv.write("Charlie,35,SF,85000\n")
    test_csv.close()
    
    # Test query
    q = QueryInline()
    result = q.sql(f"SELECT * FROM '{test_csv.name}' WHERE age > 25")
    results = result.to_list()
    
    print(f"✅ Query executed: {len(results)} rows returned")
    print(f"   Sample row: {results[0]}")
    
    # Cleanup
    Path(test_csv.name).unlink()
    
except Exception as e:
    print(f"❌ Query execution error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test shell app initialization
print("\nTesting shell app initialization...")
try:
    app = SQLShellApp(initial_file=None, history_file="/tmp/test_history")
    print("✅ Shell app initialized successfully")
    print(f"   Title: {app.title}")
    print(f"   History file: {app.history_file}")
except Exception as e:
    print(f"❌ Shell initialization error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test CLI command registration
print("\nTesting CLI command registration...")
try:
    from sqlstream.cli.main import cli
    
    # Check if shell command exists
    commands = [cmd.name for cmd in cli.commands.values()]
    if 'shell' in commands:
        print("✅ 'shell' command registered in CLI")
        print(f"   Available commands: {', '.join(commands)}")
    else:
        print("❌ 'shell' command not found in CLI")
        sys.exit(1)
        
except Exception as e:
    print(f"❌ CLI test error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Summary
print("\n" + "="*60)
print("✅ All component tests passed!")
print("="*60)
print("\nThe interactive shell is ready to use!")
print("\nTo test manually (in a real terminal, not VSCode):")
print("  python -m sqlstream shell /tmp/test_shell.csv")
print("\nKeybindings:")
print("  Ctrl+Enter or Ctrl+E  - Execute query")
print("  Ctrl+L                - Clear editor")
print("  Ctrl+D                - Exit")
print("  F1                    - Help")
print("\n⚠️  Note: VSCode terminal has Ctrl+J conflicts with Textual.")
print("   For best experience, use a native terminal (gnome-terminal, iTerm2, etc.)")
