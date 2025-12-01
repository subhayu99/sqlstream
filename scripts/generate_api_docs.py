#!/usr/bin/env python3
"""
Auto-generate API reference documentation files from source code.

This script discovers all modules in the sqlstream package and generates
markdown files with mkdocstrings syntax for automatic API documentation.

Usage:
    python scripts/generate_api_docs.py
"""

import ast
import os
import re
from pathlib import Path
from typing import Dict, List, Set
import yaml


class APIDocGenerator:
    """Generate API reference documentation from source code"""

    def __init__(self, source_dir: str = "sqlstream", docs_dir: str = "docs/api/reference"):
        self.source_dir = Path(source_dir)
        self.docs_dir = Path(docs_dir)
        self.docs_dir.mkdir(parents=True, exist_ok=True)

        self.ignore_patterns = {
            '__pycache__', '__init__.py', 'test_', '.pyc', '.pyo'
        }

        # Category definitions with nice titles
        self.category_titles = {
            "cli": "CLI Reference",
            "query": "Query API Reference",
            "readers": "Readers API Reference",
            "types": "Type System Reference",
            "operators": "Operators Reference",
            "optimizers": "Optimizers Reference",
            "executor": "Execution Engines Reference",
            "sql": "SQL Parser Reference",
            "utils": "Utilities Reference",
        }

        self.category_descriptions = {
            "cli": "CLI API documentation.",
            "query": "API documentation for query execution.",
            "readers": "Data source readers for various formats.",
            "types": "Type system and schema definitions.",
            "operators": "Query execution operators.",
            "optimizers": "Query optimization strategies.",
            "executor": "Query execution engines (Python and Pandas backends).",
            "sql": "SQL parsing and AST generation.",
            "utils": "Utility functions and helpers.",
        }

    def discover_modules(self) -> Dict[str, List[str]]:
        """Automatically discover all Python modules in the source directory"""
        modules_by_category = {}

        # Walk through the source directory
        for root, dirs, files in os.walk(self.source_dir):
            # Skip __pycache__ and other ignored directories
            dirs[:] = [d for d in dirs if d not in self.ignore_patterns]

            # Get the relative path from source_dir
            rel_path = Path(root).relative_to(self.source_dir)

            # Determine category (top-level subdirectory)
            if rel_path == Path('.'):
                # Root level files - skip
                continue

            parts = rel_path.parts
            category = parts[0]  # e.g., 'core', 'readers', 'operators'

            # Process Python files in this directory
            for file in files:
                if file.endswith('.py') and not any(pattern in file for pattern in self.ignore_patterns):
                    # Build the module path
                    module_parts = ['sqlstream'] + list(parts) + [file[:-3]]  # Remove .py
                    module_path = '.'.join(module_parts)

                    if category not in modules_by_category:
                        modules_by_category[category] = []

                    modules_by_category[category].append(module_path)

        return modules_by_category

    def get_classes_and_functions(self, module_path: str) -> Dict[str, List[str]]:
        """Extract classes and functions from a Python module"""
        # Convert module path to file path
        relative_path = module_path.replace("sqlstream.", "").replace(".", "/")
        file_path = self.source_dir / f"{relative_path}.py"

        if not file_path.exists():
            return {"classes": [], "functions": []}

        with open(file_path, 'r') as f:
            try:
                tree = ast.parse(f.read())
            except SyntaxError:
                return {"classes": [], "functions": []}

        classes = []
        functions = []

        # Only process top-level definitions (direct children of Module)
        for node in tree.body:
            if isinstance(node, ast.ClassDef):
                classes.append(node.name)
            elif isinstance(node, ast.FunctionDef):
                # Skip private functions
                if not node.name.startswith('_'):
                    functions.append(node.name)

        return {"classes": classes, "functions": functions}

    def generate_mkdocstrings_block(self, module_path: str, item_name: str, item_type: str = "class") -> str:
        """Generate mkdocstrings syntax for a class or function"""
        full_path = f"{module_path}.{item_name}"

        return f"""
## {item_name}

::: {full_path}
    options:
      show_root_heading: true
      show_source: true
"""

    def generate_category_doc(self, category_name: str, modules: List[str]) -> str:
        """Generate markdown file for a category"""
        title = self.category_titles.get(category_name, f"{category_name.title()} Reference")
        description = self.category_descriptions.get(category_name, f"Auto-generated API documentation for {category_name}.")

        lines = [f"# {title}\n"]
        lines.append(f"{description}\n")

        # Process each module in the category
        for module_path in sorted(modules):
            items = self.get_classes_and_functions(module_path)

            # Skip modules with no public classes or functions
            if not items['classes'] and not items['functions']:
                continue

            # Add classes
            for class_name in sorted(items['classes']):
                lines.append(self.generate_mkdocstrings_block(module_path, class_name, "class"))

            # Add functions
            for func_name in sorted(items['functions']):
                lines.append(self.generate_mkdocstrings_block(module_path, func_name, "function"))

        return "\n".join(lines)

    def update_mkdocs_nav(self, modules_by_category: Dict[str, List[str]], mkdocs_file: str = "mkdocs.yml"):
        """Update mkdocs.yml navigation with generated API reference pages using text manipulation"""
        mkdocs_path = Path(mkdocs_file)

        if not mkdocs_path.exists():
            print(f"Warning: {mkdocs_file} not found, skipping navigation update")
            return

        # Build the API Reference navigation structure
        api_ref_lines = ["      - API Reference:"]
        for category_name in sorted(modules_by_category.keys()):
            title = self.category_titles.get(category_name, f"{category_name.title()}")
            # Remove " Reference" suffix for cleaner nav
            title = title.replace(" Reference", "")
            api_ref_lines.append(f"          - {title}: api/reference/{category_name}.md")

        api_ref_section = "\n".join(api_ref_lines)

        # Read the current mkdocs.yml
        with open(mkdocs_path, 'r') as f:
            content = f.read()

        # Replace the Python API section with updated navigation
        # We'll look for the pattern and replace what's between API Reference and Guides

        pattern = r'(  - Python API:\s+- Overview: api/overview.md)(.*?)(      - Guides:)'

        replacement_section = f'''\\1
{api_ref_section}
\\3'''

        new_content = re.sub(pattern, replacement_section, content, flags=re.DOTALL)

        # Write back
        with open(mkdocs_path, 'w') as f:
            f.write(new_content)

        print(f"\n✓ Updated navigation in {mkdocs_file} with {len(modules_by_category)} API reference pages")


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Generate API reference documentation')
    parser.add_argument('--skip-nav-update', action='store_true',
                        help='Skip updating mkdocs.yml navigation (for CI/CD builds)')
    args = parser.parse_args()

    generator = APIDocGenerator()

    # Discover modules first
    print("Discovering modules...")
    modules_by_category = generator.discover_modules()

    # Special handling for 'core' - split into query and types
    if 'core' in modules_by_category:
        core_modules = modules_by_category.pop('core')
        query_modules = [m for m in core_modules if 'query' in m]
        type_modules = [m for m in core_modules if 'types' in m]

        if query_modules:
            modules_by_category['query'] = query_modules
        if type_modules:
            modules_by_category['types'] = type_modules

    # Generate docs
    print("\nGenerating API reference documentation...")
    for category_name, modules in sorted(modules_by_category.items()):
        output_file = generator.docs_dir / f"{category_name}.md"
        content = generator.generate_category_doc(category_name, modules)

        with open(output_file, 'w') as f:
            f.write(content)

        print(f"  ✓ Generated {output_file} ({len(modules)} modules)")

    print(f"\nGenerated {len(modules_by_category)} API reference files in {generator.docs_dir}")

    # Update mkdocs.yml navigation (only if not skipped)
    if not args.skip_nav_update:
        generator.update_mkdocs_nav(modules_by_category)
    else:
        print("\n⊘ Skipped navigation update (--skip-nav-update flag set)")

    print("\nDone! You can now run 'mkdocs build' or 'mkdocs serve' to view the documentation.")


if __name__ == "__main__":
    main()
