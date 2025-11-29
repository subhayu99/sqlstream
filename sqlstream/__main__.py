"""
SQLStream - Main entry point for CLI

When running as: python -m sqlstream
"""

if __name__ == "__main__":
    from sqlstream.cli.main import cli

    cli()
