"""
Interactive TUI mode (Coming Soon!)

This module will contain a Textual-based interactive query interface.

Future features:
- Live query editing with syntax highlighting
- Interactive result browsing with arrow keys
- Export functionality
- Query history
- Schema browser
"""


def launch_interactive(source: str):
    """
    Launch Textual-based interactive TUI

    Args:
        source: Path to data file or URL

    Raises:
        NotImplementedError: Feature not yet implemented
    """
    try:
        import textual  # noqa: F401

        raise NotImplementedError(
            "Interactive mode is coming soon! "
            "We're working on a beautiful Textual-based TUI."
        )
    except ImportError:
        raise ImportError(
            "Interactive mode requires textual library. "
            "Install with: pip install sqlstream[interactive]"
        )
