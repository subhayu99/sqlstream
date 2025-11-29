"""
Base formatter interface for CLI output

All formatters must implement the format() method.
"""

from typing import Any, Dict, List


class BaseFormatter:
    """Base class for all output formatters"""

    def format(self, results: List[Dict[str, Any]], **kwargs) -> str:
        """
        Format query results for output

        Args:
            results: List of result dictionaries
            **kwargs: Additional formatter-specific options

        Returns:
            Formatted string ready for output
        """
        raise NotImplementedError("Formatters must implement format() method")

    def get_name(self) -> str:
        """Get formatter name"""
        return self.__class__.__name__.replace("Formatter", "").lower()
