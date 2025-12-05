"""
JSON formatter for machine-readable output
"""

import json
import math
from typing import Any

from sqlstream.cli.formatters.base import BaseFormatter


class JSONFormatter(BaseFormatter):
    """Format results as JSON"""

    def format(self, results: list[dict[str, Any]], **kwargs) -> str:
        """
        Format results as JSON

        Args:
            results: List of result dictionaries
            **kwargs: Options like 'compact', 'indent'

        Returns:
            JSON string
        """

        # Handle NaN and infinity values (convert to null)
        def clean_value(val):
            if isinstance(val, float):
                if math.isnan(val) or math.isinf(val):
                    return None
            return val

        # Clean all values
        cleaned_results = [{k: clean_value(v) for k, v in row.items()} for row in results]

        # Format as JSON
        if kwargs.get("compact", False):
            return json.dumps(cleaned_results, separators=(",", ":"))
        else:
            indent = kwargs.get("indent", 2)
            return json.dumps(cleaned_results, indent=indent)
