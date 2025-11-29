"""
CSV formatter for Unix-friendly output
"""

import csv
import io
from typing import Any, Dict, List

from sqlstream.cli.formatters.base import BaseFormatter


class CSVFormatter(BaseFormatter):
    """Format results as CSV"""

    def format(self, results: List[Dict[str, Any]], **kwargs) -> str:
        """
        Format results as CSV

        Args:
            results: List of result dictionaries
            **kwargs: Options like 'delimiter', 'quote_all'

        Returns:
            CSV string
        """
        if not results:
            return ""

        # Get column names from first row
        columns = list(results[0].keys())

        # Create CSV in memory
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=columns,
            delimiter=kwargs.get("delimiter", ","),
            quoting=csv.QUOTE_MINIMAL if not kwargs.get("quote_all") else csv.QUOTE_ALL,
        )

        # Write header and rows
        writer.writeheader()
        writer.writerows(results)

        return output.getvalue()
