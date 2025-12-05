"""
Markdown formatter for documentation and sharing
"""

from typing import Any

from sqlstream.cli.formatters.base import BaseFormatter


class MarkdownFormatter(BaseFormatter):
    """Format results as a Markdown table"""

    def format(self, results: list[dict[str, Any]], **kwargs) -> str:
        """
        Format results as a Markdown table

        Args:
            results: List of result dictionaries
            **kwargs: Options like 'show_footer', 'align'

        Returns:
            Markdown formatted table string
        """
        if not results:
            return "_No results found._"

        # Get columns from first row
        columns = list(results[0].keys())

        # Build header row
        header = "| " + " | ".join(columns) + " |"

        # Build separator row with alignment
        # Default alignment is left, can be 'left', 'center', or 'right'
        align = kwargs.get("align", "left")
        separators = []
        for col in columns:
            col_align = align if isinstance(align, str) else align.get(col, "left")
            if col_align == "center":
                separators.append(":---:")
            elif col_align == "right":
                separators.append("---:")
            else:  # left or default
                separators.append(":---")

        separator = "| " + " | ".join(separators) + " |"

        # Build data rows
        data_rows = []
        for row in results:
            # Format each cell value
            values = []
            for col in columns:
                val = row[col]
                if val is None:
                    formatted_val = "_NULL_"
                elif isinstance(val, str):
                    # Escape pipe characters in strings
                    formatted_val = str(val).replace("|", "\\|")
                else:
                    formatted_val = str(val)
                values.append(formatted_val)

            data_rows.append("| " + " | ".join(values) + " |")

        # Combine all parts
        table_lines = [header, separator] + data_rows
        output = "\n".join(table_lines)

        # Add footer with row count if requested
        if kwargs.get("show_footer", True):
            row_count = len(results)
            footer = f"\n\n_{row_count} row{'s' if row_count != 1 else ''}_"
            output += footer

        return output
