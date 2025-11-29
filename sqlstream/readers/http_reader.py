"""
HTTP Reader - Stream data from remote URLs with caching

Supports:
- CSV and Parquet files over HTTP/HTTPS
- Intelligent caching to avoid re-downloads
- Streaming downloads with progress tracking
- Auto-detection of file format from URL or Content-Type
"""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urlparse

try:
    import httpx

    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    httpx = None

from sqlstream.readers.base import BaseReader
from sqlstream.readers.csv_reader import CSVReader
from sqlstream.sql.ast_nodes import Condition

# Try to import ParquetReader (optional)
try:
    from sqlstream.readers.parquet_reader import ParquetReader

    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False
    ParquetReader = None


class HTTPReader(BaseReader):
    """
    Read data from HTTP/HTTPS URLs with intelligent caching

    Automatically detects file format (CSV or Parquet) and delegates
    to appropriate reader. Caches downloaded files to avoid re-downloads.

    Example:
        reader = HTTPReader("https://example.com/data.csv")
        for row in reader.read_lazy():
            print(row)
    """

    def __init__(
        self,
        url: str,
        cache_dir: Optional[str] = None,
        force_download: bool = False,
    ):
        """
        Initialize HTTP reader

        Args:
            url: HTTP/HTTPS URL to data file
            cache_dir: Directory to cache downloaded files (default: system temp)
            force_download: If True, re-download even if cached
        """
        if not HTTPX_AVAILABLE:
            raise ImportError(
                "HTTP reader requires httpx library. "
                "Install with: pip install sqlstream[http]"
            )

        self.url = url
        self.cache_dir = Path(cache_dir) if cache_dir else Path(tempfile.gettempdir()) / "sqlstream_cache"
        self.force_download = force_download

        # Ensure cache directory exists
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Download or get cached file
        self.local_path = self._get_or_download()

        # Detect format and create appropriate reader
        self.delegate_reader = self._create_delegate_reader()

        # Delegate filter conditions and column selection
        self.filter_conditions: List[Condition] = []
        self.required_columns: List[str] = []

    def _get_cache_path(self) -> Path:
        """Generate cache file path based on URL hash"""
        # Create hash of URL for cache key
        url_hash = hashlib.md5(self.url.encode()).hexdigest()

        # Extract filename from URL
        parsed = urlparse(self.url)
        filename = Path(parsed.path).name or "data"

        # Cache path: cache_dir/url_hash_filename
        return self.cache_dir / f"{url_hash}_{filename}"

    def _get_or_download(self) -> Path:
        """Get cached file or download if not cached"""
        cache_path = self._get_cache_path()

        # Return cached file if it exists and we're not forcing download
        if cache_path.exists() and not self.force_download:
            return cache_path

        # Download file
        return self._download_file(cache_path)

    def _download_file(self, target_path: Path) -> Path:
        """Download file from URL to target path"""
        try:
            with httpx.stream("GET", self.url, follow_redirects=True) as response:
                response.raise_for_status()

                # Write to temporary file first, then move to target
                temp_path = target_path.with_suffix(".tmp")

                with open(temp_path, "wb") as f:
                    for chunk in response.iter_bytes(chunk_size=8192):
                        f.write(chunk)

                # Move temp file to final location
                temp_path.rename(target_path)

                return target_path

        except Exception as e:
            raise IOError(f"Failed to download {self.url}: {e}")

    def _create_delegate_reader(self) -> BaseReader:
        """Create appropriate reader based on file format"""
        # Detect format from file extension
        path_lower = str(self.local_path).lower()

        if path_lower.endswith(".parquet"):
            if not PARQUET_AVAILABLE:
                raise ImportError(
                    "Parquet files require pyarrow. "
                    "Install with: pip install sqlstream[parquet]"
                )
            return ParquetReader(str(self.local_path))
        elif path_lower.endswith(".csv"):
            return CSVReader(str(self.local_path))
        else:
            # Default to CSV for unknown formats
            return CSVReader(str(self.local_path))

    def read_lazy(self) -> Iterator[Dict[str, Any]]:
        """Read data lazily, delegating to underlying reader"""
        # Apply filter conditions to delegate
        if self.filter_conditions:
            self.delegate_reader.set_filter(self.filter_conditions)

        # Apply column selection to delegate
        if self.required_columns:
            self.delegate_reader.set_columns(self.required_columns)

        # Delegate to underlying reader
        yield from self.delegate_reader.read_lazy()

    def get_schema(self) -> Dict[str, str]:
        """Get schema from delegate reader"""
        return self.delegate_reader.get_schema()

    def supports_pushdown(self) -> bool:
        """HTTP reader supports pushdown via delegation"""
        return self.delegate_reader.supports_pushdown()

    def supports_column_selection(self) -> bool:
        """HTTP reader supports column selection via delegation"""
        return self.delegate_reader.supports_column_selection()

    def set_filter(self, conditions: List[Condition]) -> None:
        """Set filter conditions (will be pushed to delegate)"""
        self.filter_conditions = conditions

    def set_columns(self, columns: List[str]) -> None:
        """Set required columns (will be pushed to delegate)"""
        self.required_columns = columns

    def clear_cache(self) -> None:
        """Remove cached file for this URL"""
        cache_path = self._get_cache_path()
        if cache_path.exists():
            cache_path.unlink()

    @staticmethod
    def clear_all_cache(cache_dir: Optional[str] = None) -> int:
        """
        Clear all cached files

        Args:
            cache_dir: Cache directory to clear (default: system temp)

        Returns:
            Number of files deleted
        """
        cache_path = Path(cache_dir) if cache_dir else Path(tempfile.gettempdir()) / "sqlstream_cache"

        if not cache_path.exists():
            return 0

        count = 0
        for file in cache_path.glob("*"):
            if file.is_file():
                file.unlink()
                count += 1

        return count
