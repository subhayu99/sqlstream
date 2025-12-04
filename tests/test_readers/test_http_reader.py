"""
Tests for HTTP reader with streaming and caching
"""

from unittest.mock import Mock, patch

import pytest

from sqlstream import query
from sqlstream.readers.http_reader import HTTPReader


class TestHTTPReaderBasic:
    """Test basic HTTP reader functionality"""

    @pytest.fixture
    def mock_csv_response(self):
        """Create mock HTTP response with CSV data"""
        csv_data = b"name,age,city\nAlice,30,NYC\nBob,25,LA\n"

        mock_response = Mock()
        mock_response.iter_bytes = Mock(return_value=[csv_data])
        mock_response.raise_for_status = Mock()
        return mock_response

    def test_download_and_cache_csv(self, mock_csv_response, tmp_path):
        """Test downloading and caching CSV file"""
        url = "https://example.com/data.csv"

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_csv_response

            # First request - should download
            reader = HTTPReader(url, cache_dir=str(tmp_path))
            rows = list(reader.read_lazy())

            assert len(rows) == 2
            assert rows[0]["name"] == "Alice"
            assert rows[0]["age"] == 30

            # Verify cache file exists
            cache_files = list(tmp_path.glob("*_data.csv"))
            assert len(cache_files) == 1

    def test_use_cached_file(self, mock_csv_response, tmp_path):
        """Test that cached files are reused"""
        url = "https://example.com/data.csv"

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_csv_response

            # First request - downloads
            reader1 = HTTPReader(url, cache_dir=str(tmp_path))
            list(reader1.read_lazy())

            # Second request - should use cache (no new download)
            reader2 = HTTPReader(url, cache_dir=str(tmp_path))
            rows = list(reader2.read_lazy())

            # httpx.stream should only be called once (first download)
            assert mock_stream.call_count == 1
            assert len(rows) == 2

    def test_force_download(self, mock_csv_response, tmp_path):
        """Test force_download parameter"""
        url = "https://example.com/data.csv"

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_csv_response

            # First request
            reader1 = HTTPReader(url, cache_dir=str(tmp_path))
            list(reader1.read_lazy())

            # Second request with force_download=True
            reader2 = HTTPReader(url, cache_dir=str(tmp_path), force_download=True)
            list(reader2.read_lazy())

            # Should download twice
            assert mock_stream.call_count == 2

    def test_clear_cache(self, mock_csv_response, tmp_path):
        """Test clearing cached file"""
        url = "https://example.com/data.csv"

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_csv_response

            reader = HTTPReader(url, cache_dir=str(tmp_path))
            list(reader.read_lazy())

            # Verify cache exists
            cache_files = list(tmp_path.glob("*"))
            assert len(cache_files) == 1

            # Clear cache
            reader.clear_cache()

            # Verify cache is gone
            cache_files = list(tmp_path.glob("*"))
            assert len(cache_files) == 0

    def test_clear_all_cache(self, mock_csv_response, tmp_path):
        """Test clearing all cached files"""
        url1 = "https://example.com/data1.csv"
        url2 = "https://example.com/data2.csv"

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_csv_response

            # Download two files
            reader1 = HTTPReader(url1, cache_dir=str(tmp_path))
            reader2 = HTTPReader(url2, cache_dir=str(tmp_path))
            list(reader1.read_lazy())
            list(reader2.read_lazy())

            # Verify both cached
            cache_files = list(tmp_path.glob("*"))
            assert len(cache_files) == 2

            # Clear all
            count = HTTPReader.clear_all_cache(cache_dir=str(tmp_path))
            assert count == 2

            # Verify all gone
            cache_files = list(tmp_path.glob("*"))
            assert len(cache_files) == 0


class TestHTTPReaderFormats:
    """Test HTTP reader with different file formats"""

    def test_csv_format_detection(self, tmp_path):
        """Test CSV format detection from URL"""
        url = "https://example.com/data.csv"
        csv_data = b"name,age\nAlice,30\n"

        mock_response = Mock()
        mock_response.iter_bytes = Mock(return_value=[csv_data])
        mock_response.raise_for_status = Mock()

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            reader = HTTPReader(url, cache_dir=str(tmp_path))

            # Should create CSVReader as delegate
            assert reader.delegate_reader.__class__.__name__ == "CSVReader"

    def test_parquet_format_detection(self, tmp_path):
        """Test Parquet format detection from URL"""
        # Create a real parquet file
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq

            # Create temp parquet file
            data = {"name": ["Alice"], "age": [30]}
            table = pa.table(data)
            parquet_path = tmp_path / "test.parquet"
            pq.write_table(table, parquet_path)

            # Read file bytes
            with open(parquet_path, "rb") as f:
                parquet_data = f.read()

            url = "https://example.com/data.parquet"
            mock_response = Mock()
            mock_response.iter_bytes = Mock(return_value=[parquet_data])
            mock_response.raise_for_status = Mock()

            with patch("httpx.stream") as mock_stream:
                mock_stream.return_value.__enter__.return_value = mock_response

                reader = HTTPReader(url, cache_dir=str(tmp_path / "cache"))

                # Should create ParquetReader as delegate
                assert reader.delegate_reader.__class__.__name__ == "ParquetReader"

        except ImportError:
            pytest.skip("Parquet tests require pyarrow")


class TestHTTPReaderWithQuery:
    """Test HTTP reader through query API"""

    def test_query_http_csv(self, tmp_path):
        """Test querying CSV from HTTP URL"""
        url = "https://example.com/data.csv"
        csv_data = b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,SF\n"

        mock_response = Mock()
        mock_response.iter_bytes = Mock(return_value=[csv_data])
        mock_response.raise_for_status = Mock()

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            # Query HTTP URL
            results = query(url).sql("SELECT * FROM data WHERE age > 25").to_list()

            assert len(results) == 2
            assert results[0]["name"] == "Alice"
            assert results[1]["name"] == "Charlie"

    def test_query_http_with_filters(self, tmp_path):
        """Test querying HTTP URL with filters (predicate pushdown)"""
        url = "https://example.com/data.csv"
        csv_data = b"name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,SF\n"

        mock_response = Mock()
        mock_response.iter_bytes = Mock(return_value=[csv_data])
        mock_response.raise_for_status = Mock()

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            # Query with WHERE clause
            results = query(url).sql(
                "SELECT name, city FROM data WHERE city = 'NYC'"
            ).to_list()

            assert len(results) == 1
            assert results[0]["name"] == "Alice"
            assert set(results[0].keys()) == {"name", "city"}


class TestHTTPReaderErrorHandling:
    """Test HTTP reader error handling"""

    def test_download_failure(self, tmp_path):
        """Test handling of download failures"""
        url = "https://example.com/nonexistent.csv"

        with patch("httpx.stream") as mock_stream:
            mock_stream.side_effect = Exception("Network error")

            with pytest.raises(IOError, match="Failed to download"):
                HTTPReader(url, cache_dir=str(tmp_path))

    def test_http_error_status(self, tmp_path):
        """Test handling of HTTP error statuses"""
        url = "https://example.com/forbidden.csv"

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("404 Not Found")

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            with pytest.raises(IOError):
                HTTPReader(url, cache_dir=str(tmp_path))


class TestHTTPReaderOptimizations:
    """Test that HTTP reader delegates optimizations"""

    def test_predicate_pushdown_delegation(self, tmp_path):
        """Test that predicate pushdown is delegated to underlying reader"""
        url = "https://example.com/data.csv"
        csv_data = b"name,age\nAlice,30\nBob,25\n"

        mock_response = Mock()
        mock_response.iter_bytes = Mock(return_value=[csv_data])
        mock_response.raise_for_status = Mock()

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            reader = HTTPReader(url, cache_dir=str(tmp_path))

            # HTTP reader should support pushdown via delegation
            assert reader.supports_pushdown() is True

    def test_column_selection_delegation(self, tmp_path):
        """Test that column selection is delegated"""
        url = "https://example.com/data.csv"
        csv_data = b"name,age,city\nAlice,30,NYC\n"

        mock_response = Mock()
        mock_response.iter_bytes = Mock(return_value=[csv_data])
        mock_response.raise_for_status = Mock()

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            reader = HTTPReader(url, cache_dir=str(tmp_path))

            # HTTP reader should support column selection via delegation
            assert reader.supports_column_selection() is True
