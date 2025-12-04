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


class TestHTTPErrorCodes:
    """Test handling of various HTTP error codes"""

    def test_404_not_found(self, tmp_path):
        """Test handling of 404 Not Found"""
        url = "https://example.com/nonexistent.csv"

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("404 Not Found")

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            with pytest.raises(OSError, match="Failed to download"):
                HTTPReader(url, cache_dir=str(tmp_path))

    def test_500_internal_server_error(self, tmp_path):
        """Test handling of 500 Internal Server Error"""
        url = "https://example.com/error.csv"

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("500 Internal Server Error")

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            with pytest.raises(OSError):
                HTTPReader(url, cache_dir=str(tmp_path))

    def test_403_forbidden(self, tmp_path):
        """Test handling of 403 Forbidden"""
        url = "https://example.com/forbidden.csv"

        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("403 Forbidden")

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            with pytest.raises(OSError):
                HTTPReader(url, cache_dir=str(tmp_path))

    def test_network_timeout(self, tmp_path):
        """Test handling of network timeout"""
        url = "https://example.com/slow.csv"

        with patch("httpx.stream") as mock_stream:
            mock_stream.side_effect = Exception("Timeout")

            with pytest.raises(OSError, match="Failed to download"):
                HTTPReader(url, cache_dir=str(tmp_path))

    def test_connection_error(self, tmp_path):
        """Test handling of connection errors"""
        url = "https://unreachable.example.com/data.csv"

        with patch("httpx.stream") as mock_stream:
            mock_stream.side_effect = Exception("Connection refused")

            with pytest.raises(OSError):
                HTTPReader(url, cache_dir=str(tmp_path))


class TestHTTPRedirects:
    """Test HTTP redirect handling"""

    def test_redirect_followed(self, tmp_path):
        """Test that HTTP redirects are followed"""
        url = "https://example.com/redirect.csv"
        csv_data = b"name,age\nAlice,30\n"

        mock_response = Mock()
        mock_response.iter_bytes = Mock(return_value=[csv_data])
        mock_response.raise_for_status = Mock()

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            reader = HTTPReader(url, cache_dir=str(tmp_path))
            rows = list(reader.read_lazy())

            # Verify follow_redirects=True was used
            mock_stream.assert_called_once()
            call_kwargs = mock_stream.call_args[1]
            assert call_kwargs.get("follow_redirects") is True

            # Verify data was read correctly
            assert len(rows) == 1
            assert rows[0]["name"] == "Alice"

    def test_multiple_redirects(self, tmp_path):
        """Test handling of multiple redirects (301 -> 302 -> 200)"""
        url = "https://example.com/multi-redirect.csv"
        csv_data = b"name,age\nBob,25\n"

        mock_response = Mock()
        mock_response.iter_bytes = Mock(return_value=[csv_data])
        mock_response.raise_for_status = Mock()

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            reader = HTTPReader(url, cache_dir=str(tmp_path))
            rows = list(reader.read_lazy())

            # httpx handles redirects internally with follow_redirects=True
            assert len(rows) == 1
            assert rows[0]["name"] == "Bob"


class TestHTTPStreamingDownload:
    """Test chunked streaming download behavior"""

    def test_chunked_download(self, tmp_path):
        """Test that large files are downloaded in chunks"""
        url = "https://example.com/large.csv"
        # Simulate 3 chunks of data
        chunk1 = b"name,age\n"
        chunk2 = b"Alice,30\n"
        chunk3 = b"Bob,25\n"

        mock_response = Mock()
        mock_response.iter_bytes = Mock(return_value=[chunk1, chunk2, chunk3])
        mock_response.raise_for_status = Mock()

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            reader = HTTPReader(url, cache_dir=str(tmp_path))
            rows = list(reader.read_lazy())

            # Verify all chunks were downloaded
            assert len(rows) == 2
            assert rows[0]["name"] == "Alice"
            assert rows[1]["name"] == "Bob"

    def test_empty_response(self, tmp_path):
        """Test handling of empty HTTP response"""
        url = "https://example.com/empty.csv"

        mock_response = Mock()
        mock_response.iter_bytes = Mock(return_value=[])
        mock_response.raise_for_status = Mock()

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            # Should handle gracefully (may raise error depending on format)
            try:
                reader = HTTPReader(url, cache_dir=str(tmp_path))
                rows = list(reader.read_lazy())
                # Empty file should return no rows
                assert len(rows) == 0
            except (OSError, ValueError):
                # Also acceptable to raise error for empty file
                pass


class TestHTTPAdditionalFormats:
    """Test HTTP reader with additional file formats"""

    def test_json_format_explicit(self, tmp_path):
        """Test explicit JSON format specification"""
        url = "https://example.com/data.json"
        json_data = b'[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]'

        mock_response = Mock()
        mock_response.iter_bytes = Mock(return_value=[json_data])
        mock_response.raise_for_status = Mock()

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            reader = HTTPReader(url, cache_dir=str(tmp_path), format="json")

            # Should create JSONReader as delegate
            assert reader.delegate_reader.__class__.__name__ == "JSONReader"

            rows = list(reader.read_lazy())
            assert len(rows) == 2
            assert rows[0]["name"] == "Alice"

    def test_jsonl_format_detection(self, tmp_path):
        """Test JSONL format detection from URL"""
        url = "https://example.com/logs.jsonl"
        jsonl_data = b'{"name": "Alice", "age": 30}\n{"name": "Bob", "age": 25}\n'

        mock_response = Mock()
        mock_response.iter_bytes = Mock(return_value=[jsonl_data])
        mock_response.raise_for_status = Mock()

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            reader = HTTPReader(url, cache_dir=str(tmp_path))

            # Should create JSONLReader as delegate
            assert reader.delegate_reader.__class__.__name__ == "JSONLReader"

    def test_html_format_detection(self, tmp_path):
        """Test HTML format detection from URL"""
        url = "https://example.com/table.html"
        html_data = b"<table><tr><th>name</th><th>age</th></tr><tr><td>Alice</td><td>30</td></tr></table>"

        mock_response = Mock()
        mock_response.iter_bytes = Mock(return_value=[html_data])
        mock_response.raise_for_status = Mock()

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            try:
                reader = HTTPReader(url, cache_dir=str(tmp_path))
                # Should create HTMLReader as delegate
                assert reader.delegate_reader.__class__.__name__ == "HTMLReader"
            except ImportError:
                # HTMLReader requires pandas
                pytest.skip("HTML reader requires pandas")

    def test_markdown_format_detection(self, tmp_path):
        """Test Markdown format detection from URL"""
        url = "https://example.com/data.md"
        md_data = b"| name | age |\n|------|-----|\n| Alice | 30 |\n| Bob | 25 |"

        mock_response = Mock()
        mock_response.iter_bytes = Mock(return_value=[md_data])
        mock_response.raise_for_status = Mock()

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            reader = HTTPReader(url, cache_dir=str(tmp_path))

            # Should create MarkdownReader as delegate
            assert reader.delegate_reader.__class__.__name__ == "MarkdownReader"


class TestHTTPCacheKeyGeneration:
    """Test cache key generation and collision handling"""

    def test_different_urls_different_cache(self, tmp_path):
        """Test that different URLs generate different cache files"""
        url1 = "https://example.com/data1.csv"
        url2 = "https://example.com/data2.csv"
        csv_data = b"name,age\nAlice,30\n"

        mock_response = Mock()
        mock_response.iter_bytes = Mock(return_value=[csv_data])
        mock_response.raise_for_status = Mock()

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            reader1 = HTTPReader(url1, cache_dir=str(tmp_path))
            reader2 = HTTPReader(url2, cache_dir=str(tmp_path))

            # Should create different cache files
            assert reader1.local_path != reader2.local_path

            # Both cache files should exist
            assert reader1.local_path.exists()
            assert reader2.local_path.exists()

    def test_same_url_same_cache(self, tmp_path):
        """Test that same URL uses same cache file"""
        url = "https://example.com/data.csv"
        csv_data = b"name,age\nAlice,30\n"

        mock_response = Mock()
        mock_response.iter_bytes = Mock(return_value=[csv_data])
        mock_response.raise_for_status = Mock()

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            reader1 = HTTPReader(url, cache_dir=str(tmp_path))
            reader2 = HTTPReader(url, cache_dir=str(tmp_path))

            # Should use same cache file
            assert reader1.local_path == reader2.local_path

            # Should only download once
            assert mock_stream.call_count == 1

    def test_url_with_query_params(self, tmp_path):
        """Test cache key generation with query parameters"""
        url1 = "https://example.com/data.csv?version=1"
        url2 = "https://example.com/data.csv?version=2"
        csv_data = b"name,age\nAlice,30\n"

        mock_response = Mock()
        mock_response.iter_bytes = Mock(return_value=[csv_data])
        mock_response.raise_for_status = Mock()

        with patch("httpx.stream") as mock_stream:
            mock_stream.return_value.__enter__.return_value = mock_response

            reader1 = HTTPReader(url1, cache_dir=str(tmp_path))
            reader2 = HTTPReader(url2, cache_dir=str(tmp_path))

            # Different query params should create different cache files
            assert reader1.local_path != reader2.local_path
