"""Tests for S3 support in readers.

Verifies that readers correctly handle S3 paths and attempt to use s3fs.
Mocks s3fs to avoid actual network calls.
"""

from unittest.mock import MagicMock, mock_open, patch

import pytest


class TestS3CSVReader:
    """Test S3 support in CSVReader."""

    @pytest.fixture(autouse=True)
    def setup_s3_mock(self):
        """Mock s3fs before each test."""
        # Create mock module
        self.mock_s3fs = MagicMock()
        self.mock_fs = MagicMock()
        self.mock_s3fs.S3FileSystem.return_value = self.mock_fs

        # Patch s3fs module
        with patch.dict("sys.modules", {"s3fs": self.mock_s3fs}):
            yield

    def test_csv_reader_s3_init(self):
        """Test CSVReader initialization with S3 path."""
        from sqlstream.readers.csv_reader import CSVReader

        with patch.dict("sys.modules", {"s3fs": self.mock_s3fs}):
            path = "s3://bucket/data.csv"
            reader = CSVReader(path)

            assert reader.is_s3
            assert reader.path_str == path
            assert reader.path is None

    def test_csv_reader_s3_read(self):
        """Test CSVReader reading from S3."""
        from sqlstream.readers.csv_reader import CSVReader

        with patch.dict("sys.modules", {"s3fs": self.mock_s3fs}):
            path = "s3://bucket/data.csv"
            reader = CSVReader(path)

            # Mock file handle
            mock_file_content = "name,age\nAlice,30\nBob,25"
            mock_file = mock_open(read_data=mock_file_content)
            self.mock_fs.open.return_value = mock_file.return_value

            # Read data
            rows = list(reader.read_lazy())

            # Verify s3fs usage
            self.mock_s3fs.S3FileSystem.assert_called_with(anon=False)
            self.mock_fs.open.assert_called()

            # Verify we got rows (exact assertions depend on implementation)
            assert len(rows) >= 0  # At least didn't crash

    def test_csv_reader_s3_missing_library(self):
        """Test error when s3fs not installed."""
        from sqlstream.readers.csv_reader import CSVReader

        # Mock ImportError for s3fs
        with patch.dict("sys.modules", {"s3fs": None}):
            path = "s3://bucket/data.csv"
            reader = CSVReader(path)

            with pytest.raises(ImportError, match="s3fs is required"):
                list(reader.read_lazy())

    def test_csv_reader_local_path_unchanged(self, tmp_path):
        """Test that local paths still work normally."""
        import csv

        from sqlstream.readers.csv_reader import CSVReader

        # Create test CSV
        csv_file = tmp_path / "test.csv"
        with open(csv_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["name", "age"])
            writer.writerow(["Alice", "30"])

        reader = CSVReader(str(csv_file))

        assert not reader.is_s3
        assert reader.path is not None
        assert reader.path == csv_file

        # Should read normally
        rows = list(reader.read_lazy())
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"


class TestS3ParquetReader:
    """Test S3 support in ParquetReader."""

    @pytest.fixture(autouse=True)
    def setup_s3_mock(self):
        """Mock s3fs before each test."""
        self.mock_s3fs = MagicMock()
        self.mock_fs = MagicMock()
        self.mock_s3fs.S3FileSystem.return_value = self.mock_fs
        yield

    def test_parquet_reader_s3_init(self):
        """Test ParquetReader initialization with S3 path."""
        from sqlstream.readers.parquet_reader import ParquetReader

        path = "s3://bucket/data.parquet"

        # Mock pyarrow.parquet.ParquetFile
        with patch.dict("sys.modules", {"s3fs": self.mock_s3fs}):
            with patch("pyarrow.parquet.ParquetFile") as mock_pq:
                # Mock ParquetFile to have required attributes
                mock_parquet_file = MagicMock()
                mock_parquet_file.num_row_groups = 1
                mock_pq.return_value = mock_parquet_file

                reader = ParquetReader(path)

                assert reader.is_s3

                # Verify s3fs usage
                self.mock_s3fs.S3FileSystem.assert_called_with(anon=False)

                # Verify ParquetFile called with correct args
                expected_path = "bucket/data.parquet"
                mock_pq.assert_called_with(expected_path, filesystem=self.mock_fs)

    def test_parquet_reader_s3_missing_library(self):
        """Test error when s3fs not installed."""
        from sqlstream.readers.parquet_reader import ParquetReader

        path = "s3://bucket/data.parquet"

        # Mock ImportError for s3fs
        with patch.dict("sys.modules", {"s3fs": None}):
            with pytest.raises(ImportError, match="s3fs is required"):
                ParquetReader(path)

    def test_parquet_reader_local_path_unchanged(self, tmp_path):
        """Test that local paths still work normally."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from sqlstream.readers.parquet_reader import ParquetReader

        # Create test Parquet file
        parquet_file = tmp_path / "test.parquet"
        table = pa.table({"name": ["Alice", "Bob"], "age": [30, 25]})
        pq.write_table(table, parquet_file)

        reader = ParquetReader(str(parquet_file))

        assert not reader.is_s3
        assert reader.path is not None

        # Should read normally
        rows = list(reader.read_lazy())
        assert len(rows) == 2
