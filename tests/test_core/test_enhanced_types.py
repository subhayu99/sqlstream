"""Tests for enhanced type system with DATETIME, TIME, DECIMAL, and JSON support."""

import pytest
from datetime import datetime, date, time
from decimal import Decimal
from sqlstream.core.types import (
    DataType,
    infer_type,
    infer_type_from_string,
    parse_datetime,
    parse_date,
    parse_time,
    is_json_string,
    infer_common_type,
)


class TestDatetimeParsing:
    """Test datetime parsing with multiple formats."""
    
    def test_iso_8601_datetime(self):
        dt = parse_datetime("2024-01-15T10:30:00")
        assert dt == datetime(2024, 1, 15, 10, 30, 0)
    
    def test_iso_8601_with_microseconds(self):
        dt = parse_datetime("2024-01-15T10:30:00.123456")
        assert dt == datetime(2024, 1, 15, 10, 30, 0, 123456)
    
    def test_sql_format_datetime(self):
        dt = parse_datetime("2024-01-15 10:30:00")
        assert dt == datetime(2024, 1, 15, 10, 30, 0)
    
    def test_sql_with_microseconds(self):
        dt = parse_datetime("2024-01-15 10:30:00.123")
        assert dt.year == 2024
        assert dt.microsecond > 0
    
    def test_eu_format_datetime(self):
        dt = parse_datetime("15/01/2024 10:30:00")
        assert dt == datetime(2024, 1, 15, 10, 30, 0)
    
    def test_us_format_datetime(self):
        dt = parse_datetime("01/15/2024 10:30:00")
        assert dt == datetime(2024, 1, 15, 10, 30, 0)
    
    def test_compact_datetime(self):
        dt = parse_datetime("20240115103000")
        assert dt == datetime(2024, 1, 15, 10, 30, 0)
    
    def test_datetime_without_seconds(self):
        dt = parse_datetime("2024-01-15 10:30")
        assert dt == datetime(2024, 1, 15, 10, 30, 0)
    
    def test_invalid_datetime(self):
        assert parse_datetime("not a datetime") is None
        assert parse_datetime("2024-13-01 00:00:00") is None


class TestDateParsing:
    """Test date parsing with multiple formats."""
    
    def test_iso_date(self):
        d = parse_date("2024-01-15")
        assert d == date(2024, 1, 15)
    
    def test_eu_date(self):
        d = parse_date("15/01/2024")
        assert d == date(2024, 1, 15)
    
    def test_us_date(self):
        d = parse_date("01/15/2024")
        assert d == date(2024, 1, 15)
    
    def test_compact_date(self):
        d = parse_date("20240115")
        assert d == date(2024, 1, 15)
    
    def test_eu_dashes(self):
        d = parse_date("15-01-2024")
        assert d == date(2024, 1, 15)
    
    def test_invalid_date(self):
        assert parse_date("not a date") is None
        assert parse_date("2024-13-32") is None


class TestTimeParsing:
    """Test time parsing with multiple formats."""
    
    def test_24_hour_with_seconds(self):
        t = parse_time("14:30:00")
        assert t == time(14, 30, 0)
    
    def test_24_hour_with_microseconds(self):
        t = parse_time("14:30:00.123456")
        assert t.hour == 14
        assert t.microsecond > 0
    
    def test_24_hour_without_seconds(self):
        t = parse_time("14:30")
        assert t == time(14, 30, 0)
    
    def test_12_hour_pm(self):
        t = parse_time("02:30 PM")
        assert t == time(14, 30, 0)
    
    def test_12_hour_am(self):
        t = parse_time("02:30 AM")
        assert t == time(2, 30, 0)
    
    def test_12_hour_with_seconds(self):
        t = parse_time("02:30:45 PM")
        assert t == time(14, 30, 45)
    
    def test_invalid_time(self):
        assert parse_time("not a time") is None
        assert parse_time("25:00:00") is None


class TestJSONDetection:
    """Test JSON string detection."""
    
    def test_json_object(self):
        assert is_json_string('{"name": "Alice", "age": 30}') is True
    
    def test_json_array(self):
        assert is_json_string('[1, 2, 3, 4, 5]') is True
    
    def test_json_nested(self):
        assert is_json_string('{"user": {"name": "Bob", "age": 25}}') is True
    
    def test_not_json_plain_string(self):
        assert is_json_string('just a string') is False
    
    def test_not_json_number(self):
        assert is_json_string('42') is False
    
    def test_not_json_boolean(self):
        assert is_json_string('true') is False
    
    def test_invalid_json(self):
        assert is_json_string('{invalid json}') is False
    
    def test_non_string(self):
        assert is_json_string(42) is False
        assert is_json_string(None) is False


class TestTypeInference:
    """Test type inference from values."""
    
    def test_infer_datetime(self):
        assert infer_type("2024-01-15T10:30:00") == DataType.DATETIME
        assert infer_type("2024-01-15 10:30:00") == DataType.DATETIME
        assert infer_type(datetime(2024, 1, 15, 10, 30)) == DataType.DATETIME
    
    def test_infer_date(self):
        assert infer_type("2024-01-15") == DataType.DATE
        assert infer_type(date(2024, 1, 15)) == DataType.DATE
    
    def test_infer_time(self):
        assert infer_type("14:30:00") == DataType.TIME
        assert infer_type("02:30 PM") == DataType.TIME
        assert infer_type(time(14, 30, 0)) == DataType.TIME
    
    def test_infer_decimal(self):
        # High precision → DECIMAL
        assert infer_type("99.9999999") == DataType.DECIMAL
        assert infer_type("3.14159265358979") == DataType.DECIMAL
        assert infer_type(Decimal("19.99")) == DataType.DECIMAL
    
    def test_infer_float(self):
        # Low precision → FLOAT
        assert infer_type("3.14") == DataType.FLOAT
        assert infer_type("99.99") == DataType.FLOAT
        assert infer_type(3.14) == DataType.FLOAT
    
    def test_infer_integer(self):
        assert infer_type("42") == DataType.INTEGER
        assert infer_type(42) == DataType.INTEGER
    
    def test_infer_json(self):
        assert infer_type('{"name": "Alice"}') == DataType.JSON
        assert infer_type('[1, 2, 3]') == DataType.JSON
    
    def test_infer_string(self):
        assert infer_type("hello world") == DataType.STRING
        assert infer_type("not a number") == DataType.STRING
    
    def test_infer_boolean(self):
        assert infer_type("true") == DataType.BOOLEAN
        assert infer_type("false") == DataType.BOOLEAN
        assert infer_type(True) == DataType.BOOLEAN
    
    def test_infer_null(self):
        assert infer_type(None) == DataType.NULL
        assert infer_type("") == DataType.NULL
        assert infer_type("   ") == DataType.NULL


class TestTypeConversion:
    """Test infer_type_from_string conversion."""
    
    def test_convert_datetime(self):
        result = infer_type_from_string("2024-01-15 10:30:00")
        assert isinstance(result, datetime)
        assert result == datetime(2024, 1, 15, 10, 30, 0)
    
    def test_convert_date(self):
        result = infer_type_from_string("2024-01-15")
        assert isinstance(result, date)
        assert result == date(2024, 1, 15)
    
    def test_convert_time(self):
        result = infer_type_from_string("14:30:00")
        assert isinstance(result, time)
        assert result == time(14, 30, 0)
    
    def test_convert_decimal(self):
        result = infer_type_from_string("99.9999999")
        assert isinstance(result, Decimal)
        assert result == Decimal("99.9999999")
    
    def test_convert_float(self):
        result = infer_type_from_string("3.14")
        assert isinstance(result, float)
        assert result == 3.14
    
    def test_convert_integer(self):
        result = infer_type_from_string("42")
        assert isinstance(result, int)
        assert result == 42
    
    def test_convert_boolean(self):
        assert infer_type_from_string("true") is True
        assert infer_type_from_string("false") is False
    
    def test_convert_json(self):
        # JSON kept as string
        result = infer_type_from_string('{"name": "Alice"}')
        assert isinstance(result, str)
        assert result == '{"name": "Alice"}'


class TestTypeCoercion:
    """Test type coercion rules."""
    
    def test_numeric_coercion(self):
        assert DataType.INTEGER.coerce_to(DataType.FLOAT) == DataType.FLOAT
        assert DataType.FLOAT.coerce_to(DataType.INTEGER) == DataType.FLOAT
        assert DataType.INTEGER.coerce_to(DataType.DECIMAL) == DataType.DECIMAL
        assert DataType.FLOAT.coerce_to(DataType.DECIMAL) == DataType.DECIMAL
    
    def test_temporal_coercion(self):
        assert DataType.DATE.coerce_to(DataType.DATETIME) == DataType.DATETIME
        assert DataType.TIME.coerce_to(DataType.DATETIME) == DataType.DATETIME
        assert DataType.DATETIME.coerce_to(DataType.DATE) == DataType.DATETIME
    
    def test_json_coercion(self):
        assert DataType.JSON.coerce_to(DataType.JSON) == DataType.JSON
        assert DataType.JSON.coerce_to(DataType.STRING) == DataType.STRING
        assert DataType.STRING.coerce_to(DataType.JSON) == DataType.STRING
    
    def test_same_type_coercion(self):
        assert DataType.INTEGER.coerce_to(DataType.INTEGER) == DataType.INTEGER
        assert DataType.STRING.coerce_to(DataType.STRING) == DataType.STRING


class TestTypeComparison:
    """Test type comparability."""
    
    def test_numeric_comparable(self):
        assert DataType.INTEGER.is_comparable(DataType.FLOAT)
        assert DataType.INTEGER.is_comparable(DataType.DECIMAL)
        assert DataType.FLOAT.is_comparable(DataType.DECIMAL)
    
    def test_temporal_comparable(self):
        assert DataType.DATE.is_comparable(DataType.DATETIME)
        assert DataType.TIME.is_comparable(DataType.DATETIME)
        assert DataType.DATE.is_comparable(DataType.TIME)
    
    def test_not_comparable_across_categories(self):
        assert not DataType.INTEGER.is_comparable(DataType.STRING)
        assert not DataType.DATETIME.is_comparable(DataType.INTEGER)
        assert not DataType.JSON.is_comparable(DataType.FLOAT)
    
    def test_null_comparable_with_all(self):
        assert DataType.NULL.is_comparable(DataType.INTEGER)
        assert DataType.NULL.is_comparable(DataType.STRING)
        assert DataType.INTEGER.is_comparable(DataType.NULL)


class TestCommonTypeInference:
    """Test infer_common_type from multiple values."""
    
    def test_all_integers(self):
        assert infer_common_type([1, 2, 3]) == DataType.INTEGER
    
    def test_mixed_int_float(self):
        assert infer_common_type([1, 2.5, 3]) == DataType.FLOAT
    
    def test_mixed_numeric_with_decimal(self):
        assert infer_common_type([1, Decimal("2.5")]) == DataType.DECIMAL
    
    def test_all_dates(self):
        assert infer_common_type([date(2024, 1, 1), date(2024, 1, 2)]) == DataType.DATE
    
    def test_mixed_date_datetime(self):
        result = infer_common_type([date(2024, 1, 1), datetime(2024, 1, 2, 10, 30)])
        assert result == DataType.DATETIME
