"""
Basic sanity tests for package setup
"""

import sqlstream


def test_version():
    """Test that version is defined"""
    assert hasattr(sqlstream, "__version__")
    assert sqlstream.__version__ == "0.1.0"


def test_import():
    """Test that package can be imported"""
    import sqlstream.cli
    import sqlstream.core
    import sqlstream.operators
    import sqlstream.optimizers
    import sqlstream.readers
    import sqlstream.sql
    import sqlstream.utils

    # All subpackages should be importable
    assert sqlstream is not None
