
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession fixture for all tests."""
    session = SparkSession.builder.master("local[1]").appName("test-utils").getOrCreate()
    yield session
    session.stop()

