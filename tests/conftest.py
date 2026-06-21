import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Creates a local SparkSession for testing purposes.
    """
    spark_session = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-pyspark-local-testing")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()
