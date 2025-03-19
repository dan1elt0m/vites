import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import Row
from vites import run_benchmark
from vites.test_utils import TestBenchmarkSchema


@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder \
        .appName("BenchmarkTest") \
        .master("local[*]") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()

def test_create_benchmark_df(spark):
    schema = TestBenchmarkSchema
    df = schema.create_benchmark_dataframe(num_records=10)
    assert df.count() == 10

def test_benchmark(spark):
    schema = TestBenchmarkSchema
    df = schema.create_benchmark_dataframe(num_records=10)
    assert isinstance(df, DataFrame)
    result =  run_benchmark(df)
    assert result.count() == 3
    assert isinstance(result, DataFrame)
    result.show()
