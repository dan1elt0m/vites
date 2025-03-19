import pytest
from pyspark.sql import SparkSession, DataFrame
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
    df = schema.create_benchmark_dataframe(spark, num_records=100)
    assert df.count() == 100

def test_benchmark(spark):
    schema = TestBenchmarkSchema
    df = schema.create_benchmark_dataframe(spark, num_records=10)
    assert isinstance(df, DataFrame)
    results = run_benchmark(df)
    assert results.num_records == 10
    assert len(results.durations) == 3
    assert results.avg_duration > 0
    assert results.median_duration > 0
    assert results.id is not None

