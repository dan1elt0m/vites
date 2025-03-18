import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from vites import BenchScenario, TestBenchmarkSchema, createBenchmarkDF, toBenchmarkDF, BenchmarkDataFrame

@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder \
        .appName("BenchmarkTest") \
        .master("local[*]") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()


def test_createBenchmarkDf(spark):
    schema = TestBenchmarkSchema
    scenario = BenchScenario(df_schema=schema, num_records=10)
    df = createBenchmarkDF(scenario, spark)
    assert df.count() == 10
    print(df.show())

def test_benchmark(spark):
    scenario = BenchScenario(df_schema=TestBenchmarkSchema, num_records=10)
    df = createBenchmarkDF(scenario, spark)
    assert isinstance(df, BenchmarkDataFrame)
    result =  df.runBenchmark()
    assert isinstance(result, float)

def test_to_benchmark_df(spark):
    df = spark.createDataFrame([Row(i) for i in range(100)],schema="id int")
    bench_df = BenchmarkDataFrame(df)
    assert isinstance(bench_df, BenchmarkDataFrame)
