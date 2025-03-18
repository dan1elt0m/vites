from typing import Any, Dict, Generic, Type
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame

import time
from polyfactory import BaseFactory
from polyfactory.factories.pydantic_factory import ModelFactory, T
from pydantic import BaseModel

class BenchSchema(BaseModel):
    pass

class TestBenchmarkSchema(BenchSchema):
    field1: str
    field2: int
    field3: float

class BaseBenchmarkModelFactory(ModelFactory[T], Generic[T]):
    __base_factory__ = True
    __model__ = BenchSchema


def create_benchmark_data(schema: BaseModel) -> Dict[str, Any]:
    """Creates synthetic data based on the schema provided"""
    class BenchModelFactory(BaseBenchmarkModelFactory[schema]):
        __model__ = schema

    factory = BenchModelFactory.create_factory()
    return factory.build()


class BenchScenario(BaseModel):
    """Defines a benchmark scenario"""
    df_schema: type[BenchSchema]
    num_records: int


class BenchmarkDataFrame(SparkDataFrame):
    """A Spark DataFrame that can be used for benchmarking"""
    def __init__(self, spark_df: SparkDataFrame, *args, **kwargs):
        super().__init__(spark_df._jdf, spark_df.sql_ctx)

    def options(self, **kwargs) -> 'BenchmarkDataFrame':
        self.options = kwargs
        return self

    def _run_benchmark(self) -> float:
        print("Running benchmark")
        start = time.time()
        self.write.format('noop').mode("overwrite").save()
        duration = time.time() - start
        print(f"Benchmark took {duration} seconds")
        return duration

    def runBenchmark(self):
        return self._run_benchmark()


def createBenchmarkDF(scenario: BenchScenario, spark_session: SparkSession) -> BenchmarkDataFrame:
    """Creates a DataFrame with synthetic data based on the schema provided"""
    rdd = spark_session.sparkContext.parallelize(range(scenario.num_records))
    fake_data_rdd = rdd.map(lambda x: create_benchmark_data(scenario.df_schema))
    return BenchmarkDataFrame(spark_session.createDataFrame(fake_data_rdd))
