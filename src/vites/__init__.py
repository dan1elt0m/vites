from typing import Generic, Type, Optional, List
from pyspark.sql import Row, SparkSession, DataFrame as SparkDataFrame

import time
from polyfactory.factories.pydantic_factory import ModelFactory, T
from pydantic import BaseModel

def benchmark_factory(model: Type[BaseModel]) -> BaseModel:
    """Creates synthetic data based on the schema provided"""
    class BenchModelFactory(BaseBenchmarkModelFactory[model]):
        __model__ = model

    factory = BenchModelFactory.create_factory()
    return factory.build()

class BenchmarkSchema(BaseModel):

    @classmethod
    def create_benchmark_dataframe(cls, num_records: int = 100, spark: Optional[SparkSession] = None) -> SparkDataFrame:
        """Creates a DataFrame with synthetic data based on the schema provided"""
        if not spark:
            spark = SparkSession.builder.getOrCreate()

        rdd = spark.sparkContext.parallelize(range(num_records))
        fake_data_rdd = rdd.map(lambda x: benchmark_factory(cls))
        return spark.createDataFrame(fake_data_rdd)


class BaseBenchmarkModelFactory(ModelFactory[T], Generic[T]):
    __base_factory__ = True
    __model__ = BenchmarkSchema



def run_benchmark(df: SparkDataFrame, iterations: int = 3) -> SparkDataFrame:
    """Runs a benchmark on a DataFrame"""
    num_records = df.count()
    results = []
    for iteration in range(1, iterations + 1):
        print(f"Running benchmark - iteration: ", iteration)
        start = time.time()
        df.write.format('noop').mode("overwrite").save()
        duration = time.time() - start
        print(f"Benchmark took {duration} seconds")
        results.append(Row(num_records=num_records, iteration=iteration, duration=duration))
    results_schema = "num_records int, iteration int, duration double"
    return df.sparkSession.createDataFrame(results, results_schema)