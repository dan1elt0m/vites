from typing import Type, Generic
from pydantic import BaseModel
from polyfactory.factories.pydantic_factory import ModelFactory, T
from sparkdantic import create_spark_schema, create_json_spark_schema


class BenchmarkSchema(BaseModel):
    @classmethod
    def create_benchmark_dataframe(cls,spark: "SparkSession", num_records: int = 100, ) -> "DataFrame":
        """Creates a DataFrame with synthetic data based on the schema provided"""
        rdd = spark.sparkContext.parallelize(range(num_records))
        fake_data_rdd = rdd.map(lambda x: benchmark_factory(cls))
        spark_schema = create_spark_schema(cls)
        return spark.createDataFrame(fake_data_rdd, spark_schema)


def benchmark_factory(model: Type[BaseModel]) -> BaseModel:
    """Creates synthetic data based on the schema provided"""
    class BenchModelFactory(BaseBenchmarkModelFactory[model]):
        __model__ = model

    @classmethod
    def create_factory(cls):
        factory = super().create_factory()
        return factory

    factory = BenchModelFactory.create_factory()
    return factory.build()

class BaseBenchmarkModelFactory(ModelFactory[T], Generic[T]):
    __base_factory__ = True
    __model__ = BenchmarkSchema

