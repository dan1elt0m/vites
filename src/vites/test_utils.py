import json

from vites.factory import BenchmarkSchema
from pydantic import Field
from pyspark.sql.types import StructType, StructField, StringType


class TestBenchmarkSchema(BenchmarkSchema):
    field1: str = Field(..., min_length=5, max_length=10)
    field2: int = Field(..., ge=0, le=100)
    field3: float = Field(..., gt=0, lt=10.0)
    field4: dict[str,str] = Field(..., min_items=1, max_items=10)
    field5: dict[str, str] = Field(..., min_items=1, spark_type =StructType([StructField("key", StringType(), True), StructField("value", StringType(), True)]))

