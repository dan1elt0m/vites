import statistics
from typing import List
import uuid
import time
from pydantic import BaseModel
from vites.factory import benchmark_factory


class Results(BaseModel):
    id: uuid.UUID
    num_records: int
    median_duration: float
    avg_duration: float
    durations: List[float]



def run_benchmark(df: "DataFrame", iterations: int = 3) -> Results:
    """Runs a benchmark on a DataFrame"""
    durations = []
    for iteration in range(1, iterations + 1):
        start = time.time()
        df.write.format('noop').mode("overwrite").save()
        duration = time.time() - start
        durations.append(duration)

    num_records = df.count()
    unique_id = uuid.uuid4()
    median_duration = statistics.median(durations)
    avg_duration = statistics.mean(durations)
    return Results(id=unique_id, num_records=num_records, median_duration=median_duration, avg_duration=avg_duration, durations=durations)

