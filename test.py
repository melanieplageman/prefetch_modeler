from configurer import Workload, Storage
from units import Duration, Rate
from model import TestPipeline
import matplotlib.pyplot as plt
import pandas as pd

pipeline = TestPipeline()

def test_consumption_rate(self, original):
    return Rate(per_second=1000).value

def storage_latency(self, original):
    return int(Duration(milliseconds=2).total)

def submission_latency(self, original):
    return int(Duration(microseconds=10).total)

# TODO: throw an error in pipeline if no one has any work to do -- all return
# infinity but there is more IOs and time

# TODO: make it so that if there is no inflightlatency bucket, but there is an
# inflight bucket, either it errors out or the inflightratebucket uses a 0
# latency
storage = Storage(
    completion_latency_func = storage_latency,
    kernel_invoke_batch_size = 5,
    submission_overhead_func = submission_latency,
    max_iops=Rate(per_millisecond=1).value,
    cap_in_progress=10)

workload = Workload(
    consumption_rate_func=test_consumption_rate,
    volume=50,
    duration=Duration(seconds=10),
)
workload.configure_pipeline(pipeline)
storage.configure_pipeline(pipeline)

data = pipeline.run(workload)

# data = data[data.columns.drop(list(data.filter(regex=r'_num_ios$')))]
data = data[data.columns.drop(list(data.filter(regex=r'_to_move$')))]

data = data.reindex(data.index.union(data.index[1:] - 1), method='ffill')

# submission overhed, inflight max IOPs
print(data)
data.plot()
plt.show()
