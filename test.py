from configurer import Workload, Storage, Prefetcher, PipelineConfiguration
from units import Duration, Rate
from model import TestPipeline
import matplotlib.pyplot as plt
import pandas as pd


def test_consumption_rate(self, original):
    return Rate(per_second=1000).value

def storage_latency(self, original):
    return int(Duration(milliseconds=3).total)

def submission_latency(self, original):
    return int(Duration(microseconds=10).total)

def adjust1(self, original):
    consumed = len(self.pipeline['consumed'])
    in_progress = self.target.counter - consumed
    if consumed == 0:
        return

    if in_progress < 0.5 * self.pipeline.completion_target_distance:
        self.pipeline.completion_target_distance = int(in_progress * 1.2)

def prefetch_num_ios(self, original):
    consumed = len(self.pipeline['consumed'])
    in_progress = self.target.counter - consumed

    if in_progress + self.min_dispatch() >= self.pipeline.cap_in_progress:
        return 0

    if in_progress + self.min_dispatch() >= self.pipeline.completion_target_distance:
        return 0

    ctd = self.pipeline.completion_target_distance

    to_submit = min(
        self.pipeline.completion_target_distance - in_progress,
        self.pipeline.cap_in_progress - in_progress)

    # self.tick_data['completion_target_distance'] = self.pipeline.completion_target_distance

    print(f'ctd: {ctd}. min_dispatch: {self.min_dispatch()}. cap_in_progress: {self.pipeline.cap_in_progress}. in_progress: {in_progress}. to_submit is {to_submit}')
    return to_submit



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
)

workload = Workload(
    consumption_rate_func=test_consumption_rate,
    volume=200,
    duration=Duration(seconds=10),
)

prefetcher = Prefetcher(
                          prefetch_num_ios_func = prefetch_num_ios,
                          adjust_func = adjust1,
                          min_dispatch=1,
                          initial_completion_target_distance=50,
                          cap_in_progress=100,
                    )

pipeline_config = PipelineConfiguration(
                storage=storage,
                workload=workload,
                prefetcher=prefetcher,
            )

pipeline = pipeline_config.generate_pipeline()

data = pipeline.run(workload)

# data = data[data.columns.drop(list(data.filter(regex=r'_num_ios$')))]
# data = data[data.columns.drop(list(data.filter(regex=r'_to_move$')))]
# data = data[data.columns.drop(list(data.filter(regex=r'consumed')))]
data = data[['completed_num_ios', 'prefetched_to_move',
             'prefetched_want_to_move']]

data = data.reindex(data.index.union(data.index[1:] - 1), method='ffill')

# submission overhed, inflight max IOPs
print(data)
data.plot()
plt.show()
