from configurer import Workload, Prefetcher, PipelineConfiguration
from units import Duration, Rate
import matplotlib.pyplot as plt
import pandas as pd
from storage import fast_local1, slow_cloud1
import sys
from adjusters import *
from plot import io_title

def prefetch_num_ios(self, original):
    consumed = len(self.pipeline['consumed'])
    submitted = len(self.pipeline['submitted'])
    inflight = len(self.pipeline['inflight'])
    in_progress = self.target.counter - consumed

    self.tick_data['completion_target_distance'] = self.pipeline.completion_target_distance
    if in_progress + self.min_dispatch() >= self.pipeline.cap_in_progress:
        return 0

    if in_progress + self.min_dispatch() >= self.pipeline.completion_target_distance:
        return 0

    ctd = self.pipeline.completion_target_distance

    to_submit = min(
        self.pipeline.completion_target_distance - in_progress,
        self.pipeline.cap_in_progress - in_progress)

    # to_submit = min(len(self), to_submit)

    print(f'ctd: {ctd}. min_dispatch: {self.min_dispatch()}. cap_in_progress: {self.pipeline.cap_in_progress}. in_progress: {in_progress}. to_submit is {to_submit}')
    return to_submit


prefetcher = Prefetcher(
    prefetch_num_ios_func = prefetch_num_ios,
    adjust_func = adjust1,
    min_dispatch=2,
    initial_completion_target_distance=10,
    cap_in_progress=100,
)

def test_consumption_rate(self, original):
    return Rate(per_second=5000).value

workload1 = Workload(
    id=1,
    consumption_rate_func=test_consumption_rate,
    volume=200,
    duration=Duration(seconds=10),
    trace_ios = [1, 5, 100]
)

pipeline_config = PipelineConfiguration(
    storage=fast_local1,
    workload=workload1,
    prefetcher=prefetcher,
)

pipeline = pipeline_config.generate_pipeline()

### Run

data = pipeline.run(workload)
data = data.reindex(data.index.union(data.index[1:] - 1), method='ffill')

### Plot

# IO data
view = pd.DataFrame(index=data.index)
rename = {
    'sync': 'baseline_sync_to_move',
    'fetch': 'baseline_all_to_move',
    'prefetch': 'remaining_to_move',
    'claim': 'awaiting_buffer_to_move',
    'invoke_kernel': 'w_claimed_buffer_to_move',
    'num_ios_w_buffer': 'w_claimed_buffer_num_ios',
    'submit': 'kernel_batch_to_move',
    'dispatch': 'submitted_to_move',
    'complete': 'inflight_to_move',
    'consume': 'completed_to_move',
    'completion_target_distance': 'remaining_completion_target_distance',
}
rename = {k: data[v] for k, v in rename.items() if v in data}
view = view.assign(**rename)

print(view)

# Wait data
wait_view = pd.DataFrame(index=data.index)
for column in data:
    if not column.endswith('_want_to_move'):
        continue
    bucket_name = column.removesuffix('_want_to_move')
    wait_view[f'{bucket_name}_wait'] = data.apply(
        lambda record: record[f'{bucket_name}_want_to_move'] > record[f'{bucket_name}_to_move'],
        axis='columns')

wait_rename = {
    'wait_dispatch': 'submitted_wait',
    'wait_consume': 'completed_wait',
}

wait_rename = {k: wait_view[v] for k, v in wait_rename.items() if v in wait_view}
wait_view = wait_view.assign(**wait_rename)
dropped_columns = [column for column in wait_view if column not in wait_rename.keys()]
wait_view = wait_view.drop(columns=dropped_columns)
print(wait_view)

# Trace data
tracer_view = pd.concat(tracer.data for tracer in workload1.tracers)

bucket_sequence = [bucket.name for bucket in pipeline.buckets]
tracer_view = pd.concat(tracer.data for tracer in workload1.tracers) \
    .dropna(axis='index', subset='interval') \
    .pivot(index='io', columns='bucket', values='interval') \
    .reindex(bucket_sequence, axis='columns', fill_value=0)

print(tracer_view)
tracer_view.plot(kind='barh', stacked=True)

# Do plot
figure, axes = plt.subplots(2)

axes[0].set_xlim([0, max(view.index)])
view.plot(ax=axes[0],
                    title=io_title(pipeline_config.storage, pipeline_config.workload, pipeline_config.prefetcher)
          )
axes[1].get_yaxis().set_visible(False)
wait_view.astype(int).plot.area(ax=axes[1], stacked=False)

show = sys.argv[1]
title = sys.argv[2]

if show == 'show':
    plt.show()
else:
    plt.savefig(f'images/{title}.png')

