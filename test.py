from configurer import Workload, Prefetcher, PipelineConfiguration
from units import Duration, Rate
import matplotlib.pyplot as plt
import pandas as pd
from storage import fast_local, slow_cloud

def handle_early_stage1(bucket):
    consumed = len(self.pipeline['consumed'])
    submitted = len(self.pipeline['submitted'])
    inflight = len(self.pipeline['inflight'])
    completed = len(self.pipeline['completed'])
    in_progress = self.target.counter - consumed

    if submitted == 0:
        return

    if consumed == 0:
        return

def bounded_bump(value, ratio, caps):
    new_val = max(1, int(value * ratio))
    for cap in caps:
        new_val = min(new_val, cap)
    return new_val

# TODO: how to make it so that I can iterate through different early stage
# handlers and plot each one
# and then iterate through "steps" in the algorithm and feature them in the
# plot too
def adjust1(self, original):
    consumed = len(self.pipeline['consumed'])
    submitted = len(self.pipeline['submitted'])
    inflight = len(self.pipeline['inflight'])
    completed = len(self.pipeline['completed'])
    in_progress = self.target.counter - consumed

    if submitted == 0:
        return

    if consumed == 0:
        return

    # handle_early_stage_funcs[0]()
    ctd = self.pipeline.completion_target_distance
    # if submitted > 1.2 * inflight:
    #     self.pipeline.completion_target_distance = bounded_bump(ctd, 0.8, caps)

    caps = [self.pipeline.cap_in_progress]
    if completed < 0.9 * ctd:
        self.pipeline.completion_target_distance = bounded_bump(ctd, 1.2, caps)

    if in_progress < 0.5 * ctd:
        self.pipeline.completion_target_distance = bounded_bump(in_progress, 1.2, caps)

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


def test_consumption_rate(self, original):
    return Rate(per_second=5000).value

workload = Workload(
    consumption_rate_func=test_consumption_rate,
    volume=200,
    duration=Duration(seconds=10),
    trace_ios = [1, 5, 100]
)

prefetcher = Prefetcher(
    prefetch_num_ios_func = prefetch_num_ios,
    adjust_func = adjust1,
    min_dispatch=1,
    initial_completion_target_distance=10,
    cap_in_progress=100,
)

pipeline_config = PipelineConfiguration(
    storage=fast_local,
    workload=workload,
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
trace_data = []
for tracer in workload.tracers:
    trace_data.extend(tracer.trace_data)

trace_view = pd.DataFrame(trace_data).set_index('tick')
print(trace_view)

# Do plot
figure, axes = plt.subplots(2)
view.plot(ax=axes[0])
axes[1].get_yaxis().set_visible(False)
wait_view.astype(int).plot.area(ax=axes[1], stacked=False)
plt.show()

# title = sys.argv[1]
# plt.savefig(f'images/{title}.png')
