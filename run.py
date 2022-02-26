from configurer import PrefetchConfiguration, PipelineConfiguration, Rate, Duration, Iteration, Storage, Workload
from plot import do_plot

def algo_logger(prefetch_bucket):
    d = {}
    d['submitting'] = len(prefetch_bucket.pipeline.submitted_bucket)
    d['submitted_total'] = prefetch_bucket.pipeline.submitted_bucket.counter
    d['inflight'] = len(prefetch_bucket.pipeline.inflight_bucket)
    d['completed_not_consumed'] = len(prefetch_bucket.pipeline.completed_bucket)
    d['in_progress_ios'] = d['submitting'] + d['inflight'] + d['completed_not_consumed']
    d['consumed_total'] = prefetch_bucket.pipeline.consumed_bucket.counter
    d['consumption_rate'] = prefetch_bucket.pipeline.completed_bucket.consumption_rate()
    d['completed_target_distance'] = prefetch_bucket.completion_target_distance

    log_str = f"[{prefetch_bucket.tick}]:"
    for k, v in d.items():
        log_str += f'{k}: {v}, '

    return log_str

# LOG_PREFETCH = True
LOG_PREFETCH = False

storage = Storage(
            max_iops=100,
            cap_inflight=100,
            cap_in_progress=200,
            submission_overhead=Duration(microseconds=10),
            base_completion_latency=Duration(microseconds=400),
            )

def consumption_rate_func(self):
    if self.counter <= 100:
        return Rate(per_second=10000)
    if self.counter > 100:
        return Rate(per_second=20000)

def consumption_rate_func2(self):
    if self.tick <= 5000:
        return Rate(per_second=5000)
    else:
        rate = Rate(per_second=20000)
        return rate

workload = Workload(
                volume=100,
                duration=Duration(seconds=2),
                consumption_rate_func=consumption_rate_func2,
                )

def prefetch_size1(self):
    inflight = len(self.pipeline.inflight_bucket)
    completed_not_consumed = len(self.pipeline.completed_bucket)

    if inflight >= self.pipeline.target_inflight - self.min_dispatch:
        return 0

    if completed_not_consumed + inflight + self.min_dispatch >= self.pipeline.completion_target_distance:
        return 0

    target = self.pipeline.completion_target_distance - inflight - completed_not_consumed

    target = max(self.min_dispatch, target)
    to_submit = min(self.pipeline.target_inflight - inflight, target)
    if LOG_PREFETCH:
        print('Post Adjustment:\n' + algo_logger(self))
    return to_submit

def adjust1(self):
    inflight = len(self.pipeline.inflight_bucket)
    completed_not_consumed = len(self.pipeline.completed_bucket)
    consumed_total = self.pipeline.consumed_bucket.counter

    if consumed_total == 0:
        return

    if inflight >= 0.9 * self.pipeline.target_inflight:
        desired_target_inflight = self.pipeline.target_inflight + 1
        self.pipeline.target_inflight = min(desired_target_inflight,
                                            self.pipeline.cap_inflight)

    if completed_not_consumed < 0.25 * self.pipeline.completion_target_distance:
        desired_completion_target_distance = self.pipeline.completion_target_distance + 1
        self.pipeline.completion_target_distance = min(desired_completion_target_distance, self.pipeline.cap_in_progress)

prefetch_config = PrefetchConfiguration(
                          prefetch_size_func=prefetch_size1,
                          min_dispatch=2,
                          initial_completion_target_distance=15,
                          initial_target_inflight=10,
                    )

pipeline_config = PipelineConfiguration(
    prefetch_configuration=prefetch_config,
    storage=storage,
)

print(f'config is {pipeline_config}')

iterations = [
    Iteration(
        {
            'PrefetchBucket.adjust_before': adjust1,
            'CompleteBucket.adjust_after': adjust1,
        }
    ),
    Iteration(
        {
            'PrefetchBucket.adjust_before': adjust1,
        }
    ),
]

for iteration in iterations:
    # For now, you must specify whole numbers for Duration and Rate
    pipeline = pipeline_config.generate_pipeline()
    iteration.configure_pipeline(pipeline)
    workload.configure_pipeline(pipeline)
    prefetch_config.configure_pipeline(pipeline)

    data = pipeline.run(workload)

    do_plot(data)
