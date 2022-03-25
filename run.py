from configurer import PipelineConfiguration, Prefetcher, Storage, Workload
from units import Rate, Duration, InfiniteRate
from plot import do_plot, to_row
import math

def algo_logger(prefetch_bucket):
    d = {}
    d['submitting'] = len(prefetch_bucket.pipeline.submitted_bucket)
    d['submitted_total'] = prefetch_bucket.pipeline.submitted_bucket.counter
    d['inflight'] = len(prefetch_bucket.pipeline.inflight_bucket)
    d['completed_not_consumed'] = len(prefetch_bucket.pipeline.completed_bucket)
    d['in_progress_ios'] = d['submitting'] + d['inflight'] + d['completed_not_consumed']
    d['consumed_total'] = prefetch_bucket.pipeline.consumed_bucket.counter
    d['consumption_rate'] = prefetch_bucket.pipeline.completed_bucket.consumption_rate()
    d['completed_target_distance'] = prefetch_bucket.pipeline.completion_target_distance

    log_str = f"[{prefetch_bucket.tick}]:"
    for k, v in d.items():
        log_str += f'{k}: {v}, '

    return log_str

LOG_PREFETCH = False
#LOG_PREFETCH = False

# TODO: make this formula better
def storage_latency1(self, original):
    base_completion_latency=Duration(milliseconds=4)
    return int(base_completion_latency.total)

def submission_overhead1(self, original):
    submission_overhead=Duration(microseconds=10)
    return int(submission_overhead.total)

# For now, you must specify whole numbers for Duration and Rate
storages = [
    Storage(
            completion_latency_func = storage_latency1,
            submission_overhead_func = submission_overhead1,
            max_iops=1000,
        # TODO: cap based on completion latency and max iops
            cap_inflight=5,
            cap_in_progress=16,
            ),
    ]

# reasonable upper bound 100000
# reasonable lower bound 10
def consumption_rate_func1(self, original):
    return Rate(per_second=1000)

workloads = [
    Workload(
                consumption_rate_func=consumption_rate_func1,
                volume=10,
                duration=Duration(seconds=2),
                )
    ]

def adjust1(self, original):
    base_ctd_decrease = 0.2
    inflight = len(self.pipeline['inflight'])
    completed_not_consumed = len(self.pipeline['completed'])
    consumed_total = self.pipeline['consumed'].counter

    if consumed_total == 0:
        return

    if inflight >= 0.9 * self.pipeline.target_inflight:
        desired_target_inflight = self.pipeline.target_inflight + 1
        self.pipeline.target_inflight = min(desired_target_inflight,
                                            self.pipeline.cap_inflight)

    ctd = self.pipeline.completion_target_distance

    if completed_not_consumed < 0.25 * ctd:
        desired_ctd = ctd + 1
        self.pipeline.completion_target_distance = min(desired_ctd, self.pipeline.cap_in_progress)

    if completed_not_consumed > 1.2 * ctd:
        new_cnc_ctd_ratio = completed_not_consumed / ctd
        multiplier = new_cnc_ctd_ratio / self.pipeline.cnc_ctd_ratio
        new_decrease = multiplier * base_ctd_decrease
        desired_ctd = math.ceil(ctd * (1 - new_decrease))
        self.pipeline.cnc_ctd_ratio = new_cnc_ctd_ratio
        # print(f'desired_ctd is {desired_ctd}')
        self.pipeline.completion_target_distance = min(desired_ctd, self.pipeline.cap_in_progress)

    if LOG_PREFETCH:
        print('Post Adjustment:\n' + algo_logger(self))

prefetchers = [
            Prefetcher(
                          adjusters = {
                                        'prefetched.adjust_before' : adjust1,
                                        'completed.adjust_before' : adjust1,
                                      },
                          min_dispatch=2,
                          initial_completion_target_distance=15,
                          initial_target_inflight=5,
                    ),
            ]

for storage in storages:
    print(f'storage is:\n{storage}\n')
    for workload in workloads:
        print(f'workload is:\n{workload}\n')
        for prefetcher in prefetchers:
            print(f'prefetcher is:\n{prefetcher}\n')
            pipeline_config = PipelineConfiguration(
                storage=storage,
                workload=workload,
                prefetcher=prefetcher,
            )

            pipeline = pipeline_config.generate_pipeline()

            data = pipeline.run(workload)
            print(data)
            # print(f"{to_row(data)}\n")
            do_plot(data)
