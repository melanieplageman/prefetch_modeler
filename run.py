import pandas as pd
from configurer import *
from model import *
from bucket import *
from plot import single_plot
from override import override, AlgorithmCollection

# TODO: at IO completion, issue more if previously limited by target_inflight

# TODO: add minimums for completion_target_distance, target_inflight, min_dispatch

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

LOG_PREFETCH = False

prefetch_desired_move_size_algos = AlgorithmCollection()

@prefetch_desired_move_size_algos.algorithm
def algo1(self):

    # TODO: if submission overhead is sufficiently high, we may need to account
    # for that when calculating whether or not we can submit more here given
    # the inflight cap and completion latency
    inflight = len(self.pipeline.inflight_bucket)
    completed_not_consumed = len(self.pipeline.completed_bucket)

    # If dispatching more would cause us to hit our soft cap for inflight
    # requests, don't submit now
    # TODO: consider decreasing target_inflight
    # TODO: set a variable indicating that this has happened
    if inflight >= self.target_inflight - self.min_dispatch:
        return 0

    # If there are enough completed requests, don't submit any more
    # Since we may have overshot,
    # TODO: consider decreasing completion_target_distance
    if completed_not_consumed + inflight + self.min_dispatch >= self.completion_target_distance:
        return 0

    # TODO: what would be good logic for inc/dec target_inflight?
    if inflight >= 0.9 * self.target_inflight:
        desired_target_inflight = self.target_inflight + 1
        self.target_inflight = min(desired_target_inflight, self.cap_inflight)

    # If we are lagging too far behind on completed requests, it is time to
    # increase completion_target_distance.
    # TODO: account for this happening initially
    if completed_not_consumed < 0.25 * self.completion_target_distance:
        desired_completion_target_distance = self.completion_target_distance + 1
        self.completion_target_distance = min(desired_completion_target_distance,
                                              self.cap_in_progress)

    target = self.completion_target_distance - inflight - completed_not_consumed
    # Don't submit less than self.min_dispatch
    # This shouldn't exceed limits on completed or inflight due to boundary
    # checking above.
    target = max(self.min_dispatch, target)
    to_submit = min(self.target_inflight - inflight, target)
    if LOG_PREFETCH:
        print('Post Adjustment:\n' + algo_logger(self))
    return to_submit

@prefetch_desired_move_size_algos.algorithm
def algo2(self):
    return 3

def consumption_rate_func(self):
    if self.counter <= 100:
        return Rate(per_second=10000)
    if self.counter > 100:
        return Rate(per_second=20000)

@override('CompletedGateBucket.consumption_rate')
def consumption_rate_func2(self):
    if self.tick <= 5000:
        return Rate(per_second=5000)
    else:
        rate = Rate(per_second=20000)
        return rate

prefetch_configs = [
    PrefetchConfiguration(cap_inflight=100,
                          cap_in_progress=200,
                          min_dispatch=2,
                          initial_completion_target_distance=15,
                          initial_target_inflight=10,),
]

# For now, you must specify whole numbers for Duration and Rate
for algo in prefetch_desired_move_size_algos:
    for config in prefetch_configs:
        wanted_move_size = override('PrefetchGateBucket.wanted_move_size')(algo)
        print(f'Algorithm: {wanted_move_size.__name__}')
        config = PipelineConfiguration(
            prefetch_configuration=config,
            submission_overhead=Duration(microseconds=10),
            max_iops=100,
            base_completion_latency=Duration(microseconds=400),
        )
        print(f'config is {config}')
        pipeline = config.generate_pipeline()

        data = pipeline.run(volume=100, duration=Duration(seconds=2))

        to_plot = pd.DataFrame(index=data.index)

        to_plot['wait'] = data.apply(lambda record:
            record['completed_want_to_move'] > record['completed_to_move'], axis='columns')

        last_wait = 0
        for i in range(len(to_plot['wait'])):
            last_wait = last_wait + 1 if to_plot['wait'][i] else 0
            to_plot['wait'][i] = last_wait

        to_plot['completed'] = data['completed_num_ios']
        to_plot['inflight'] = data['inflight_num_ios']
        to_plot['consumed'] = data['consumed_num_ios']
        to_plot['completion_target_distance'] = data['prefetched_completion_target_distance']
        to_plot['target_inflight'] = data['prefetched_target_inflight']

        single_plot(to_plot)
