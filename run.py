import pandas as pd
from configurer import *
from model import *
from bucket import *
from plot import single_plot
from override import override, AlgorithmCollection

# TODO: at IO completion, issue more if previously limited by max_inflight

# TODO: add minimums for completion_target_distance, max_inflight, min_dispatch

def algo_logger(prefetch_bucket):
    d = {}
    d['submitting in prog'] = len(prefetch_bucket.pipeline.submitted_bucket)
    d['submitted'] = prefetch_bucket.pipeline.submitted_bucket.counter
    d['inflight'] = len(prefetch_bucket.pipeline.inflight_bucket)
    d['completed_but_not_consumed'] = len(prefetch_bucket.pipeline.completed_bucket)
    d['consumed'] = len(prefetch_bucket.pipeline.consumed_bucket)

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
    completed = len(self.pipeline.completed_bucket)

    # If dispatching more would cause us to hit our soft cap for inflight
    # requests, don't submit now
    # TODO: consider decreasing max_inflight
    # TODO: set a variable indicating that this has happened
    if inflight >= self.max_inflight - self.min_dispatch:
        return 0

    # If there are enough completed requests, don't submit any more
    # Since we may have overshot,
    # TODO: consider decreasing completion_target_distance
    if completed + inflight + self.min_dispatch >= self.completion_target_distance:
        return 0

    # TODO: what would be good logic for inc/dec max_inflight?
    if inflight >= 0.9 * self.max_inflight:
        desired_max_inflight = self.max_inflight + 1
        self.max_inflight = min(desired_max_inflight, self.inflight_cap)

    # If we are lagging too far behind on completed requests, it is time to
    # increase completion_target_distance.
    # TODO: account for this happening initially
    if completed < 0.25 * self.completion_target_distance:
        desired_completion_target_distance = self.completion_target_distance + 1
        self.completion_target_distance = min(desired_completion_target_distance,
                                              self.completed_cap)

    target_inflight = self.completion_target_distance - inflight - completed
    # Don't submit less than self.min_dispatch
    # This shouldn't exceed limits on completed or inflight due to boundary
    # checking above.
    target_inflight = max(self.min_dispatch, target_inflight)
    to_submit = min(self.max_inflight - inflight, target_inflight)
    if LOG_PREFETCH:
        print('Post Adjustment:\n' + algo_logger(self))
    return to_submit

@prefetch_desired_move_size_algos.algorithm
def algo2(self):
    return 3

prefetch_configs = [
    PrefetchConfiguration(inflight_cap=100,
                          completed_cap=200,
                          min_dispatch=2,
                          initial_completion_target_distance=15,
                          initial_max_inflight=10,),
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
            consumption_rate=Rate(per_second=50000),
        )
        print(config)
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
        to_plot['max_inflight'] = data['prefetched_max_inflight']

        single_plot(to_plot)
