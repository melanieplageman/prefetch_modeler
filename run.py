import pandas as pd
from pipeline_configurer import *
from model import *
from bucket import *
from plot import single_plot

# TODO: at IO completion, issue more if previously limited by max_inflight

# TODO: add minimums for completion_target_distance, max_inflight, min_dispatch


def algo1(self):
    inflight = len(self.inflight_bucket)
    completed = len(self.completed_bucket)

    # TODO: if submission overhead is sufficiently high, we may need to account
    # for that when calculating whether or not we can submit more here given
    # the inflight cap and completion latency

    # If we are hitting our soft cap for inflight requests, don't submit now
    # TODO: consider decreasing max_inflight
    # TODO: set a variable indicating that this has happened
    if inflight >= self.max_inflight:
        return 0

    # If there are enough completed requests, don't submit any more
    # Since we may have overshot,
    # TODO: consider decreasing completion_target_distance
    if completed >= self.completion_target_distance - self.min_dispatch:
        return 0

    # TODO: what would be good logic for inc/dec max_inflight?
    if inflight < 0.9 * self.max_inflight:
        desired_max_inflight = self.max_inflight + 1
        self.max_inflight = min(desired_max_inflight, self.inflight_cap)

    # If we are lagging too far behind on completed requests, it is time to
    # increase completion_target_distance.
    # TODO: account for this happening initially
    if completed < 0.25 * self.completion_target_distance:
        desired_completion_target_distance = self.completion_target_distance + 1
        self.completion_target_distance = min(desired_completion_target_distance,
                                              self.completed_cap)

    # TODO: some kind of boundary checking to ensure that our dispatch size
    # doesn't cause us to exceed our completed or inflight caps
    return self.min_dispatch


def try_algos(algo):
    pipeline = PipelineConfiguration(
        inflight_cap=1000,
        submission_overhead=1,
        max_iops=10000,
        base_completion_latency=1,
        consumption_rate=2,
        consumption_size=1,
        prefetch_distance_algorithm=algo1,
        completed_cap=20,
        completion_target_distance=15,
        min_dispatch=2,
        max_inflight=25,
    ).generate_pipeline()

    # data = pipeline.run(volume=1000, duration=1000)
    data = pipeline.run(volume=1000)

    to_plot = pd.DataFrame(index=data.index)
    to_plot['wait'] = data.apply(lambda record:
        record['completed_want_to_move'] > record['completed_num_ios'] +
                                 record['completed_to_move'], axis='columns')
    to_plot['completed'] = data['completed_num_ios']
    to_plot['inflight'] = data['inflight_num_ios']
    to_plot['consumed'] = data['consumed_num_ios']
    to_plot['completion_target_distance'] = data['prefetched_completion_target_distance']
    to_plot['max_inflight'] = data['prefetched_max_inflight']

    single_plot(to_plot)

try_algos(algo1)