import pandas as pd
from pipeline_configurer import *
from model import *
from bucket import *
from plot import single_plot

# TODO: at IO completion, issue more if previously limited by max_inflight

# TODO: add minimums for completion_target_distance, max_inflight, min_dispatch


def algo1(self):
    intake = len(self.pipeline.intake)
    prefetched = len(self.pipeline.prefetched_bucket)
    submitted = len(self.pipeline.submitted_bucket)
    inflight = len(self.pipeline.inflight_bucket)
    completed = len(self.pipeline.completed_bucket)
    consumed = len(self.pipeline.consumed_bucket)
    print(f'intake: {intake}, '
          f'prefetched: {prefetched}, '
          f'submitted: {submitted}, '
          f'inflight: {inflight}, '
          f'completed: {completed}, '
          f'consumed: {consumed}.'
          )

    # TODO: if submission overhead is sufficiently high, we may need to account
    # for that when calculating whether or not we can submit more here given
    # the inflight cap and completion latency

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
    print(f"[{self.tick}] target dist: {self.completion_target_distance}, inflight: {inflight}, completed: {completed}, submitting: {to_submit}")
    return to_submit

# For now, you must specify whole numbers for Duration and Rate
def try_algos(algo):
    config = PipelineConfiguration(
        inflight_cap=100,
        submission_overhead=Duration(microseconds=10),
        max_iops=100,
        base_completion_latency=Duration(microseconds=400),
        consumption_rate=Rate(per_second=50000),
        prefetch_distance_algorithm=algo1,
        completed_cap=200,
        completion_target_distance=15,
        min_dispatch=2,
        max_inflight=10,
    )
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

try_algos(algo1)
