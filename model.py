from bucket import (
    Pipeline, GateBucket, DialBucket, IntakeBucket, StopBucket, RateBucket,
    CapacityBucket)
from units import Rate
from override import overrideable
import math


class TestPipeline(Pipeline):
    def __init__(self):
        # variables for prefetch algorithm under test
        self.completion_target_distance = 512
        self.min_dispatch = 2
        self.target_inflight = 10

        self.cnc_ctd_ratio = 1

        super().__init__()


# @TestPipeline.bucket('prefetched')
# class PrefetchBucket(GateBucket):
#     def wanted_move_size(self):
#         # TODO: make this consider submitted, waiting to be inflight, and
#         # inflight?
#         total_inflight = len(self.pipeline['inflight']) + len(self.pipeline['inflight_latency'])
#         completed_not_consumed = len(self.pipeline['completed'])

#         if total_inflight >= self.pipeline.target_inflight - self.min_dispatch:
#             return 0

#         if completed_not_consumed + total_inflight + self.min_dispatch >= self.pipeline.completion_target_distance:
#             return 0

#         target = self.pipeline.completion_target_distance - total_inflight - completed_not_consumed

#         target = max(self.min_dispatch, target)
#         to_submit = min(self.pipeline.target_inflight - total_inflight, target)
#         # print(f'target: {target}. min_dispatch: {self.min_dispatch}. target inflight: {self.pipeline.target_inflight}. total_inflight: {total_inflight}. to_submit is {to_submit}')
#         return to_submit

#     def run(self):
#         super().run()
#         self.tick_data['completion_target_distance'] = self.pipeline.completion_target_distance
#         self.tick_data['target_inflight'] = self.pipeline.target_inflight
#         self.tick_data['min_dispatch'] = self.min_dispatch


@TestPipeline.bucket('submitted')
class SubmitBucket(DialBucket):
    pass


@TestPipeline.bucket('inflight')
class InflightRateBucket(CapacityBucket):
    pass


@TestPipeline.bucket('inflight_latency')
class InflightLatencyBucket(DialBucket):
    pass


@TestPipeline.bucket('completed')
class CompleteBucket(RateBucket):
    pass


TestPipeline.bucket('consumed')(StopBucket)
