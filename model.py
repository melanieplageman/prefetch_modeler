from bucket import Pipeline, GateBucket, DialBucket, IntakeBucket, StopBucket, RateBucket
from units import Rate
from override import overrideable
import math

class TestPipeline(Pipeline):
    def __init__(self):

        self.intake = IntakeBucket("intake", self)
        self.prefetched_bucket = PrefetchBucket("prefetched", self)
        self.submitted_bucket = SubmitBucket("submitted", self)
        self.inflight_bucket = InflightRateBucket("inflight", self)
        self.inflight_latency_bucket = InflightLatencyBucket("inflight_latency", self)
        self.completed_bucket = CompleteBucket("completed", self)
        self.consumed_bucket = StopBucket("consumed", self)

        # variables for prefetch algorithm under test
        self.completion_target_distance = 512
        self.min_dispatch = 2
        self.target_inflight = 10

        self.cnc_ctd_ratio = 1


        super().__init__(self.intake, self.prefetched_bucket,
                         self.submitted_bucket,
                         self.inflight_bucket,
                         self.inflight_latency_bucket,
                         self.completed_bucket, self.consumed_bucket)

class PrefetchBucket(GateBucket):
    def wanted_move_size(self):
        # TODO: make this consider submitted, waiting to be inflight, and
        # inflight?
        total_inflight = len(self.pipeline.inflight_bucket) + len(self.pipeline.inflight_latency_bucket)
        completed_not_consumed = len(self.pipeline.completed_bucket)

        if total_inflight >= self.pipeline.target_inflight - self.min_dispatch:
            return 0

        if completed_not_consumed + total_inflight + self.min_dispatch >= self.pipeline.completion_target_distance:
            return 0

        target = self.pipeline.completion_target_distance - total_inflight - completed_not_consumed

        target = max(self.min_dispatch, target)
        to_submit = min(self.pipeline.target_inflight - total_inflight, target)
        # print(f'target: {target}. min_dispatch: {self.min_dispatch}. target inflight: {self.pipeline.target_inflight}. total_inflight: {total_inflight}. to_submit is {to_submit}')
        return to_submit

    def run(self):
        super().run()
        self.tick_data['completion_target_distance'] = self.pipeline.completion_target_distance
        self.tick_data['target_inflight'] = self.pipeline.target_inflight
        self.tick_data['min_dispatch'] = self.min_dispatch


class SubmitBucket(DialBucket):
    def __init__(self, *args, **kwargs):
        self.submission_overhead = 1
        super().__init__(*args, **kwargs)


class InflightLatencyBucket(DialBucket):
    def __init__(self, *args, **kwargs):
        self.base_completion_latency = 1
        super().__init__(*args, **kwargs)


class InflightRateBucket(RateBucket):
    def __init__(self, *args, **kwargs):
        self.max_iops = 10
        super().__init__(*args, **kwargs)

    def rate(self):
        return Rate(per_second=self.max_iops).value


class CompleteBucket(RateBucket):
    @overrideable
    def consumption_rate(self):
        return Rate(per_second=1000)

    def rate(self):
        return self.consumption_rate().value
