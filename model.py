from bucket import (
    Pipeline, GateBucket, DialBucket, StopBucket, RateBucket,
    CapacityBucket, ThresholdBucket)
from units import Rate
from override import overrideable
import math
import itertools


class TestPipeline(Pipeline):
    def __init__(self):
        # variables for prefetch algorithm under test
        self.completion_target_distance = None
        self.min_dispatch = None

        super().__init__()


@TestPipeline.bucket('prefetched')
class AlgorithmBucket(GateBucket):
    @overrideable
    def min_dispatch(self):
        raise NotImplementedError()

    @overrideable
    def adjust(self):
        pass

    @overrideable
    def wanted_move_size(self):
        raise NotImplementedError()

    def run(self):
        if not self.tick == 0:
            self.adjust()
        super().run()


@TestPipeline.bucket('ringmaster')
class RingMaster(GateBucket):
    def wanted_move_size(self):
        in_progress = self.target.counter - len(self.pipeline['consumed'])
        self.tick_data['in_progress'] = in_progress
        return max(min(self.pipeline.cap_in_progress - in_progress, len(self)), 0)

    def next_action(self):
        in_progress = self.target.counter - len(self.pipeline['consumed'])
        if len(self) > 0 and in_progress < self.pipeline.cap_in_progress:
            return self.tick + 1
        return math.inf


@TestPipeline.bucket('invoked')
class InvokeBucket(ThresholdBucket):
    pass


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
