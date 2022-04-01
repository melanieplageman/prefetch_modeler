from bucket import (
    Pipeline, GateBucket, DialBucket, StopBucket, RateBucket,
    TargetCapacityBucket, ThresholdBucket, Bucket, GlobalCapacityBucket)
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


# @TestPipeline.bucket('test')
# class TestBucket(GateBucket):
#     def wanted_move_size(self):
#         return 10


@TestPipeline.bucket('baseline_all')
class BaselineAllBucket(GateBucket):
    def wanted_move_size(self):
        return len(self)


# @TestPipeline.bucket('baseline_sync')
# class BaselineSyncBucket(GateBucket):
#     def wanted_move_size(self):
#         if self.tick == 0:
#             return 0
#         if self.pipeline['completed'].next_action() == math.inf:
#             return 0
#         return 1


@TestPipeline.bucket('remaining')
class AlgorithmBucket(GateBucket):
    """
    A bucket that will move the number of IOs specified by an algorithm, with
    the option of modifying the algorithm on each run.
    """
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


@TestPipeline.bucket('awaiting_buffer')
class RingMaster(GlobalCapacityBucket):
    pass


@TestPipeline.bucket('w_claimed_buffer')
class InvokeBucket(ThresholdBucket):
    pass


@TestPipeline.bucket('kernel_batch')
class SubmitBucket(DialBucket):
    pass


@TestPipeline.bucket('submitted')
class InflightRateBucket(TargetCapacityBucket):
    pass


@TestPipeline.bucket('inflight')
class InflightLatencyBucket(DialBucket):
    pass


@TestPipeline.bucket('completed')
class CompleteBucket(RateBucket):
    pass


TestPipeline.bucket('consumed')(StopBucket)
