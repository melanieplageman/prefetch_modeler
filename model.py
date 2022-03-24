from bucket import Pipeline, GateBucket, DialBucket, IntakeBucket, StopBucket
from units import Rate
from override import overrideable
import math

class TestPipeline(Pipeline):
    def __init__(self):

        self.intake = IntakeBucket("intake", self)
        self.prefetched_bucket = PrefetchBucket("prefetched", self)
        self.submitted_bucket = SubmitBucket("submitted", self)
        self.waited_bucket = WaitBucket("waited", self)
        self.inflight_bucket = InflightBucket("inflight", self)
        self.completed_bucket = CompleteBucket("completed", self)
        self.consumed_bucket = StopBucket("consumed", self)

        # variables for prefetch algorithm under test
        self.completion_target_distance = 512
        self.min_dispatch = 2
        self.target_inflight = 10


        super().__init__(self.intake, self.prefetched_bucket,
                         self.submitted_bucket, self.waited_bucket,
                         self.inflight_bucket,
                         self.completed_bucket, self.consumed_bucket)

class PrefetchBucket(GateBucket):
    @overrideable
    def wanted_move_size(self):
        inflight = len(self.pipeline.inflight_bucket)
        completed_not_consumed = len(self.pipeline.completed_bucket)

        if inflight >= self.pipeline.target_inflight:
            return 0

        if completed_not_consumed >= self.pipeline.completion_target_distance - self.min_dispatch:
            return 0

        return self.min_dispatch

    def run(self):
        super().run()
        self.tick_data['completion_target_distance'] = self.pipeline.completion_target_distance
        self.tick_data['target_inflight'] = self.pipeline.target_inflight
        self.tick_data['min_dispatch'] = self.min_dispatch


class SubmitBucket(DialBucket):
    LATENCY = 1

# Remember next_action() is infinity but is actually whenever InflightBucket
# has its next action, so, if we ever try to be clever about executing some
# buckets and not others, we'll have to handle this
class WaitBucket(GateBucket):
    def wanted_move_size(self):
        return min(len(self), max(0, self.target.max_iops - len(self.target)))

class InflightBucket(DialBucket):
    def __init__(self, *args, **kwargs):
        self.max_iops = 10
        self.base_completion_latency = 1
        super().__init__(*args, **kwargs)

    @overrideable
    def latency(self):
        # TODO: make this formula better
        return int(self.base_completion_latency + (0.01 * len(self)))


# TODO: Turn into a subclass of bucket instead
class CompleteBucket(GateBucket):
    def __init__(self, *args, **kwargs):
        self._consumption_interval = 0
        self._last_consumption = 0
        super().__init__(*args, **kwargs)

    @overrideable
    def consumption_rate(self):
        return Rate(per_second=1000)

    def wanted_move_size(self):
        # Consumption rate may change based on time elapsed or blocks consumed.
        consumption_rate = self.consumption_rate().value
        if consumption_rate >= 1:
            raise ValueError(f'Value {self.consumption_rate} exceeds maximum consumption rate of 1 IO per microsecond. Try a slower rate.')
        self._consumption_interval = int(1 / consumption_rate)

        # If it is not time to consume, we don't want to move any IOs
        if self.tick < self._last_consumption + self._consumption_interval:
            return 0

        # we want to consume and we can. though doing this consumption may mean
        # that our consumption rate changes since consumption rate can be based
        # on # requests consumed,
        if len(self):
            self._last_consumption = self.tick
        return 1

    def next_action(self):
        if not self.source:
            return math.inf

        # self._last_consumption + self._consumption_interval is the next tick
        # that this bucket should consume, if this bucket has been full since
        # the last time it consumed.
        return max(self._last_consumption + self._consumption_interval,
                   self.tick + 1)
