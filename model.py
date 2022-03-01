from bucket import Pipeline, GateBucket, DialBucket, IntakeBucket, StopBucket
from units import Rate
from override import overrideable

class TestPipeline(Pipeline):
    def __init__(self):

        self.intake = IntakeBucket("intake", self)
        self.prefetched_bucket = PrefetchBucket("prefetched", self)
        self.submitted_bucket = SubmitBucket("submitted", self)
        self.inflight_bucket = InflightBucket("inflight", self)
        self.completed_bucket = CompleteBucket("completed", self)
        self.consumed_bucket = StopBucket("consumed", self)

        # variables for prefetch algorithm under test
        self.completion_target_distance = 512
        self.min_dispatch = 2
        self.target_inflight = 10


        super().__init__(self.intake, self.prefetched_bucket,
                         self.submitted_bucket, self.inflight_bucket,
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

class InflightBucket(DialBucket):
    max_iops = 10000
    base_completion_latency = 1

    @overrideable
    def latency(self):
        # TODO: make this formula better
        completion_latency = self.base_completion_latency
        if len(self) + 1 >= self.max_iops:
            completion_latency = 10
        completion_latency += (0.01 * len(self))
        return self.base_completion_latency


class CompleteBucket(GateBucket):
    def __init__(self, *args, **kwargs):
        self._next_consumption = 0
        self._consumption_interval = 0
        self._last_consumption = 0
        super().__init__(*args, **kwargs)

    @overrideable
    def consumption_rate(self):
        return Rate(per_second=1000)

    def next_action(self):
        # Because consumption rate can be based on time elapsed, it must be
        # recalculated on every tick (as opposed to on every consumption) in
        # case it would change whether or not a consumption is required on this
        # tick.
        consumption_rate = self.consumption_rate().value
        if consumption_rate >= 1:
            raise ValueError(f'Value {self.consumption_rate} exceeds maximum consumption rate of 1 IO per microsecond. Try a slower rate.')
        # Consumption rate has changed based on time elapsed.
        consumption_interval = int(1 / consumption_rate)
        if self._consumption_interval != consumption_interval:
            self._consumption_interval = consumption_interval
            self._next_consumption = max(self.tick + 1, self._last_consumption + self._consumption_interval)

        return self._next_consumption

    def wanted_move_size(self):
        # If it is not time to consume, we don't want to move any IOs
        if self.tick != self._next_consumption:
            return 0

        # TODO: this probably needs to go outside of this function since this
        # returns what we want and shouldn't know about what we have?

        # we want to consume and we can
        # though doing this consumption may mean that our consumption rate
        # changes since consumption rate can be based on # requests consumed,
        # we will anyway recalculate the consumption rate on every tick, so we
        # will find out then.
        if len(self):
            self._last_consumption = self.tick
            self._next_consumption += self._consumption_interval

        # We want to consume but there are no completed IOs to move, this is a
        # wait and we'll try again next tick.
        else:
            self._next_consumption += 1

        return 1
