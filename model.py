from bucket import Pipeline, GateBucket, DialBucket, IntakeBucket, StopBucket
from override import overrideable

class TestPipeline(Pipeline):
    def __init__(self):
        self.intake = IntakeBucket("intake")
        self.submitted_bucket = SubmittedDialBucket("submitted")
        self.inflight_bucket = InflightDialBucket("inflight")
        self.completed_bucket = CompletedGateBucket("completed")
        self.consumed_bucket = StopBucket("consumed")

        self.prefetched_bucket = PrefetchedGateBucket(
            "prefetched", self)

        super().__init__(self.intake, self.prefetched_bucket,
                         self.submitted_bucket, self.inflight_bucket,
                         self.completed_bucket, self.consumed_bucket)


class PrefetchedGateBucket(GateBucket):
    def __init__(self, name, pipeline):
        super().__init__(name)
        self.pipeline = pipeline

        self.inflight_cap = 10000
        self.completed_cap = 10000

        # variables for prefetch algorithm under test
        self.completion_target_distance = 512
        self.min_dispatch = 2
        self.max_inflight = 10

    @overrideable('PrefetchGateBucket.wanted_move_size')
    def wanted_move_size(self):
        inflight = len(self.pipeline.inflight_bucket)
        completed = len(self.pipeline.completed_bucket)

        if inflight >= self.max_inflight:
            return 0

        if completed >= self.completion_target_distance - self.min_dispatch:
            return 0

        return self.min_dispatch

    def run(self):
        super().run()
        self.tick_data['completion_target_distance'] = self.completion_target_distance
        self.tick_data['max_inflight'] = self.max_inflight
        self.tick_data['min_dispatch'] = self.min_dispatch


class SubmittedDialBucket(DialBucket):
    LATENCY = 1


class InflightDialBucket(DialBucket):
    MAX_IOPS = 10000
    BASE_COMPLETION_LATENCY = 1

    def latency(self):
        # TODO: make this formula better
        completion_latency = self.BASE_COMPLETION_LATENCY
        if len(self) + 1 >= self.MAX_IOPS:
            completion_latency = 10
        completion_latency += (0.01 * len(self))
        return self.BASE_COMPLETION_LATENCY


class CompletedGateBucket(GateBucket):
    def __init__(self, name):
        super().__init__(name)
        self.consumption_rate = 1

    @property
    def consumption_rate(self):
        return self._consumption_rate

    @consumption_rate.setter
    def consumption_rate(self, rate):
        self._consumption_rate = rate
        if self._consumption_rate < 1:
            self.consumption_interval = int(1 / self._consumption_rate)
            self.consumption_size = 1
            self.next_consumption = (self.tick or 0) + self.consumption_interval
        else:
            self.consumption_interval = 1
            self.consumption_size = int(self._consumption_rate)
            self.next_consumption = (self.tick or 0) + 0

    def next_action(self):
        return self.next_consumption

    def wanted_move_size(self):
        if self.tick != self.next_consumption:
            return 0

        if len(self.source) == 0 and self.consumption_rate < 1:
            self.next_consumption += 1
        else:
            self.next_consumption += self.consumption_interval

        return self.consumption_size
