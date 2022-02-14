from bucket import Pipeline, GateBucket, DialBucket, IntakeBucket, StopBucket


class TestPipeline(Pipeline):
    def __init__(self):
        self.intake = IntakeBucket("intake")
        self.submitted_bucket = SubmittedDialBucket("submitted")
        self.inflight_bucket = InflightDialBucket("inflight")
        self.completed_bucket = CompletedGateBucket("completed")
        self.consumed_bucket = StopBucket("consumed")

        self.prefetched_bucket = PrefetchedGateBucket(
            "prefetched", self.completed_bucket, self.inflight_bucket)

        super().__init__(self.intake, self.prefetched_bucket,
                         self.submitted_bucket, self.inflight_bucket,
                         self.completed_bucket, self.consumed_bucket)


class PrefetchedGateBucket(GateBucket):
    def __init__(self, name, completed_bucket, inflight_bucket):
        super().__init__(name)
        self.completed_bucket = completed_bucket
        self.inflight_bucket = inflight_bucket

        self.inflight_cap = 10000
        self.completed_cap = 10000

        # variables for prefetch algorithm under test
        self.completion_target_distance = 512
        self.min_dispatch = 2
        self.max_inflight = 10

        self.desired_move_size_override = None

    def wanted_move_size(self):
        if self.desired_move_size_override is not None:
            return self.desired_move_size_override(self)

        inflight = len(self.inflight_bucket)
        completed = len(self.completed_bucket)

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
    LATENCY = 0.1


class InflightDialBucket(DialBucket):
    MAX_IOPS = 10000
    BASE_COMPLETION_LATENCY = 1.2

    def latency(self):
        # TODO: make this formula better
        completion_latency = self.BASE_COMPLETION_LATENCY
        if len(self) + 1 >= self.MAX_IOPS:
            completion_latency = 10
        completion_latency += (0.01 * len(self))
        return self.BASE_COMPLETION_LATENCY


class CompletedGateBucket(GateBucket):
    CONSUMPTION_RATE = 1
    CONSUMPTION_SIZE = 1

    def wanted_move_size(self):
        if not self.tick % self.CONSUMPTION_RATE:
            return self.CONSUMPTION_SIZE

        return 0
