from bucket import GateBucket, DialBucket

class PrefetchedGateBucket(GateBucket):
    def __init__(self, completed_bucket, inflight_bucket):
        super().__init__()
        self.completed_bucket = completed_bucket
        self.inflight_bucket = inflight_bucket

        self.inflight_cap = 10000
        self.completed_cap = 10000

        # variables for prefetch algorithm under test
        self.completion_target_distance = 512
        self.min_dispatch = 2
        self.max_inflight = 10

        self.desired_move_size_override = None

    def desired_move_size(self, tick):
        if self.desired_move_size_override is not None:
            return self.desired_move_size_override(self, tick)

        inflight = self.inflight_bucket.num_ios
        completed = self.completed_bucket.num_ios

        if inflight >= self.max_inflight:
            return 0

        if completed >= self.completion_target_distance - self.min_dispatch:
            return 0

        return self.min_dispatch

    def run(self, tick):
        super().run(tick)
        self.measurer['completion_target_distance'] = self.completion_target_distance
        self.measurer['max_inflight'] = self.max_inflight
        self.measurer['min_dispatch'] = self.min_dispatch

class SubmittedDialBucket(DialBucket):
    SUBMISSION_OVERHEAD = 0.1

    def latency(self):
        return self.SUBMISSION_OVERHEAD

class InflightDialBucket(DialBucket):
    MAX_IOPS = 10000
    BASE_COMPLETION_LATENCY = 1.2

    def latency(self):
        # TODO: make this formula better
        completion_latency = self.BASE_COMPLETION_LATENCY
        if self.num_ios + 1 >= self.MAX_IOPS:
            completion_latency = 10
        completion_latency += (0.01 * self.num_ios)
        return self.BASE_COMPLETION_LATENCY

class CompletedGateBucket(GateBucket):
    CONSUMPTION_RATE = 1
    CONSUMPTION_SIZE = 1

    last_consumption = 0

    def desired_move_size(self, tick):
        try_consume = tick >= self.last_consumption + self.CONSUMPTION_RATE
        if try_consume:
            self.last_consumption = tick
            return self.CONSUMPTION_SIZE

        return 0

class ConsumedGateBucket(GateBucket):
    def desired_move_size(self, tick):
        return 0
