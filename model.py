from bucket import GateBucket, DialBucket

class PrefetchedGateBucket(GateBucket):
    def __init__(self, completed_bucket, inflight_bucket):
        super().__init__()
        self.completed_bucket = completed_bucket
        self.inflight_bucket = inflight_bucket

        # variables for prefetch algorithm under test
        self.completion_target_distance = 512
        self.min_dispatch = 2
        self.max_inflight = 10

    def desired_move_size(self, tick):
        completed = self.completed_bucket.num_ios
        if completed >= self.completion_target_distance - self.min_dispatch:
            self.completion_target_distance += 1
            return 0

        if self.inflight_bucket.num_ios >= self.max_inflight:
            return 0

        return self.min_dispatch

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
        return 1

class CompletedGateBucket(GateBucket):
    CONSUMPTION_RATE = 1

    last_consumption = 0

    def desired_move_size(self, tick):
        try_consume = tick >= self.last_consumption + self.CONSUMPTION_RATE
        if try_consume:
            self.last_consumption = tick
            return 1

        return 0

class ConsumedGateBucket(GateBucket):
    def desired_move_size(self, tick):
        return 0
