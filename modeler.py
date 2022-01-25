import time, math
from dataclasses import dataclass

COMPLETION_TARGET_DISTANCE = 512
MAX_IN_FLIGHT = 1
MIN_DISPATCH = 2

CONSUMPTION_RATE = 1
BASE_COMPLETION_LATENCY = 1.2
SUBMISSION_OVERHEAD = 0.1

@dataclass
class IO:
    expiry: int = 0

    def is_expired(self, current_tick):
        return self.expiry <= current_tick

# TODO: sublcass an ABC class
class Bucket:
    def __init__(self):
        self.ios = []
        self.target_bucket = None

    def __repr__(self):
        return f"{type(self).__name__}()"

    @property
    def num_ios(self):
        return len(self.ios)

    def add(self, io, current_tick):
        io.expiry = current_tick + self.latency(self.num_ios)
        self.ios.append(io)

    def move(self, io, current_tick):
        self.ios.remove(io)
        self.target_bucket.add(io, current_tick)

    def latency(self, num_ios):
        return 0

    def run(self, current_tick):
        if not self.target_bucket:
            return

        for io in self.ios:
            if not io.is_expired(current_tick):
                continue
            print(f'tick: {current_tick}. moving io from {self} to {self.target_bucket}')
            self.move(io, current_tick)

class SubmittedBucket(Bucket):
    def latency(self, num_ios):
        return SUBMISSION_OVERHEAD

class InFlightBucket(Bucket):
    # Latency here is how long an IO will be inflight -- or completion latency
    def latency(self, num_ios):
        # TODO: make this change with the number of requests inflight
        completion_latency = BASE_COMPLETION_LATENCY
        print(f'num_ios is {num_ios}. completion latency is {completion_latency}')
        return completion_latency

class CompletedBucket(Bucket):
    pass

class Pipeline:
    def __init__(self, buckets):
        self.buckets = buckets

        for i in range(len(self.buckets) - 1):
            self.buckets[i].target_bucket = self.buckets[i + 1]

    def enqueue(self, item, current_tick):
        self.buckets[0].add(item, current_tick)

    def dequeue(self, current_tick):
        for io in self.buckets[-1].ios:
            if not io.is_expired(current_tick):
                continue
            self.buckets[-1].ios.remove(io)
            return io

    def run(self, current_tick):
        for bucket in self.buckets:
            bucket.run(current_tick)

class Scan:
    def __init__(self, pipeline, nblocks, initial_tick=0, consumption_rate=CONSUMPTION_RATE):
        self.pipeline = pipeline
        self.consumption_rate = consumption_rate
        self.last_consumption = initial_tick
        self.nblocks = nblocks
        self.submitted = 0
        self.tried_consumed = False
        self.acquired_io = None
        self.consumed = 0

    def should_consume(self, current_tick):
        if self.last_consumption + self.consumption_rate < current_tick:
            return False
        self.last_consumption = current_tick
        self.tried_consumed = True
        return True

    def get_io(self, current_tick):
        self.acquired_io = self.pipeline.dequeue(current_tick)
        return self.acquired_io

    def calc_submit_window(self, completed, inflight):
        if completed >= COMPLETION_TARGET_DISTANCE - MIN_DISPATCH:
            return 0

        if inflight >= MAX_IN_FLIGHT:
            return 0

        # Submit the lesser of the number of blocks left in the scan and
        # MIN_DISPATCH
        batch_size = min(self.nblocks - self.submitted, MIN_DISPATCH)

        return batch_size

    def submit_io(self, current_tick, num_to_submit):
        while num_to_submit > 0:
            self.pipeline.enqueue(IO(), current_tick)
            self.submitted += 1
            num_to_submit -= 1

    def run(self, current_tick, completed_bucket, inflight_bucket):
        scan.submit_io(current_tick, scan.calc_submit_window(completed_bucket.num_ios,
                                                             inflight_bucket.num_ios))

        if scan.should_consume(current_tick):
            if scan.get_io(current_tick):
                scan.consumed += 1

class ScanMeasurer:
    def __init__(self, scan):
        self.scan = scan
        self.tried_consumed = []
        self.waited = []
        self.acquired = []

    def __repr__(self):
        return f'TryConsume:{self.tried_consumed}\nAcquired:  {self.acquired}\nWaited:    {self.waited}\n'

    def measure(self):
        self.tried_consumed.append(int(self.scan.tried_consumed))
        self.acquired.append(int(self.scan.acquired_io is not None))
        self.waited.append(int(self.scan.acquired_io is None))

class BucketMeasurer:
    def __init__(self, bucket):
        self.before_ios = []
        self.after_ios = []
        self.bucket = bucket

    def measure_before_scan(self):
        self.before_ios.append(self.bucket.num_ios)

    def measure_after_scan(self):
        self.after_ios.append(self.bucket.num_ios)

    def __repr__(self):
        return f'Before: {self.before_ios}\nAfter:  {self.after_ios}\n'

class SubmittedMeasurer(BucketMeasurer):
    def __repr__(self):
        return 'Submitted:\n' + super().__repr__()

class InflightMeasurer(BucketMeasurer):
    def __repr__(self):
        return 'InFlight:\n' + super().__repr__()

class CompletedMeasurer(BucketMeasurer):
    def __repr__(self):
        return 'Completed:\n' + super().__repr__()

submitted_bucket = SubmittedBucket()
inflight_bucket = InFlightBucket()
completed_bucket = CompletedBucket()

buckets = [submitted_bucket, inflight_bucket, completed_bucket]
pipeline = Pipeline(buckets)

submitted_measurer = SubmittedMeasurer(submitted_bucket)
inflight_measurer = InflightMeasurer(inflight_bucket)
completed_measurer = CompletedMeasurer(completed_bucket)

bucket_measurers = [submitted_measurer, inflight_measurer, completed_measurer]

nblocks = 10
total_ticks = 10

scan = Scan(pipeline, nblocks)
scan_measurer = ScanMeasurer(scan)

for i in range(total_ticks):
    if scan.consumed >= scan.nblocks:
        break

    scan_measurer.measure()

    for bucket_measurer in bucket_measurers:
        bucket_measurer.measure_before_scan()

    scan.run(i, completed_bucket, inflight_bucket)

    for bucket_measurer in bucket_measurers:
        bucket_measurer.measure_after_scan()

    pipeline.run(i)

print(scan_measurer)
for bucket_measurer in bucket_measurers:
    print(bucket_measurer)
