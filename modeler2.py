from enum import Enum
import time, math

COMPLETION_TARGET_DISTANCE = 512
MAX_IN_FLIGHT = 1
MIN_DISPATCH = 2

# TODO: subclass a data class
class IO:
    def __init__(self):
        self.expiry = None

    def is_expired(self, current_tick):
        return self.expiry <= current_tick

    def __repr__(self):
        return f'IO. Entered: {str(self.expiry)}'

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
    # Submission overhead
    def latency(self, num_ios):
        return 0.1

class InFlightBucket(Bucket):
    def latency(self, num_ios):
        queue_depth = max(num_ios, 1)
        completion_latency = queue_depth * 1.2
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
    def __init__(self, pipeline, nblocks, initial_tick=0, consumption_rate=1):
        self.pipeline = pipeline
        self.consumption_rate = consumption_rate
        self.last_consumption = initial_tick
        self.nblocks = nblocks
        self.submitted = 0
        self.batch_size = 0
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

    def calc_submit_window(self, completed_bucket, inflight_bucket):
        if completed.num_ios >= COMPLETION_TARGET_DISTANCE - MIN_DISPATCH:
            return 0

        if inflight_bucket.num_ios >= MAX_IN_FLIGHT:
            return 0

        # Submit the lesser of the number of blocks left in the scan and
        # MIN_DISPATCH
        self.batch_size = min(self.nblocks - self.submitted, MIN_DISPATCH)

        return self.batch_size

    def submit_io(self, current_tick, num_to_submit):
        submitted = num_to_submit
        while num_to_submit > 0:
            self.pipeline.enqueue(IO(), current_tick)
            self.submitted += 1
            num_to_submit -= 1

        return submitted

    def run(self, current_tick, completed, inflight):

        scan.submit_io(current_tick, scan.calc_submit_window(completed, inflight))

        if scan.should_consume(current_tick):
            if scan.get_io(current_tick):
                scan.consumed += 1

class Measurer:
    def __init__(self, buckets, scan, pipeline):
        self.scan = scan
        self.buckets = buckets
        self.pipeline = pipeline
        # Can be bitmap
        self.tried_consumed = []
        self.acquired = []
        self.waited = []

        self.before_submitted = []
        self.before_inflight = []
        self.before_completed = []

        self.after_submitted = []
        self.after_inflight = []
        self.after_completed = []

    def __repr__(self):
        tried_consumed = f'TryConsume:{self.tried_consumed}\n'
        waited = f'Waited:    {self.waited}\n'
        acquired = f'Acquired:  {self.acquired}\n'

        submitted_before = f'Submitted Before: {self.before_submitted}\n'
        inflight_before = f'Inflight Before:  {self.before_inflight}\n'
        completed_before = f'Completed Before: {self.before_completed}\n'
        submitted_after = f'Submitted After: {self.after_submitted}\n'
        inflight_after = f'Inflight After:  {self.after_inflight}\n'
        completed_after = f'Completed After: {self.after_completed}\n'
        return tried_consumed + waited + acquired + submitted_before + inflight_before + completed_before + submitted_after + inflight_after + completed_after

    def measure_before_scan(self):
        self.before_submitted.append(self.buckets[0].num_ios)
        self.before_inflight.append(self.buckets[1].num_ios)
        self.before_completed.append(self.buckets[2].num_ios)

    def measure_after_scan(self):
        self.tried_consumed.append(int(self.scan.tried_consumed))
        self.acquired.append(int(self.scan.acquired_io is not None))
        self.waited.append(int(self.scan.acquired_io is None))

        self.after_submitted.append(self.buckets[0].num_ios)
        self.after_inflight.append(self.buckets[1].num_ios)
        self.after_completed.append(self.buckets[2].num_ios)

submitted = SubmittedBucket()
inflight = InFlightBucket()
completed = CompletedBucket()

buckets = [submitted, inflight, completed]

pipeline = Pipeline(buckets)
nblocks = 10
total_ticks = 10

scan = Scan(pipeline, nblocks)

measurer = Measurer(buckets, scan, pipeline)

# or until condition met
for i in range(total_ticks):
    if scan.consumed >= scan.nblocks:
        break
    measurer.measure_before_scan()
    scan.run(i, completed, inflight)
    measurer.measure_after_scan()
    pipeline.run(i)

print(measurer)
