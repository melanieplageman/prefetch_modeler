import time, math
from dataclasses import dataclass
import logging
from config import *

logger = logging.getLogger()
logger.setLevel('CRITICAL')

MAX_IN_FLIGHT = 10
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
            logger.warning('tick: %s. moving io from %s to %s',
                           str(current_tick), str(self),
                           str(self.target_bucket))
            self.move(io, current_tick)

class SubmittedBucket(Bucket):
    def latency(self, num_ios):
        return SUBMISSION_OVERHEAD

class InFlightBucket(Bucket):
    # Latency here is how long an IO will be inflight -- or completion latency
    def latency(self, num_ios):
        # TODO: make this change with the number of requests inflight
        completion_latency = BASE_COMPLETION_LATENCY
        completion_latency = (0.01 * num_ios) + BASE_COMPLETION_LATENCY
        logger.warning('num_ios is %s. completion latency is %s.', num_ios,
                   completion_latency)
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
    def __init__(self, pipeline, nblocks, initial_tick=0,
                 consumption_rate=CONSUMPTION_RATE):
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
        inputs = config['adjustment_algorithm']['inputs']

        completion_target_distance = inputs['completion_target_distance']
        min_dispatch = inputs['min_dispatch']
        max_in_flight = inputs['max_in_flight']

        if completed >= completion_target_distance - min_dispatch:
            return 0

        if inflight >= max_in_flight:
            return 0

        # Submit the lesser of the number of blocks left in the scan and
        # MIN_DISPATCH
        batch_size = min(self.nblocks - self.submitted, min_dispatch)

        return batch_size

    def submit_io(self, current_tick, num_to_submit):
        while num_to_submit > 0:
            self.pipeline.enqueue(IO(), current_tick)
            self.submitted += 1
            num_to_submit -= 1

    def run(self, current_tick, completed_bucket, inflight_bucket):
        self.submit_io(current_tick,
                       self.calc_submit_window(completed_bucket.num_ios,
                                               inflight_bucket.num_ios))

        if self.should_consume(current_tick):
            if self.get_io(current_tick):
                self.consumed += 1
