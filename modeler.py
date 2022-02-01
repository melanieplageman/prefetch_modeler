import time, math
from dataclasses import dataclass
import logging

logger = logging.getLogger()
logger.setLevel('CRITICAL')

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

class Pipeline:
    def __init__(self, buckets):
        self.buckets = buckets + [Bucket()]

        for i in range(len(self.buckets) - 1):
            self.buckets[i].target_bucket = self.buckets[i + 1]

    def enqueue(self, item, current_tick):
        self.buckets[0].add(item, current_tick)

    def dequeue(self, current_tick):
        try:
            return self.buckets[-1].ios.pop()
        except IndexError:
            return None

    def run(self, current_tick):
        for bucket in self.buckets:
            bucket.run(current_tick)


class Scan:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.nblocks = 0
        self.consumed = 0
        self.acquired_io = None
        self.tried_consumed = False
        self.last_consumption = 0

    @property
    def done(self):
        return self.consumed >= self.nblocks

    def should_consume(self, tick):
        self.tried_consumed = self.last_consumption + self.consumption_rate < tick
        if self.tried_consumed:
            self.last_consumption = tick
        return self.tried_consumed

    def acquire_size(self, tick):
        return 1 if self.should_consume(tick) else 0

    def run(self, tick):
        acquire_size = self.acquire_size(tick)
        for i in range(acquire_size):
            self.acquired_io = self.pipeline.dequeue(tick)
            if self.acquired_io:
                self.consumed += 1

class Prefetcher:
    def __init__(self, scan, pipeline):
        self.pipeline = pipeline
        self.scan = scan
        self.submitted = 0

    def submit_size(self):
        return 1

    def run(self, tick):
        submit_size = self.submit_size()
        for i in range(submit_size):
            self.pipeline.enqueue(IO(), tick)
            self.submitted += 1

class Model:
    def __init__(self, pipeline, scan, prefetcher, measurer):
        self.pipeline = pipeline
        self.scan = scan
        self.measurer = measurer
        self.prefetcher = prefetcher

    def run(self, total_ticks):
        for i in range(total_ticks):
            if self.scan.done:
                break

            self.measurer.run_before_scan()

            self.scan.run(i)

            self.prefetcher.run(i)

            self.measurer.run_after_scan()

            self.pipeline.run(i)

        return self.measurer.analyze()
