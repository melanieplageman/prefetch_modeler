from prefetch_modeler.core import ContinueBucket, GlobalCapacityBucket, RateBucket, \
Rate, Duration, ForkBucket, Bucket, GateBucket
from prefetch_modeler.prefetcher_type import ConstantRatePrefetcher
from dataclasses import dataclass
from fractions import Fraction
import itertools
import math
from numpy import mean

class ConstantDistancePrefetcher(GlobalCapacityBucket):
    name = 'cd_fetcher'
    prefetch_distance = 100

    @classmethod
    def hint(cls):
        return (1, cls.__name__)

    def max_buffers(self):
        return self.prefetch_distance

@dataclass
class LatencyLogEntry:
    tick: int
    in_storage: int
    latency: float

@dataclass
class WaitLogEntry:
    tick: int
    in_storage: int
    wait: float

@dataclass
class CompletionLogEntry:
    tick: int
    number: int

class VariableDistancePrefetcher(ConstantDistancePrefetcher):
    headroom = 2
    target_idle_time = 20
    multiplier = 1 / 20_000_000

    def __init__(self, *args, **kwargs):
        self.prefetch_distance = self.headroom
        self.latency_log = []
        self.wait_log = []
        self.completion_log = []

        self.waited_at = None
        self.last_adjusted = 0
        self.adjust = True

        super().__init__(*args, **kwargs)

    def remove(self, io):
        if not getattr(io, "cached", False):
            if self.waited_at is not None:
                io.wait_time = self.tick - self.waited_at
            else:
                io.wait_time = 0
            io.submission_time = self.tick
            io.prefetch_distance = self.prefetch_distance
        return super().remove(io)

    @property
    def completion_rate(self):
        if not self.completion_log:
            return None
        denom = 100000
        from itertools import takewhile
        newlog = takewhile(
            lambda entry: entry.tick > self.tick - denom,
            reversed(self.completion_log)
        )

        nios = sum(entry.number for entry in newlog)
        return float(nios / denom)

    @property
    def in_storage(self):
        return len(self.pipeline['minimum_latency']) + \
            len(self.pipeline['inflight']) + \
            len(self.pipeline['deadline'])

    def max_buffers(self):
        if self.adjust is False:
            return int(self.prefetch_distance)

        if self.waited_at is not None:
            wait_time = self.tick - self.waited_at
        else:
            wait_time = 0

        idle_time = sum(self.tick - io.completion_time for io in self.pipeline['completed'])
        idle_time -= self.headroom * self.target_idle_time

        delta = wait_time - idle_time

        multiplier = self.multiplier * (self.tick - self.last_adjusted)

        self.prefetch_distance += multiplier * delta
        self.prefetch_distance = max(self.prefetch_distance, 0)

        self.adjust = False
        self.last_adjusted = self.tick

        self.info['delta'] = delta
        self.info['wait_time'] = wait_time
        self.info['idle_time'] = idle_time

        if self.in_storage > 0:
            wait_benefit = float(wait_time / self.in_storage)
        else:
            wait_benefit = None

        self.info['wait_benefit'] = wait_benefit
        if wait_benefit is not None:
            new_wait_log = WaitLogEntry(tick=self.tick, in_storage=self.in_storage, wait=wait_time)

            if len(self.wait_log) > 1:
                self.info['wait_benefit_dt'] = self.wait_dt(self.wait_log[-1],
                                                            new_wait_log)
            if wait_benefit is not None:
                self.wait_log.append(new_wait_log)

        return int(self.prefetch_distance)

    @property
    def cnc(self):
        return len(self.pipeline['completed'])

    def avg_real_io_latency(self, ios):
        real_ios = [io for io in ios if getattr(io, 'cached', None) is None]
        if not real_ios:
            return None
        completion_latencies = [io.completion_time - io.submission_time for io in real_ios]
        return mean(completion_latencies)

    def wait_dt(self, old, new):
        old_wait_benefit = 0
        if old.in_storage > 0:
            old_wait_benefit = float(old.wait / old.in_storage)

        new_wait_benefit = 0
        if new.in_storage > 0:
            new_wait_benefit = float(new.wait / new.in_storage)

        time_delta =  new.tick - old.tick
        if time_delta > 0:
            return (new_wait_benefit - old_wait_benefit) / time_delta
        return 0

    def latency_dt(self, old, new):
        old_latency_cost = float(old.latency / old.in_storage)

        new_latency_cost = float(new.latency / new.in_storage)

        time_delta =  new.tick - old.tick
        if time_delta > 0:
            return (new_latency_cost - old_latency_cost) / time_delta
        return 0

    def reaction(self):
        # Determine current wait time
        if len(self) == 0:
            self.waited_at = None
        elif self.waited_at is None and self.cnc < self.headroom:
            self.waited_at = self.tick
        elif self.waited_at is not None and self.cnc >= self.headroom:
            self.waited_at = None

        completed = self.pipeline['completed']

        # Don't adjust unless we have a consumption
        self.adjust = completed.info['to_move'] > 0

        # Record idle time on each completed not consumed IO
        for io in completed:
            io.completion_time = getattr(io, "completion_time", self.tick)

        ncompleted = self.pipeline['deadline'].info['to_move']

        if ncompleted > 0:
            entry = CompletionLogEntry(tick=self.tick, number=ncompleted)
            self.completion_log.append(entry)

        avg_total_latency = self.avg_real_io_latency(completed)

        if avg_total_latency is None or self.in_storage == 0:
            return

        self.info['latency_cost'] = float(avg_total_latency / self.in_storage)

        new_latency_log = LatencyLogEntry(tick=self.tick, in_storage=self.in_storage, latency=avg_total_latency)

        if len(self.latency_log) > 1:
            self.info['latency_cost_dt'] = self.latency_dt(self.latency_log[-1],
                                                    new_latency_log)

        self.latency_log.append(new_latency_log)


    def next_action(self):
        return self.tick + 1 if self.adjust else super().next_action()


class TestConstantPrefetcher(ConstantRatePrefetcher):
    og_rate = Rate(per_second=2000)

    def remove(self, io):
        io.submission_time = self.tick
        super().remove(io)

    def reaction(self):
        completed = self.pipeline['completed']

        for io in self.pipeline['completed']:
            io.completion_time = getattr(io, "completion_time", self.tick)

