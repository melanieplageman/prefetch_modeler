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
class CompletionLogEntry:
    tick: int
    number: int

class VariableDistancePrefetcher(ConstantDistancePrefetcher):
    headroom = 2
    target_idle_time = 20
    multiplier = 1 / 20_000_000

    def __init__(self, *args, **kwargs):
        self.prefetch_distance = 8

        self.waited_at = None
        self.last_adjusted = 0
        self.adjust = False

        self.tput_denom = 100000

        self.completion_log = []

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
        from itertools import takewhile
        newlog = takewhile(
            lambda entry: entry.tick > self.tick - self.tput_denom,
            reversed(self.completion_log)
        )

        nios = sum(entry.number for entry in newlog)
        return float(nios / self.tput_denom)

    @property
    def in_storage(self):
        return len(self.pipeline['minimum_latency']) + \
            len(self.pipeline['inflight']) + \
            len(self.pipeline['deadline'])

    def max_buffers(self):
        if self.adjust is False:
            return int(self.prefetch_distance)

        new_pfd = self.prefetch_distance

        if self.waited_at is not None:
            wait_time = self.tick - self.waited_at
        else:
            wait_time = 0

        idle_time = sum(self.tick - io.completion_time for io in self.pipeline['completed'])
        idle_time -= self.headroom * self.target_idle_time

        delta = wait_time - idle_time

        multiplier = self.multiplier * (self.tick - self.last_adjusted)

        new_pfd += multiplier * delta
        new_pfd = max(new_pfd, 2)

        self.prefetch_distance = new_pfd
        self.adjust = False
        self.last_adjusted = self.tick

        self.info['delta'] = delta
        self.info['wait_time'] = wait_time
        self.info['idle_time'] = idle_time

        return int(self.prefetch_distance)

    @property
    def cnc(self):
        return len(self.pipeline['completed'])

    def reaction(self):
        # Determine current wait time
        if self.waited_at is None and self.cnc < self.headroom:
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

    def next_action(self):
        return self.tick + 1 if self.adjust else super().next_action()
