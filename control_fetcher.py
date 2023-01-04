from prefetch_modeler.core import ContinueBucket, GlobalCapacityBucket, RateBucket, \
Rate, Duration, ForkBucket, Bucket, GateBucket
from prefetch_modeler.prefetcher_type import ConstantRatePrefetcher
from constant_distance_prefetcher import ConstantDistancePrefetcher
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

@dataclass
class ChangeLogEntry:
    tick: int
    prefetch_distance: int
    throughput: float

class ControlFetcher(ConstantDistancePrefetcher):
    headroom = 20
    target_idle_time = 20

    def __init__(self, *args, **kwargs):
        self.prefetch_distance = 3
        self.last_adjusted = 0
        self.adjust_interval = 80000
        self.adjustment_size = 2
        self.waited_at = None
        self.adjust = True
        self.completion_log = []
        self.change_log = []
        self.headroom = 2

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

        self.info['delta'] = delta
        self.info['wait_time'] = wait_time
        self.info['idle_time'] = idle_time

        new_pfd = self.prefetch_distance * self.adjustment_size
        print(f'tick: {self.tick}. old pfd: {self.prefetch_distance}. new pfd: {new_pfd}')
        crate = self.completion_rate
        tput = crate if crate is not None else 0
        self.change_log.append(ChangeLogEntry(tick=self.tick,
            prefetch_distance=new_pfd, throughput=tput))
        self.prefetch_distance = new_pfd

        self.adjust = False
        self.last_adjusted = self.tick

        return int(self.prefetch_distance)

    @property
    def cnc(self):
        return len(self.pipeline['completed'])

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

    def reaction(self):
        # Determine current wait time
        if len(self) == 0:
            self.waited_at = None
        if self.waited_at is None and self.cnc < self.headroom:
            self.waited_at = self.tick
        elif self.waited_at is not None and self.cnc >= self.headroom:
            self.waited_at = None

        completed = self.pipeline['completed']
        consumed = self.pipeline['consumed']

        ncompleted = self.pipeline['deadline'].info['to_move']

        if ncompleted > 0:
            entry = CompletionLogEntry(tick=self.tick, number=ncompleted)
            self.completion_log.append(entry)

        # Record idle time on each completed not consumed IO
        # do this for cached IOs too?
        for io in completed:
            io.completion_time = getattr(io, "completion_time", self.tick)

        if self.last_adjusted + self.adjust_interval <= self.tick:
            self.adjust = True

    def next_action(self):
        return self.tick + 1 if self.adjust else super().next_action()
