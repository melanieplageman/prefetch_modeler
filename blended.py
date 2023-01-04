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

@dataclass
class ChangeLogEntry:
    tick: int
    prefetch_distance: int
    throughput: float

class BlendedPrefetcher(ConstantDistancePrefetcher):
    headroom = 2
    target_idle_time = 20
    multiplier = 1 / 20_000_000

    def __init__(self, *args, **kwargs):
        self.prefetch_distance = self.headroom
        self.calculated_pfd = None

        self.waited_at = None
        self.last_adjusted = 0
        self.adjust = True

        self.tput_denom = 100000

        self.completion_log = []
        self.change_log = []

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

        old_pfd = self.prefetch_distance

        if self.waited_at is not None:
            wait_time = self.tick - self.waited_at
        else:
            wait_time = 0

        idle_time = sum(self.tick - io.completion_time for io in self.pipeline['completed'])
        idle_time -= self.headroom * self.target_idle_time

        delta = wait_time - idle_time

        multiplier = self.multiplier * (self.tick - self.last_adjusted)

        self.prefetch_distance += multiplier * delta

        if self.calculated_pfd is not None:
            self.prefetch_distance = self.calculated_pfd

        if old_pfd != self.prefetch_distance:
            # print(f'tick: {self.tick}. old pfd: {self.prefetch_distance}. new pfd: {self.prefetch_distance}')
            crate = self.completion_rate
            tput = crate if crate is not None else 0
            self.change_log.append(ChangeLogEntry(tick=self.tick,
                prefetch_distance=self.prefetch_distance, throughput=tput))

        self.adjust = False
        self.last_adjusted = self.tick

        self.info['delta'] = delta
        self.info['wait_time'] = wait_time
        self.info['idle_time'] = idle_time

        self.prefetch_distance = max(self.prefetch_distance, 0)
        return int(self.prefetch_distance)

    @property
    def cnc(self):
        return len(self.pipeline['completed'])

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

        consumed = self.pipeline['consumed']
        tput = self.completion_rate
        if tput is None:
            return
        avg_tput = 0
        avg_pfd = 0
        nentries = 0
        for io in consumed:
            if getattr(io, "cached", None) is not None:
                continue

            if getattr(io, "accounted", None) is not None:
                continue

            io.accounted = True

            if io.prefetch_distance == self.prefetch_distance:
                continue

            entry = self.change_log[-1]
            if tput > entry.throughput:
                continue
            avg_pfd += entry.prefetch_distance
            avg_tput += entry.throughput
            nentries += 1
        
        if nentries == 0:
            return

        avg_tput = avg_tput / nentries
        avg_pfd = avg_pfd / nentries

        if tput < avg_tput:
            self.calculated_pfd = avg_pfd
        else:
            self.calculated_pfd = None

    def next_action(self):
        return self.tick + 1 if self.adjust else super().next_action()

