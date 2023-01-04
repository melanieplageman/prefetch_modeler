from prefetch_modeler.core import ContinueBucket, GlobalCapacityBucket, RateBucket, \
Rate, Duration, ForkBucket, Bucket, GateBucket
from prefetch_modeler.prefetcher_type import ConstantRatePrefetcher
from dataclasses import dataclass
from fractions import Fraction
import itertools
import math
from numpy import mean
from itertools import takewhile

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
    submitted: int
    consumed: int
    throughput_interval_start: int
    throughput_interval_end: int
    wait_time: int
    idle_time: int
    prefetch_ratio: float
    tput_ratio: float

class RollbackFetcher(ConstantDistancePrefetcher):
    headroom = 2
    target_idle_time = 20
    multiplier = 1 / 20_000_000

    def __init__(self, *args, **kwargs):
        self.prefetch_distance = 8

        self.last_tput = None
        self.tput_denom = 100000

        self.waited_at = None
        self.last_adjusted = 0
        self.adjust = False

        self.completion_log = []
        self.change_log = [ChangeLogEntry(tick=0,
            prefetch_distance=self.prefetch_distance, submitted=0, consumed=0,
            throughput_interval_start=0, throughput_interval_end=0,
            wait_time=0, idle_time=0, prefetch_ratio=0, tput_ratio=0)]

        super().__init__(*args, **kwargs)

    def remove(self, io):
        if not getattr(io, "cached", False):
            if self.waited_at is not None:
                io.wait_time = self.tick - self.waited_at
            else:
                io.wait_time = 0
            io.submission_time = self.tick
            io.prefetch_distance = self.prefetch_distance
            for entry in reversed(self.change_log):
                if entry.prefetch_distance == io.prefetch_distance:
                    entry.submitted += 1
                    break
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

        self.change_log[-1].wait_time = wait_time
        self.change_log[-1].idle_time = idle_time

        if self.current_pfd_entry.consumed < int(self.current_pfd_entry.prefetch_distance):
            raise ValueError(f"should only adjust after consuming current pfd # IOs. current pfd: {self.prefetch_distance} or {self.current_pfd_entry.prefetch_distance}. # consumed: {self.current_pfd_entry.consumed}")

        if len(self.change_log) > 1:
            previous_pfd_entry = self.change_log[-2]
            current_pfd_entry = self.change_log[-1]

            previous_pfd = previous_pfd_entry.prefetch_distance
            previous_duration = previous_pfd_entry.throughput_interval_end - previous_pfd_entry.throughput_interval_start
            previous_tput = previous_pfd_entry.consumed / previous_duration

            current_pfd = current_pfd_entry.prefetch_distance
            current_duration = current_pfd_entry.throughput_interval_end - current_pfd_entry.throughput_interval_start
            current_tput = current_pfd_entry.consumed / current_duration

            prefetch_ratio = current_pfd / previous_pfd
            tput_ratio = current_tput / previous_tput
            # print(f"current avg tput: {current_avg_tput}. previous avg tput: {previous_avg_tput}")
            # if prefetch_ratio > 1:
            print(f"self.tick: {self.tick}. prefetch_ratio: {format(prefetch_ratio, '.2f')}. tput_ratio: {format(tput_ratio, '.2f')}. wait time: {wait_time}. idle time: {idle_time}. suggested_new_pfd: {format(new_pfd, '.2f')}. current_pfd: {format(current_pfd, '.2f')}. previous_pfd: {format(previous_pfd, '.2f')}.")
            # consider current_pfd_entry.wait_time >= previous_pfd_entry.wait_time:
            if prefetch_ratio > 1 and prefetch_ratio > (tput_ratio * 0.9) and new_pfd > current_pfd:
                new_pfd = current_pfd * 0.75

        # TODO: Need to handle pfd 0 and 1 (to handle 1 need to consider
        # changing duration)
        new_pfd = max(new_pfd, 2)

        if new_pfd != self.prefetch_distance:
            # print(f'tick: {self.tick}. old pfd: {self.prefetch_distance}. new pfd: {new_pfd}')
            self.change_log.append(ChangeLogEntry(tick=self.tick,
                prefetch_distance=new_pfd, submitted=0,
                consumed=0,
                throughput_interval_start=0, throughput_interval_end=0,
                wait_time=0, idle_time=0, prefetch_ratio=0, tput_ratio=0))

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

    @property
    def current_pfd_entry(self):
        return self.change_log[-1]

    def reaction(self):
        # Determine current wait time
        if self.waited_at is None and self.cnc < self.headroom:
            self.waited_at = self.tick
        elif self.waited_at is not None and self.cnc >= self.headroom:
            self.waited_at = None

        completed = self.pipeline['completed']
        ncompleted = self.pipeline['deadline'].info['to_move']
        consumed = self.pipeline['consumed']
        nconsumed = self.pipeline['completed'].info['to_move']

        if ncompleted > 0:
            self.completion_log.append(CompletionLogEntry(tick=self.tick, number=ncompleted))

        # Record idle time on each completed not consumed IO
        for io in completed:
            io.completion_time = getattr(io, "completion_time", self.tick)

        for io in consumed:
            if getattr(io, "cached", None) is not None:
                continue

            if getattr(io, "accounted", None) is not None:
                continue

            io.accounted = True

            for entry in reversed(self.change_log):
                if entry.prefetch_distance == io.prefetch_distance:
                    if entry.consumed == 0:
                        entry.throughput_interval_start = io.completion_time
                    entry.consumed += 1
                    if entry.consumed >= int(entry.prefetch_distance):
                        entry.throughput_interval_end = io.completion_time
                    break
            # print(f"consumed IO pfd: {io.prefetch_distance}. current pfd: {self.prefetch_distance} current change_log entry pfd: {self.current_pfd_entry.prefetch_distance}")

        # Don't adjust unless threshhold has been met
        if nconsumed > 0 and self.current_pfd_entry.consumed >= int(self.current_pfd_entry.prefetch_distance):
            duration = self.current_pfd_entry.throughput_interval_end - self.current_pfd_entry.throughput_interval_start
            consumed = self.current_pfd_entry.consumed
            pfd = self.current_pfd_entry.prefetch_distance
            self.last_tput = consumed / duration
            # print(f'tick: {self.tick}. pfd: {pfd}. duration: {duration}. consumed: {consumed}. tput: {consumed / duration * 1000}')
            self.adjust = True

        self.info['throughput'] = self.last_tput

    def next_action(self):
        return self.tick + 1 if self.adjust else super().next_action()
