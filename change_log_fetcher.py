from prefetch_modeler.core import ContinueBucket, GlobalCapacityBucket, RateBucket, \
Rate, Duration, ForkBucket, Bucket, GateBucket
from prefetch_modeler.prefetcher_type import ConstantRatePrefetcher
from constant_distance_prefetcher import ConstantDistancePrefetcher
from dataclasses import dataclass
from fractions import Fraction
import itertools
import math
from numpy import mean

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
class ChangeLogEntry:
    tick: int
    prefetch_distance: int
    change: int
    upvote: int
    downvote: int

class ChangeLogFetcher(ConstantDistancePrefetcher):
    headroom = 20
    target_idle_time = 20

    def __init__(self, *args, **kwargs):
        self.prefetch_distance = self.headroom
        # new prefetch distance
        self.change_log = [ChangeLogEntry(tick=0,
                                          prefetch_distance=self.prefetch_distance,
                                          change=5, upvote=1, downvote=1)]

        self.waited_at = None
        self.adjust = True

        super().__init__(*args, **kwargs)

    def remove(self, io):
        # submission time should be for cached too?
        if not getattr(io, "cached", False):
            io.submission_time = self.tick
            if self.waited_at is not None:
                io.wait_time = self.tick - self.waited_at
            else:
                io.wait_time = 0
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

        self.adjust = False

        self.info['delta'] = delta
        self.info['wait_time'] = wait_time
        self.info['idle_time'] = idle_time

        if len(self.change_log) < 2:
            return int(self.prefetch_distance)

        accumulated_change = 0
        for entry in list(reversed(self.change_log))[:20]:
            if entry.upvote > entry.downvote * 1.2:
                accumulated_change += entry.change * 1
            elif entry.downvote > entry.upvote * 1.2:
                accumulated_change -= entry.change * 1
        print("adjust", accumulated_change)
        accumulated_change = int(accumulated_change)

        if accumulated_change == 0:
            accumulated_change = 1 if self.tick % 2 == 0 else -1

        new_prefetch_distance = self.prefetch_distance + accumulated_change
        if new_prefetch_distance <= 0:
            new_prefetch_distance = 1

        if new_prefetch_distance != self.prefetch_distance:
            self.change_log.append(ChangeLogEntry(tick=self.tick,
                                                    prefetch_distance=new_prefetch_distance,
                                                    change = accumulated_change,
                                                    upvote=1, downvote=1))
            print(f'old pfd: {self.prefetch_distance}. new pfd: {new_prefetch_distance}')
            self.prefetch_distance = new_prefetch_distance

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
        consumed = self.pipeline['consumed']


        # Record idle time on each completed not consumed IO
        # do this for cached IOs too?
        for io in completed:
            io.completion_time = getattr(io, "completion_time", self.tick)

        # skip if we don't have a consumption
        if not completed.info['to_move']:
            return


        # skip if we don't have any information
        if len(consumed.consumption_log) < 2:
            new_prefetch_distance = self.prefetch_distance + 1
            self.change_log.append(ChangeLogEntry(tick=self.tick,
                                                    prefetch_distance=new_prefetch_distance,
                                                    change = 1,
                                                    upvote=1, downvote=1))
            print(f'old pfd: {self.prefetch_distance}. new pfd: {new_prefetch_distance}')
            self.prefetch_distance = new_prefetch_distance

            return

        new = consumed.consumption_log[-1]

        # old = consumed.consumption_log[-2]

        old = None
        for i, consumed_io in enumerate(reversed(consumed.consumption_log)):
            if consumed_io.consumed < new.submitted:
                old = consumed_io
                break

        if old is None:
            return

        print(new.consumed - new.submitted, old.consumed - old.submitted)

        # The time to consume increased. Bad!
        if new.consumed - new.submitted >= old.consumed - old.submitted:
            responsible_change = None
            for change in reversed(self.change_log):
                if change.tick < new.submitted:
                    responsible_change = change
                    break

            if responsible_change is not None:
                self.adjust = True
                responsible_change.downvote += 1

        # The time to consume decreased. Good!
        else:
            responsible_change = None
            for change in reversed(self.change_log):
                if change.tick < new.submitted:
                    responsible_change = change
                    break

            if responsible_change is not None:
                self.adjust = True
                responsible_change.upvote += 1

    def next_action(self):
        return self.tick + 1 if self.adjust else super().next_action()
