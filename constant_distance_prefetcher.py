from prefetch_modeler.core import ContinueBucket, GlobalCapacityBucket, RateBucket, \
Rate, Duration, ForkBucket, MarkerBucket, Bucket, GateBucket
from dataclasses import dataclass
from fractions import Fraction
import itertools
import math

class ConstantDistancePrefetcher(GlobalCapacityBucket):
    name = 'cd_fetcher'
    prefetch_distance = 100

    @classmethod
    def hint(cls):
        return (1, cls.__name__)

    def max_buffers(self):
        return self.prefetch_distance


class VariableDistancePrefetcher(ConstantDistancePrefetcher):
    headroom = 4
    target_idle_time = 20
    multiplier = 1 / 20_000_000

    def __init__(self, *args, **kwargs):
        self.prefetch_distance = self.headroom

        self.waited_at = None
        self.last_adjusted = 0
        self.adjust = True

        super().__init__(*args, **kwargs)

    def max_buffers(self):
        if self.adjust is False:
            return int(self.prefetch_distance)

        # positive
        if self.waited_at is not None:
            wait_time = self.tick - self.waited_at
        else:
            wait_time = 0

        # positive

        idle_time = sum(self.tick - io.completion_time for io in self.pipeline['completed'])
        idle_time -= self.headroom * self.target_idle_time

        delta = wait_time - idle_time

        # positive
        multiplier = self.multiplier * (self.tick - self.last_adjusted)

        self.prefetch_distance += multiplier * delta
        self.prefetch_distance = max(self.prefetch_distance, 0)

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
        completed = self.pipeline['completed']

        if self.waited_at is None and self.cnc < self.headroom:
            self.waited_at = self.tick
        elif self.waited_at is not None and self.cnc >= self.headroom:
            self.waited_at = None

        # Record idle time on each completed not consumed IO
        for io in self.pipeline['completed']:
            io.completion_time = getattr(io, "completion_time", self.tick)

        # Don't adjust unless we have a consumption
        self.adjust = completed.info['to_move'] > 0

    def next_action(self):
        return self.tick + 1 if self.adjust else super().next_action()
