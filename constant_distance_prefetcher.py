from prefetch_modeler.core import ContinueBucket, GlobalCapacityBucket, RateBucket, \
Rate, Duration, ForkBucket, MarkerBucket, Bucket
from dataclasses import dataclass
from fractions import Fraction
import itertools
import math

class ConstantDistancePrefetcher(Bucket):
    name = 'cd_fetcher'
    cnc_headroom = 3

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.run_prefetcher = True

    @classmethod
    def hint(cls):
        return (1, cls.__name__)

    @property
    def in_storage(self):
        return len(self.pipeline['minimum_latency']) + \
            len(self.pipeline['inflight']) + \
            len(self.pipeline['deadline'])

    @property
    def completed(self):
        return len(self.pipeline['completed'])

    def next_action(self):
        if not self.source:
            return math.inf

        if self.run_prefetcher:
            return self.tick + 1

        return math.inf

    def to_move(self):
        pass

    def run(self):
        if not self.run_prefetcher:
            source = frozenset()
        else:
            source = frozenset(self.source)

        moved = []
        for io in source:
            if getattr(io, 'cached', None):
                target = self.pipeline['completed']
            else:
                target = self.target

            if 1 + len(moved) + self.completed + self.in_storage >= self.cnc_headroom:
                break

            self.remove(io)
            target.add(io)

            moved.append(io)

        self.info['actual_to_move'] = frozenset(moved)
        self.info['to_move'] = len(moved)

        self.run_prefetcher = False

    def reaction(self):
        consumed = self.pipeline['completed'].info['to_move']

        # TODO: multiply cnc_headroom by something with latency
        if self.pipeline['completed'].waiting:
            new_headroom = self.cnc_headroom * 1.5
            print(f'{self.tick}: headroom before: {self.cnc_headroom}. headroom after: {new_headroom}')
            self.cnc_headroom = new_headroom

        # wanted = self.pipeline['completed'].info['want_to_move']
        # new_headroom = max(1, self.cnc_headroom + wanted - consumed)
        # if new_headroom != self.cnc_headroom:
        #     print(f'{self.tick}: headroom before: {self.cnc_headroom}. headroom after: {new_headroom}')
        #     self.cnc_headroom = new_headroom

        if consumed:
            self.run_prefetcher = True




class CachedBaselineFetchAll(Bucket):
    name = 'remaining'

    @classmethod
    def hint(cls):
        return (1, cls.__name__)

    def next_action(self):
        return self.tick + 1 if self.source else math.inf

    def to_move(self):
        pass

    def run(self):
        moved = []

        source = frozenset(self.source)
        for io in source:
            if getattr(io, 'cached', None):
                target = self.pipeline['completed']
            else:
                target = self.target

            self.remove(io)
            target.add(io)

            moved.append(io)

        self.info['actual_to_move'] = frozenset(moved)
        self.info['to_move'] = len(moved)


class CachedBaselineSync(Bucket):
    name = 'remaining'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.run_prefetcher = True

    @classmethod
    def hint(cls):
        return (1, cls.__name__)

    @property
    def in_storage(self):
        return len(self.pipeline['minimum_latency']) + \
            len(self.pipeline['inflight']) + \
            len(self.pipeline['deadline'])

    @property
    def completed(self):
        return len(self.pipeline['completed'])

    def to_move(self):
        pass

    def next_action(self):
        if not self.source:
            return math.inf

        if self.run_prefetcher:
            return self.tick + 1

        return math.inf

    def run(self):
        if self.in_storage + self.completed >= 1:
            self.info['actual_to_move'] = frozenset()
            self.info['to_move'] = 0
            return

        if not self.run_prefetcher:
            source = frozenset()
        else:
            source = frozenset(self.source)

        moved = []
        source = frozenset(self.source)
        for io in source:
            if getattr(io, 'cached', None):
                target = self.pipeline['completed']
            else:
                target = self.target

            self.remove(io)
            target.add(io)

            moved.append(io)
            break

        self.info['actual_to_move'] = frozenset(moved)
        self.info['to_move'] = len(moved)

        self.run_prefetcher = False

    def reaction(self):
        if self.pipeline['completed'].info['to_move']:
            self.run_prefetcher = True
