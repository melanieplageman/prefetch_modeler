from prefetch_modeler.core import ContinueBucket, GlobalCapacityBucket, RateBucket, \
Rate, Duration, ForkBucket, Bucket
from dataclasses import dataclass
from fractions import Fraction
import itertools
import math

from collections import namedtuple
Movement = namedtuple('MovementRecord', ['tick', 'number'])

class CDVariableHeadroom(Bucket):
    name = 'cd_fetcher'
    cnc_headroom = 3
    raw_lookback = 10
    avg_lookback = 10

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.workload_record = []

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

    @property
    def raw_demand_rate(self):
        move_record = list(reversed(self.workload_record))
        intervals = list(zip(move_record, move_record[1:]))

        number_moved = 0
        time_elapsed = 0
        number_seen = 0
        for newer_movement, older_movement in intervals:
            number_moved += newer_movement.number
            time_elapsed += newer_movement.tick - older_movement.tick
            number_seen += 1
            if number_seen > self.raw_lookback:
                break

        if time_elapsed == 0:
            raw_rate = 0
        else:
            raw_rate = Fraction(number_moved, time_elapsed)

        return raw_rate

    def next_action(self):
        if self.pipeline['completed'].info['to_move']:
            return self.tick + 1
        return math.inf

    def to_move(self):
        pass

    def calculate_latency(self):
        num_ios = 0
        total_latency = 0

        for io in self.pipeline['completed']:
            if hasattr(io, 'cached'):
                continue

            if getattr(io, 'completed', None) is None:
                io.completed = self.tick
            latency = io.completed - io.submitted
            total_latency += latency
            num_ios += 1

        # If consumption rate is fast enough, IOs might always be moved to
        # consumed right away, so we need to find them and count them
        for io in self.pipeline['consumed']:
            if hasattr(io, 'cached'):
                continue

            if getattr(io, 'completed', None) is not None:
                continue
            io.completed = self.tick
            latency = io.completed - io.submitted
            total_latency += latency
            num_ios += 1

        if num_ios == 0:
            return 0

        latency = total_latency / num_ios
        return latency

    def reaction(self):
        if self.pipeline['completed'].info['to_move']:
            moved = len([io for io in self.pipeline['completed'].info['actual_to_move'] if not
                        hasattr(io, 'cached')])

            movement = Movement(self.tick, moved)
            self.workload_record.append(movement)
            self.latency = self.calculate_latency()
            new_headroom = self.raw_demand_rate * self.latency
            print(f'new headroom is {new_headroom}. old headroom is {self.cnc_headroom}')
            self.cnc_headroom = new_headroom

    def run(self):
        moved = []

        source = frozenset(self.source)
        for io in source:
            if getattr(io, 'cached', None):
                target = self.pipeline['completed']
            else:
                target = self.target

            if 1 + len(moved) + self.completed + self.in_storage >= self.cnc_headroom:
                break

            io.submitted = self.tick

            self.remove(io)
            target.add(io)

            moved.append(io)

        self.info['actual_to_move'] = frozenset(moved)
        self.info['to_move'] = len(moved)
