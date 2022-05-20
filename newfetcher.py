from prefetch_modeler.core import RateBucket, Rate
from dataclasses import dataclass
from fractions import Fraction
import math

@dataclass
class LogItem:
    tick: int
    in_storage: int
    latency: float
    prefetch_rate: Rate
    storage_rate: Rate

from collections import namedtuple
Movement = namedtuple('MovementRecord', ['tick', 'number'])

class NewFetcher(RateBucket):
    name = 'newfetcher'

    def __init__(self, *args, **kwargs):
        self.og_rate = Rate(per_second=2000)
        self.storage_record = []
        self.raw_lookback = 4
        self.log = [LogItem(tick=0, in_storage=0, latency=0,
                            prefetch_rate=self.og_rate.value,
                            storage_rate=0)]

        super().__init__(*args, **kwargs)
        self._rate = self.rate()

    @property
    def period(self):
        return self.log[-1]

    def rate(self):
        # return self.period.prefetch_rate

        # rate = Fraction(self.period.in_storage / self.period.latency)

        if getattr(self, 'pipeline', None) is not None:
            return self.pipeline['remaining'].rate()
        else:
            return self.period.prefetch_rate

    @property
    def in_progress(self):
        return self.counter - len(self.pipeline['consumed'])

    @property
    def completed(self):
        return len(self.pipeline['completed'])

    @property
    def in_storage(self):
        return self.in_progress - self.completed - len(self)

    # TODO: refactor
    @property
    def latency_change(self):
        if len(self.log) < 2:
            return 0

        period_newer = self.log[-1]
        period_older = self.log[-2]

        length = period_newer.tick - period_older.tick

        older_latency = math.ceil(period_older.latency)
        newer_latency = math.ceil(period_newer.latency)
        result = Fraction(newer_latency - older_latency, length)
        print(f'result is {result}')
        return result

    @property
    def storage_rate_change(self):
        if len(self.log) < 2:
            return 0

        period_newer = self.log[-1]
        period_older = self.log[-2]

        length = period_newer.tick - period_older.tick

        return Fraction(period_newer.storage_rate - period_older.storage_rate, length)

    @property
    def raw_storage_rate(self):
        move_record = list(reversed(self.storage_record))
        intervals = list(zip(move_record, move_record[1:]))

        number_moved = 0
        time_elapsed = 0
        number_seen = 0
        for newer_movement, older_movement in intervals:
            number_moved += newer_movement.number
            # print(f'newer movement tick: {newer_movement.tick}. older movement tick: {older_movement.tick}')
            time_elapsed += newer_movement.tick - older_movement.tick
            number_seen += 1
            if number_seen > self.raw_lookback:
                break

        if time_elapsed == 0:
            raw_rate = 0
        else:
            raw_rate = Fraction(number_moved, time_elapsed)

        return raw_rate

    def reaction(self):
        for io in self.pipeline['submitted']:
            if getattr(io, 'submitted', None) is not None:
                continue
            io.submitted = self.tick

        # In case the IO is moved immediately to the inflight bucket
        for io in self.pipeline['inflight']:
            if getattr(io, 'submitted', None) is not None:
                continue
            io.submitted = self.tick

        num_ios = 0
        total_latency = 0

        for io in self.pipeline['completed']:
            if getattr(io, 'completed', None) is None:
                io.completed = self.tick
            latency = io.completed - io.submitted
            total_latency += latency
            num_ios += 1

        # If consumption rate is fast enough, IOs might always be moved to
        # consumed right away, so we need to find them and count them
        for io in self.pipeline['consumed']:
            if getattr(io, 'completed', None) is not None:
                continue
            io.completed = self.tick
            latency = io.completed - io.submitted
            total_latency += latency
            num_ios += 1

        if num_ios == 0:
            return

        if self.pipeline['inflight'].info['to_move']:
            movement = Movement(self.tick, self.pipeline['inflight'].info['to_move'])
            self.storage_record.append(movement)

        self.log.append(LogItem(tick=self.tick,
                                in_storage=self.in_storage,
                                latency=float(total_latency / num_ios),
                                prefetch_rate=self.rate(),
                                storage_rate=self.raw_storage_rate,))
