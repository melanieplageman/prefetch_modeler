from prefetch_modeler.core import ContinueBucket, GlobalCapacityBucket, RateBucket, \
Rate, Duration, ForkBucket
from dataclasses import dataclass
from fractions import Fraction
import itertools
import math


@dataclass(kw_only=True)
class LedgerEntry:
    tick: int
    raw_demand_rate: Fraction
    prefetch_rate: Rate


from collections import namedtuple
Movement = namedtuple('MovementRecord', ['tick', 'number'])

class PartiallyCachedPIPrefetcher(RateBucket):
    name = 'remaining'
    og_rate = Rate(per_second=6000)
    raw_lookback = 66
    avg_lookback = 10
    kp = 0.5
    ki_cnc = -Rate(per_second=40).value
    cnc_headroom = 8
    use_raw = False

    def __init__(self, *args, **kwargs):
        self.ledger = [LedgerEntry(tick=0,
                                   raw_demand_rate=0,
                                   prefetch_rate=self.og_rate.value)]

        super().__init__(*args, **kwargs)
        self.workload_record = []
        self.run_prefetcher = True

    def rate(self):
        return self.period.prefetch_rate

    @property
    def in_storage(self):
        return len(self.pipeline['minimum_latency']) + \
            len(self.pipeline['inflight']) + \
            len(self.pipeline['deadline'])

    @classmethod
    def hint(cls):
        return (1, cls.__name__)

    @property
    def period(self):
        return self.ledger[-1]

    @property
    def completed(self):
        return len(self.pipeline['completed'])

    def run(self):
        if self.run_prefetcher:
            to_move = super().to_move()
        else:
            to_move = frozenset()

        self.info['actual_to_move'] = to_move
        self.info['to_move'] = len(to_move)

        cached_blocks_moved = 0
        for io in to_move:
            target = self.target
            if getattr(io, 'cached', None):
                target = self.pipeline['completed']
                self.volume += 1
                cached_blocks_moved += 1
            else:
                target = self.target
            self.remove(io)
            target.add(io)
            # if cached_blocks_moved + self.completed >= self.cnc_headroom:
            #     break
                # TODO would need to remove the remaining IOs which were set to
                # be moved from actual_to_move and also decrement to_move and
                # increment volume for each of these remaining ones

        self.run_prefetcher = False

    def next_action(self):
        if not self.source:
            return math.inf

        if self.counter == 0:
            return self.tick + 1

        if self.run_prefetcher:
            return self.tick + 1

        # if self.in_storage + self.completed <= self.cnc_headroom:
        #     return self.tick + 1

        # if self.pipeline['completed'].info['to_move']:
        #     return self.tick + 1

        # if self.volume >= 1:
        #     return self.tick + 1

        return math.inf

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

    @property
    def demand_rate(self):
        if self.use_raw:
            return self.raw_demand_rate
        raw_demand_rate = self.raw_demand_rate

        if raw_demand_rate == 0:
            return 0

        start_idx = 0
        for i, entry in enumerate(self.ledger):
            if entry.raw_demand_rate != 0:
                start_idx = i
                break

        usable_demand_rate_log = self.ledger[start_idx:]

        total = sum([item.raw_demand_rate for item in itertools.islice(
            reversed(usable_demand_rate_log), self.avg_lookback)])
        return Fraction(total, self.avg_lookback)

    @property
    def proportional_term(self):
        adjustment = 0
        prefetch_rate = self.rate()
        demand_rate = self.demand_rate
        if demand_rate != 0:
            adjustment = demand_rate - prefetch_rate
        return adjustment

    @property
    def cnc_integral_term(self):
        if self.demand_rate == 0:
            return 0
        iterm = self.completed - self.cnc_headroom
        return iterm


    def adjust(self):
        demand_rate = self.demand_rate
        prefetch_rate = self.rate()

        p = self.proportional_term
        cnc_i = self.cnc_integral_term

        pc = p * self.kp
        cnc_ic = cnc_i * self.ki_cnc

        new_rate = prefetch_rate
        new_rate += pc
        new_rate += cnc_ic
        if new_rate < 0:
            new_rate = 0

        self.ledger.append(LedgerEntry(tick=self.tick,
                                       raw_demand_rate=self.raw_demand_rate,
                                       prefetch_rate=new_rate))

    def reaction(self):
        if self.pipeline['completed'].info['to_move']:
            moved = len([io for io in self.pipeline['completed'].info['actual_to_move'] if not
                        hasattr(io, 'cached')])

            if moved > 0:
                movement = Movement(self.tick, moved)
                self.workload_record.append(movement)
            self.run_prefetcher = True
            self.adjust()
