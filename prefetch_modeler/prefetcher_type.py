from prefetch_modeler.core import ContinueBucket, GlobalCapacityBucket, RateBucket, \
Rate, Duration
from dataclasses import dataclass
from fractions import Fraction
import itertools
import math


class BaselineSync(GlobalCapacityBucket):
    name = 'remaining'

    def max_buffers(self):
        return 1


class BaselineFetchAll(ContinueBucket):
    name = 'remaining'


class ConstantPrefetcher(RateBucket):
    name = 'remaining'

    og_rate = Rate(per_second=2000)

    @classmethod
    def hint(cls):
        return (2, f"Constant Rate: {cls.og_rate}")

    def rate(self):
        return self.og_rate.value


def recent_mean(iterator, take=8):
    numerator = denominator = 0
    for i, number in enumerate(itertools.islice(iterator, take)):
        weight = math.exp(i)
        numerator += weight * number
        denominator += weight
    return numerator / denominator


@dataclass(kw_only=True)
class LedgerEntry:
    tick: int
    raw_demand_rate: Fraction
    prefetch_rate: Rate


def humanify(rate):
    return math.ceil(float(rate) * 1000 * 1000)

from collections import namedtuple
Movement = namedtuple('MovementRecord', ['tick', 'number'])

class PIPrefetcher(RateBucket):
    name = 'remaining'
    og_rate = Rate(per_second=6000)
    raw_lookback = 66
    avg_lookback = 10
    kp = 0.5
    ki_cnc = -Rate(per_second=40).value
    cnc_headroom = 8

    def __init__(self, *args, **kwargs):
        self.ledger = [LedgerEntry(tick=0,
                                   raw_demand_rate=0,
                                   prefetch_rate=self.og_rate.value)]

        super().__init__(*args, **kwargs)
        self.workload_record = []
        self.storage_record = []

    def rate(self):
        return self.period.prefetch_rate

    @classmethod
    def hint(cls):
        return (1, cls.__name__)

    @property
    def period(self):
        return self.ledger[-1]

    @property
    def lifetime_demands(self):
        return self.pipeline['consumed'].counter

    @property
    def lifetime_completes(self):
        return self.pipeline['completed'].counter

    @property
    def completed(self):
        return len(self.pipeline['completed'])

    @property
    def inflight(self):
        return len(self.pipeline['inflight'])

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

    def run(self, *args, **kwargs):
        super().run(*args, **kwargs)

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
        # if self.pipeline['inflight'].info['to_move']:
        #     inflight = len(self.pipeline['inflight'])
        #     submitted = len(self.pipeline['submitted'])
        #     print(f'Tick: {self.tick}. inflight: {inflight}. submitted: {submitted}')

        if self.pipeline['completed'].info['to_move']:
            movement = Movement(self.tick, self.pipeline['completed'].info['to_move'])
            self.workload_record.append(movement)
            self.adjust()
