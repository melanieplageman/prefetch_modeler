from enum import Enum
from prefetch_modeler.core import GateBucket, ContinueBucket, \
GlobalCapacityBucket, RateBucket, Rate
from fractions import Fraction
from dataclasses import dataclass
from typing import Optional
import math
import itertools


class BaselineSync(GlobalCapacityBucket):
    name = 'remaining'

    def max_buffers(self):
        return 1


class BaselineFetchAll(ContinueBucket):
    name = 'remaining'

class PeriodRate:
    def __init__(self, rate, required_correction):
        self.rate = rate
        self.required_correction = required_correction


@dataclass
class Interval:
    tick: int
    rate: Fraction
    completed: int

    length: Optional[int] = None


def recent_mean(iterator, take=8):
    numerator = denominator = 0
    for i, number in enumerate(itertools.islice(iterator, take)):
        weight = math.exp(i)
        numerator += weight * number
        denominator += weight
    return numerator / denominator


class CoolPrefetcher(RateBucket):
    name = 'remaining'

    initial_rate = Rate(per_second=2000).value
    completed_headroom = 10
    multiplier = 2

    def __init__(self, *args, **kwargs):
        self.ledger = [Interval(tick=0, rate=self.initial_rate, completed=0)]
        self.sample_io = None
        super().__init__(*args, **kwargs)

    def next_rate_up(self, rate, complete_dt):
        next_rate = rate - complete_dt
        if next_rate < rate * self.multiplier:
            next_rate = rate * self.multiplier

        next_rate -= self.burn_down_rate()
        if next_rate < rate:
            return rate
        return next_rate

    def next_rate_down(self, rate, complete_dt):
        next_rate = rate - complete_dt - self.burn_down_rate()
        if next_rate < rate / self.multiplier:
            next_rate = rate / self.multiplier
        return next_rate

    def burn_down_rate(self):
        # How long will the next period be? Let's use a recency biased mean of
        # the last periods
        next_period_length = recent_mean(period.length for period in reversed(self.ledger))

        # How many completed IOs do we want to burn down?
        burn_down = self.completed - self.completed_headroom

        if burn_down <= 0:
            return 0

        return Fraction.from_float(burn_down / next_period_length)

    def correction(self):
        if self.completed < self.period.completed:
            return 'up'
        if self.completed < self.completed_headroom:
            return 'up'
        return 'down'

    def adjust(self):
        self.period.length = self.tick - self.period.tick
        rate = self.period.rate

        if len(self.ledger) > 2:
            completed_dt = Fraction(self.completed - self.period.completed,
                                    self.period.length)

            old_rate = self.ledger[-2].rate

            if self.correction() == 'down':
                rate = self.next_rate_down(old_rate, completed_dt)
            else:
                rate = self.next_rate_up(old_rate, completed_dt)

        period = Interval(tick=self.tick, rate=rate, completed=self.completed)
        self.ledger.append(period)
        self.sample_io = None

    def to_move(self):
        to_move = super().to_move()
        if self.sample_io is None:
            self.sample_io = next(iter(to_move), None)
        return to_move

    def should_adjust(self):
        if self.sample_io is None:
            return False
        if self.sample_io in self.pipeline['completed']:
            return True
        if self.sample_io in self.pipeline['consumed']:
            return True
        return False

    def rate(self):
        return self.period.rate

    @property
    def period(self):
        return self.ledger[-1]

    @property
    def completed(self):
        return len(self.pipeline['completed'])

    def run(self, *args, **kwargs):
        if self.should_adjust():
            self.adjust()
        super().run(*args, **kwargs)

    def next_action(self):
        # If we just adjusted, make sure that we run on the next tick so that
        # the rate change is reflected
        if self.period.tick == self.tick:
            return self.tick + 1
        return super().next_action()


prefetcher_list = [
    # [BaselineFetchAll],
    # [BaselineSync],
    [CoolPrefetcher],
    # [AdjustedPrefetcher2],
]
