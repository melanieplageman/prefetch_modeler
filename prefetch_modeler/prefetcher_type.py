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
    # Note that this is in-progress IOs which are not in flight and not
    # completed -- waiting to be inflight
    awaiting_dispatch: int
    inflight: int

    length: Optional[int] = None

    too_fast_for_storage: bool = False


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
    awaiting_dispatch_headroom = 4
    multiplier = 2

    def __init__(self, *args, **kwargs):
        self.ledger = [Interval(tick=0, rate=self.initial_rate, completed=0,
                                awaiting_dispatch=0, inflight=0)]
        self.sample_io = None
        super().__init__(*args, **kwargs)

    def next_rate_up(self, rate, dt, backlog, headroom):
        next_rate = rate - dt
        if next_rate < rate * self.multiplier:
            next_rate = rate * self.multiplier

        next_rate -= self.burn_down_rate(backlog, headroom)
        if next_rate < rate:
            return rate
        return next_rate

    def next_rate_down(self, rate, dt, backlog, headroom):
        next_rate = rate - dt - self.burn_down_rate(backlog, headroom)

        if next_rate < rate / self.multiplier:
            next_rate = rate / self.multiplier
        return next_rate

    def burn_down_rate(self, backlog, headroom):
        # How long will the next period be? Let's use a recency biased mean of
        # the last periods
        next_period_length = recent_mean(period.length for period in reversed(self.ledger))

        # How many completed IOs do we want to burn down?
        burn_down = backlog - headroom

        if burn_down <= 0:
            return 0

        return Fraction.from_float(burn_down / next_period_length)

    def adjust(self):
        self.period.length = self.tick - self.period.tick
        rate = self.period.rate

        if len(self.ledger) > 2:
            completed_dt = Fraction(self.completed - self.period.completed,
                                    self.period.length)

            awaiting_dispatch_dt = Fraction(self.awaiting_dispatch - self.period.awaiting_dispatch,
                                            self.period.length)

            old_rate = self.ledger[-2].rate

            print(f"completed_dt: {completed_dt}. awaiting_dispatch_dt: {awaiting_dispatch_dt}")
            if completed_dt > 0 or self.completed > self.completed_headroom:
                rate = self.next_rate_down(old_rate, completed_dt,
                                           self.completed,
                                           self.completed_headroom)

            elif awaiting_dispatch_dt > 0 or self.awaiting_dispatch > self.awaiting_dispatch_headroom:
                rate = self.next_rate_down(old_rate, awaiting_dispatch_dt,
                                           self.awaiting_dispatch,
                                           self.awaiting_dispatch_headroom)
                self.ledger[-2].too_fast_for_storage = True

            else:
                too_fast_rates = [period.rate for period in
                                  reversed(self.ledger) if
                                  period.too_fast_for_storage]
                if too_fast_rates:
                    too_fast_rate = recent_mean(too_fast_rates)
                else:
                    too_fast_rate = None

                if too_fast_rate is not None and old_rate > too_fast_rate:
                    rate = (old_rate + too_fast_rate) / 2
                else:
                    rate = self.next_rate_up(old_rate, completed_dt,
                                            self.completed,
                                            self.completed_headroom)


        if rate != self.period.rate:
            previous_ad = self.ledger[-1].awaiting_dispatch
            previous_comp = self.ledger[-1].completed
            previous_if = self.ledger[-1].inflight
            ad_str = f"awaiting_dispatch: {previous_ad}, {self.awaiting_dispatch}. "
            if_str = f"inflight: {previous_if}, {self.inflight}. "
            comp_str = f"completed: {previous_comp}, {self.completed}. "
            rate_str = f"old_rate: {self.period.rate}, new_rate: {rate}. "
            print(ad_str + if_str + comp_str + rate_str)


        period = Interval(tick=self.tick, rate=rate, completed=self.completed,
                          awaiting_dispatch=self.awaiting_dispatch,
                          inflight=self.inflight)

        self.ledger.append(period)
        self.sample_io = None

    def to_move(self):
        self.tick_data['awaiting_dispatch'] = self.awaiting_dispatch
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
    def in_progress(self):
        return self.counter - len(self.pipeline['consumed'])

    @property
    def awaiting_dispatch(self):
        return self.in_progress - self.inflight - self.completed - len(self)

    @property
    def inflight(self):
        return len(self.pipeline['inflight'])

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
    [BaselineFetchAll],
    # [BaselineSync],
    [CoolPrefetcher],
    # [AdjustedPrefetcher2],
]
