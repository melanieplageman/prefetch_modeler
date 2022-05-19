from prefetch_modeler.core import RateBucket, StopBucket, Rate, Interval
from dataclasses import dataclass
from collections import OrderedDict
import numpy as np
import math
import random

@dataclass(kw_only=True)
class SuperInterval(Interval):
    completed: int
    # Note that this is in-progress IOs which are not in flight and not
    # completed -- waiting to be inflight
    awaiting_dispatch: int
    inflight: int


class rangerator:
    def __init__(self, steps):
        self.steps = steps
        self._ranges = []

    @property
    def ranges(self):
        if self._ranges:
            return self._ranges
        lower = 0
        for i in range(len(self.steps)):
            upper = lower + self.steps[i]
            self._ranges.append(range(lower, upper))
            lower = upper
        return self._ranges


class SavedRates:
    _saved_rates = OrderedDict()

    def __init__(self, steps, rates, default_rate=1000):
        self.default_rate = default_rate
        self.ranges = rangerator(steps).ranges
        self.rates = rates

    def current_range(self, tick):
        for r in self.ranges:
            if tick in r:
                return r
        return None

    def current_range_idx(self, tick):
        for i, r in enumerate(self.ranges):
            if tick in r:
                return i
        return None

    @property
    def saved_rates(self):
        if self._saved_rates:
            return self._saved_rates

        o = OrderedDict()
        idx = 0
        for r in self.ranges:
            o[r] = self.rates[idx]
            idx = (idx + 1) % len(self.rates)

        self._saved_rates = sorted(o.items(), key=lambda r: r[0].start)
        return self._saved_rates

    def get_rate(self, tick):
        for r in self.saved_rates:
            if tick in r[0]:
                return Rate(per_second=math.ceil(r[1])).value
        return Rate(per_second=math.ceil(self.default_rate)).value


class SineRaterator:
    amplitude = 1000
    _rates = []
    period = 10000

    def __init__(self, ranges):
        self.ranges = ranges

    @property
    def rates(self):
        if self._rates:
            return self._rates
        for r in self.ranges:
            self._rates.append(math.ceil((np.sin(r.start / self.period) + 1) * self.amplitude))
        return self._rates


def workload_type(hint, consumption_rate_func, saved_rates):
    class completed(RateBucket):
        cnc_headroom = 2

        def __init__(self, *args, **kwargs):
            self.consumerator = None
            self.saved_rates = saved_rates
            super().__init__(*args, **kwargs)

        @classmethod
        def hint(cls):
            return (1, hint)

        def rate(self):
            return consumption_rate_func(self)

        def next_action(self):
            # the lower bound of the next range in the ranges array
            # the min of that and super next_action
            next_action = super().next_action()
            current_range_start = self.saved_rates.current_range_idx(self.tick)
            if current_range_start is None:
                return next_action
            if current_range_start + 1 >= len(self.saved_rates.rates):
                return next_action
            next_range_start = self.saved_rates.ranges[current_range_start + 1].start + 1
            # print(f'tick: {self.tick}. next_range_start: {next_range_start}. next_action: {next_action}')
            return min(next_range_start, next_action)

    class consumed(StopBucket):
        pass

    return [completed, consumed]

def test_consumption_rate(self):
    return Rate(per_second=2000).value

even_wl = workload_type('Even Workload', test_consumption_rate,
                        [])

def consumption_rate_func2(self):
    if getattr(self, 'tick', 0) <= 5000:
        return Rate(per_second=2000).value
    elif getattr(self, 'tick', 0) <= 20000:
        return Rate(per_second=5000).value
    else:
        return Rate(per_second=10000).value

def consumption_rate_func3(self):
    return self.saved_rates.get_rate(getattr(self, 'tick', 0))

def consumption_rate_func4(self):
    if getattr(self, 'tick', 0) <= 10000:
        return Rate(per_second=1000).value
    elif getattr(self, 'tick', 0) <= 50000:
        return Rate(per_second=500).value
    elif getattr(self, 'tick', 0) <= 90000:
        return Rate(per_second=4000).value
    elif getattr(self, 'tick', 0) <= 102000:
        return Rate(per_second=1000).value
    elif getattr(self, 'tick', 0) <= 109000:
        return Rate(per_second=500).value
    else:
        return Rate(per_second=1000).value

def consumption_rate_func6(self):
    return self.saved_rates.get_rate(getattr(self, 'tick', 0))

steps = [10000, 10000, 100000]
rates = [1000, 3000, 2000]
default_rate = 1400
saved_rates_reg1 = SavedRates(steps, rates, default_rate)

uneven_wl = workload_type('Uneven Workload', consumption_rate_func3,
                          saved_rates_reg1)

steps = [2000, 5000, 200, 4444, 22, 10000, 35678, 2000, 10000, 200]
rates = SineRaterator(rangerator(steps).ranges).rates
default_rate = 1000
saved_rates_sine1 = SavedRates(steps, rates, default_rate)

uneven_wl = workload_type('Uneven Workload', consumption_rate_func6,
                          saved_rates_sine1)
