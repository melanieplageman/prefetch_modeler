from enum import Enum
from prefetch_modeler.core import GateBucket, ContinueBucket, \
GlobalCapacityBucket, RateBucket, SamplingRateBucket, \
Rate, Interval, Duration
from fractions import Fraction
from dataclasses import dataclass
import math
import itertools

@dataclass(kw_only=True)
class PIDPrefetchInterval(Interval):
    completed: int
    # Note that this is in-progress IOs which are not in flight and not
    # completed -- waiting to be inflight
    awaiting_dispatch: int
    inflight: int
    lt_demanded: int
    lt_completed: int
    integral_term: int
    proportional_term: Fraction
    derivative_term: Fraction
    cnc_headroom: int

class FastPIDPrefetcher(SamplingRateBucket):
    ki = -Rate(per_second=40).value       # should be in units of per-second
    kp = -0.9                               # should be dimensionless
    kd = -Duration(microseconds=2).total  # should be in units of seconds
    cnc_headroom = 6
    aw_headroom = 2
    og_rate = Rate(per_second=2000)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.ledger = [PIDPrefetchInterval(tick=0, rate=self.og_rate.value, completed=0,
                                awaiting_dispatch=0, inflight=0,
                                           cnc_headroom=self.cnc_headroom,
                                  proportional_term=0,
                                  integral_term=0,
                                  derivative_term=0,
                                        lt_demanded=0, lt_completed=0)]
        self._rate = self.rate()
        print(f"Initial Rate: {self._rate} {float(self._rate)}")

        self.period.cnc_headroom = self.cnc_headroom

    def adj_cnc(self, period):
        return period.completed - self.cnc_headroom

    def adj_cnc(self, period):
        return period.completed - period.cnc_headroom

    def change_cnc_rate(self, period0, period1):
        return Fraction(self.adj_cnc(period1) - self.adj_cnc(period0), period1.length)

    def change_change_cnc_rate(self, period0, period1, period2):
        cdt_p1 = self.change_cnc_rate(period0, period1)
        cdt_p2 = self.change_cnc_rate(period1, period2)

        return Fraction(cdt_p2 - cdt_p1, period2.length + period1.length)

    @property
    def integral_term(self):
        return self.adj_cnc(self.period)

    @property
    def proportional_term(self):
        if len(self.ledger) < 2:
            return 0

        return Fraction(
            self.ledger[-1].completed - self.ledger[-2].completed,
            self.ledger[-1].length)

    @property
    def derivative_term(self):
        if len(self.ledger) < 3:
            return 0

        period2 = self.ledger[-1]
        period1 = self.ledger[-2]
        period0 = self.ledger[-3]
        return self.change_change_cnc_rate(period0, period1, period2)

    @property
    def pid_log(self):
        log_str = f'{self.tick}: '

        if len(self.ledger) < 1:
            return log_str

        log_str += f'P: {float(self.proportional_term)}. I: {float(self.integral_term)}. D: {float(self.derivative_term)}.'
        return log_str

    @property
    def completed_log(self):
        log_str = f'{self.tick}: '

        if len(self.ledger) < 1:
            return log_str

        period2 = self.ledger[-1]
        log_str += f'p2_completed: {self.adj_cnc(period2)}. p2_length: {period2.length}. '

        if len(self.ledger) < 2:
            return log_str

        period1 = self.ledger[-2]
        log_str += f'p1_completed: {self.adj_cnc(period1)}. p1_length: {period1.length}. '

        if len(self.ledger) < 3:
            return log_str

        period0 = self.ledger[-3]
        log_str += f'p0_completed: {self.adj_cnc(period0)}. p0_length: {period0.length}. '

        return log_str

    def log_to_move(self):
        self.tick_data['awaiting_dispatch'] = self.awaiting_dispatch
        self.tick_data['demand_rate'] = float(self.demand_rate)
        self.tick_data['storage_completed_rate'] = float(self.storage_complete_rate)
        self.tick_data['integral_term_w_coefficient'] = float(self.period.integral_term * self.ki)
        self.tick_data['integral_term'] = float(self.period.integral_term)
        self.tick_data['derivative_term_w_coefficient'] = float(self.period.derivative_term * self.kd)
        self.tick_data['derivative_term'] = float(self.period.derivative_term)
        self.tick_data['proportional_term_w_coefficient'] = float(self.period.proportional_term * self.kp)
        self.tick_data['proportional_term'] = float(self.period.proportional_term)
        self.tick_data['cnc_headroom'] = self.cnc_headroom
        self.tick_data['aw_headroom'] = self.aw_headroom

    @property
    def demand_rate(self):
        if len(self.ledger) < 2:
            return 0

        period2 = self.ledger[-1]
        period1 = self.ledger[-2]
        return Fraction(period2.lt_demanded - period1.lt_demanded, period2.length)

    @property
    def storage_complete_rate(self):
        if len(self.ledger) < 2:
            return 0

        period2 = self.ledger[-1]
        period1 = self.ledger[-2]
        return Fraction(period2.lt_completed - period1.lt_completed, period2.length)

    @property
    def lifetime_demands(self):
        return self.pipeline['completed'].demanded

    @property
    def lifetime_completes(self):
        return self.pipeline['completed'].counter

    @property
    def completed(self):
        return len(self.pipeline['completed'])

    @property
    def in_progress(self):
        return self.counter - len(self.pipeline['consumed'])

    @property
    def awaiting_dispatch(self):
        return self.in_progress - self.inflight - self.completed - len(self)

    @property
    def inflight(self):
        return len(self.pipeline['inflight'])

    def to_move(self):
        self.log_to_move()

        if self.rate() == 0 and self.sample_io is None and len(self.source):
            self.sample_io = next(iter(self.source))
            return frozenset([self.sample_io])

        to_move = super().to_move()
        if self.sample_io is None:
            self.sample_io = next(iter(to_move), None)
        return to_move

    def adjust(self):
        self.period.length = self.tick - self.period.tick
        rate = self.period.rate

        log_str = f'Tick: {self.tick}. Starting Rate: {float(rate)}. '

        integral_term = self.integral_term * self.ki
        proportional_term = self.proportional_term * self.kp
        derivative_term = self.derivative_term * self.kd

        terms = integral_term + proportional_term + derivative_term
        if terms > 0 and terms <= 0.0001:
            self.cnc_headroom = math.ceil(self.period.cnc_headroom * 0.5)

        rate += terms

        if rate < 0:
            rate = 0

        log_str += f'Rate: {float(rate)}. '
        print(log_str)
        print(self.pid_log)
        print(self.completed_log)
        period = PIDPrefetchInterval(tick=self.tick, rate=rate, completed=self.completed,
                                  awaiting_dispatch=self.awaiting_dispatch,
                                  inflight=self.inflight,
                                     cnc_headroom=self.cnc_headroom,
                                  proportional_term=self.proportional_term,
                                  integral_term=self.integral_term,
                                  derivative_term=self.derivative_term,
                                  lt_demanded=self.lifetime_demands,
                                  lt_completed=self.lifetime_completes)

        self.ledger.append(period)
        self.sample_io = None
