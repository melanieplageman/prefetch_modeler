from enum import Enum
from prefetch_modeler.core import GateBucket, ContinueBucket, \
GlobalCapacityBucket, RateBucket, SamplingRateBucket, Rate, Interval, Duration
from fractions import Fraction
from dataclasses import dataclass
import math
import itertools


class BaselineSync(GlobalCapacityBucket):
    name = 'remaining'

    def max_buffers(self):
        return 1

    def to_move(self):
        self.tick_data['awaiting_dispatch'] = self.awaiting_dispatch
        return super().to_move()

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



class BaselineFetchAll(ContinueBucket):
    name = 'remaining'

    def to_move(self):
        self.tick_data['awaiting_dispatch'] = self.awaiting_dispatch
        return super().to_move()

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


class PeriodRate:
    def __init__(self, rate, required_correction):
        self.rate = rate
        self.required_correction = required_correction

class ConstantPrefetcher(RateBucket):
    name = 'remaining'

    og_rate = Rate(per_second=2000)

    @classmethod
    def hint(cls):
        return (2, f"Constant Rate: {cls.og_rate}")

    def rate(self):
        return self.og_rate.value

@dataclass(kw_only=True)
class PrefetchInterval(Interval):
    completed: int
    # Note that this is in-progress IOs which are not in flight and not
    # completed -- waiting to be inflight
    awaiting_dispatch: int
    inflight: int
    lt_demanded: int
    lt_completed: int

def recent_mean(iterator, take=8):
    numerator = denominator = 0
    for i, number in enumerate(itertools.islice(iterator, take)):
        weight = math.exp(i)
        numerator += weight * number
        denominator += weight
    return numerator / denominator


class SamplingPrefetcher(SamplingRateBucket):
    name = 'remaining'

    og_rate = Rate(per_second=2000)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ledger = [PrefetchInterval(tick=0, rate=self.og_rate.value, completed=0,
                                awaiting_dispatch=0, inflight=0,
                                        lt_demanded=0, lt_completed=0)]
        self._rate = self.rate()
        print(f"Initial Rate: {self._rate} {float(self._rate)}")

    def to_move(self):
        return super().to_move()

    def log_to_move(self):
        self.tick_data['awaiting_dispatch'] = self.awaiting_dispatch
        self.tick_data['demand_rate'] = float(self.demand_rate)
        self.tick_data['storage_completed_rate'] = float(self.storage_complete_rate)

    def adjust(self):
        pass

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

    @property
    def lifetime_demands(self):
        return self.pipeline['completed'].demanded

    @property
    def lifetime_completes(self):
        return self.pipeline['completed'].counter

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


@dataclass(kw_only=True)
class PIDPrefetchInterval(PrefetchInterval):
    integral_term: int
    proportional_term: Fraction
    derivative_term: Fraction
    cnc_headroom: int


class PIDPrefetcher(SamplingPrefetcher):
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

    def log_to_move(self):
        super().log_to_move()
        self.tick_data['integral_term_w_coefficient'] = float(self.period.integral_term * self.ki)
        self.tick_data['integral_term'] = float(self.period.integral_term)
        self.tick_data['derivative_term_w_coefficient'] = float(self.period.derivative_term * self.kd)
        self.tick_data['derivative_term'] = float(self.period.derivative_term)
        self.tick_data['proportional_term_w_coefficient'] = float(self.period.proportional_term * self.kp)
        self.tick_data['proportional_term'] = float(self.period.proportional_term)
        self.tick_data['cnc_headroom'] = self.cnc_headroom
        self.tick_data['aw_headroom'] = self.aw_headroom

    def to_move(self):
        return super().to_move()

    def adj_awd(self, period):
        return period.awaiting_dispatch - self.aw_headroom

    def adj_cnc(self, period):
        return period.completed - self.cnc_headroom

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

        period2 = self.ledger[-1]
        period1 = self.ledger[-2]
        return self.change_cnc_rate(period1, period2)

    @property
    def derivative_term(self):
        if len(self.ledger) < 3:
            return 0

        period2 = self.ledger[-1]
        period1 = self.ledger[-2]
        period0 = self.ledger[-3]
        return self.change_change_cnc_rate(period0, period1, period2)

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

    @property
    def awd_log(self):
        log_str = f'{self.tick}: '

        if len(self.ledger) < 1:
            return log_str

        period2 = self.ledger[-1]
        p2_log_str = f'p2_awaiting_dispatch: {self.adj_awd(period2)}. p2_length: {period2.length}. '

        if len(self.ledger) < 2:
            return log_str + p2_log_str

        period1 = self.ledger[-2]
        p1_log_str = f'p1_awaiting_dispatch: {self.adj_awd(period1)}. p1_length: {period1.length}. '

        if len(self.ledger) < 3:
            return log_str + p1_log_str + p2_log_str

        period0 = self.ledger[-3]
        p0_log_str = f'p0_awaiting_dispatch: {self.adj_awd(period0)}. p2_length: {period0.length}. '

        return log_str + p0_log_str + p1_log_str + p2_log_str

    def adjust(self):
        pass


class FastPIDPrefetcher(PIDPrefetcher):
    ki = -Rate(per_second=40).value       # should be in units of per-second
    kp = -0.9                               # should be dimensionless
    kd = -Duration(microseconds=2).total  # should be in units of seconds
    cnc_headroom = 6
    aw_headroom = 2
    og_rate = Rate(per_second=2000)

    @property
    def integral_term(self):
        return self.adj_cnc(self.period)

    @property
    def proportional_term(self):
        if len(self.ledger) < 2:
            return 0

        period2 = self.ledger[-1]
        period1 = self.ledger[-2]
        return self.change_cnc_rate(period1, period2)

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

        rate += integral_term + proportional_term + derivative_term

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


class SlowPIDPrefetcher(PIDPrefetcher):
    ki = -Rate(per_second=100).value       # should be in units of per-second
    kp = -0.9                               # should be dimensionless
    kd = -Duration(microseconds=2).total  # should be in units of seconds
    cnc_headroom = 6
    aw_headroom = 2
    og_rate = Rate(per_second=8000)

    def change_awd_rate(self, period0, period1):
        return Fraction(self.adj_awd(period1) - self.adj_awd(period0), period1.length)

    @property
    def integral_term(self):
        return self.adj_awd(self.period)

    @property
    def proportional_term(self):
        if len(self.ledger) < 2:
            return 0

        # length = (self.ledger[-1].length + self.ledger[-2].length) / 2
        # length = Fraction.from_float(length)
        # return Fraction(
        #     self.adj_awd(self.ledger[-1]) - self.adj_awd(self.ledger[-2]),
        #     length)

        change = self.adj_awd(self.ledger[-1]) - self.adj_awd(self.ledger[-2])
        length = self.ledger[-1].length
        return change / length
        import math
        return math.sqrt((change * change) / (length * length))

    @property
    def derivative_term(self):
        if len(self.ledger) < 3:
            return 0

        period2 = self.ledger[-1]
        period1 = self.ledger[-2]
        period0 = self.ledger[-3]
        awdt_p1 = self.change_awd_rate(period0, period1)
        awdt_p2 = self.change_awd_rate(period1, period2)
        return Fraction(awdt_p2 - awdt_p1, period2.length + period1.length)

    @property
    def pid_log(self):
        log_str = ''

        if len(self.ledger) < 1:
            return log_str

        log_str += f'P: {float(self.proportional_term * self.kp)}. I: {float(self.integral_term * self.ki)}. D: {float(self.derivative_term * self.kd)}.'
        return log_str

    def to_move(self):
        self.tick_data['awaiting_dispatch'] = self.awaiting_dispatch
        self.tick_data['demand_rate'] = float(self.demand_rate)
        self.tick_data['storage_completed_rate'] = float(self.storage_complete_rate)
        self.log_to_move()

        if self.sample_io is not None:
            return super().to_move()

        submitted = self.pipeline['submitted']
        submitted_ios = list(submitted.source.items())

        if len(submitted_ios) == 0:

            if self.rate() == 0 and len(self.source):
                self.sample_io = next(iter(self.source))
                return frozenset([self.sample_io])

            to_move = super().to_move()
            self.sample_io = next(iter(to_move), None)
            return to_move

        else:
            self.sample_io = submitted_ios[0][0]
            if self.rate() == 0 and len(self.source):
                return frozenset([self.sample_io])
            return super().to_move()


    def adjust(self):
        self.period.length = self.tick - self.period.tick
        rate = self.period.rate

        log_str = f'Tick: {self.tick}. Starting Rate: {float(rate)}. '

        integral_term = self.integral_term * self.ki
        proportional_term = self.proportional_term * self.kp
        derivative_term = self.derivative_term * self.kd

        if self.adj_awd(self.period) > self.aw_headroom * 2:
            proportional_term = 0

        rate += integral_term + proportional_term + derivative_term

        if rate < 0:
            rate = 0

        log_str += f'Rate: {float(rate)}. '
        print(log_str)
        print(self.pid_log)
        print(self.awd_log)
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


class PIDPrefetcher2(FastPIDPrefetcher):
    ki = -Rate(per_second=20).value       # should be in units of per-second
    kp = -0.3                               # should be dimensionless
    kd = -Duration(microseconds=2).total  # should be in units of seconds
    og_rate = Rate(per_second=2000)

class PIDPrefetcher2(FastPIDPrefetcher):
    cnc_headroom = 16
    ki = -Rate(per_second=50).value
    kp = -0.6
    kd = -Duration(microseconds=20).total
    og_rate = Rate(per_second=2000)

class PIDPrefetcher3(FastPIDPrefetcher):
    cnc_headroom = 100
    ki = -Rate(per_second=20).value
    kp = -0.3
    kd = -Duration(microseconds=2).total
    og_rate = Rate(per_second=8000)

class PIDPrefetcher4(SlowPIDPrefetcher):
    ki = -Rate(per_second=2000).value
    kp = -0.9
    kd = -Duration(microseconds=10).total
    og_rate = Rate(per_second=8000)

class PIDPrefetcher1(SlowPIDPrefetcher):
    aw_headroom = 8
    ki = -Rate(per_second=10).value
    kp = -0.8
    kd = -Duration(microseconds=2).total
    og_rate = Rate(per_second=400)


prefetcher_list = [
    [BaselineFetchAll],
    # [BaselineSync],
    # [CoolPrefetcher],
    [PIDPrefetcher3],
    # [PIDPrefetcher2],
    # [PIDPrefetcher1],
    # [SimpleSamplingPrefetcher5],
    # [ConstantPrefetcher],
    # [AdjustedPrefetcher2],
]
