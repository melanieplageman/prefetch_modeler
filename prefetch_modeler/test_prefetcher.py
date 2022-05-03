from prefetch_modeler.core import RateBucket, Rate, Interval, Duration
from dataclasses import dataclass
from fractions import Fraction

@dataclass(kw_only=True)
class PIDPrefetchInterval(Interval):
    completed: int
    awaiting_dispatch: int
    inflight: int
    lt_demanded: int
    lt_completed: int

class TestPrefetcher(RateBucket):
    name = 'remaining'
    og_rate = Rate(per_second=6000)
    sample_period = 1000
    lookback = 66

    def __init__(self, *args, **kwargs):
        self.ledger = [PIDPrefetchInterval(tick=0, rate=self.og_rate.value,
                                           completed=0, awaiting_dispatch=0,
                                           inflight=0, lt_demanded=0,
                                           lt_completed=0)]
        self.next_sample = self.sample_period
        super().__init__(*args, **kwargs)
        self._rate = self.rate()
        print(f"Initial Rate: {self._rate} {float(self._rate)}")

    def get_periods(self):
        if len(self.ledger) < self.lookback:
            return None, None

        period_newer = self.ledger[-1]
        period_older = self.ledger[-1 * self.lookback]

        # print(f'length: {period_newer.tick - period_older.tick}')
        return period_older, period_newer

    def rate(self):
        return self.period.rate

    @property
    def period(self):
        return self.ledger[-1]

    @property
    def lifetime_demands(self):
        return self.pipeline['consumed'].counter
        # return self.pipeline['completed'].demanded

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

    @property
    def awd_rate(self):
        period_older, period_newer = self.get_periods()
        if period_older is None or period_newer is None:
            return 0

        length = period_newer.tick - period_older.tick
        return Fraction(period_newer.awaiting_dispatch - period_older.awaiting_dispatch,
                        length)

    @property
    def cnc_rate(self):
        period_older, period_newer = self.get_periods()
        if period_older is None or period_newer is None:
            return 0

        length = period_newer.tick - period_older.tick
        return Fraction(period_newer.completed - period_older.completed,
                        length)

    @property
    def demand_rate(self):
        period_older, period_newer = self.get_periods()
        if period_older is None or period_newer is None:
            return 0

        length = period_newer.tick - period_older.tick
        return Fraction(period_newer.lt_demanded - period_older.lt_demanded,
                        length)

    @property
    def storage_complete_rate(self):
        period_older, period_newer = self.get_periods()
        if period_older is None or period_newer is None:
            return 0

        length = period_newer.tick - period_older.tick
        return Fraction(period_newer.lt_completed - period_older.lt_completed,
                        length)

    def should_sample(self):
        return self.tick >= self.next_sample

    def next_action(self):
        next_action = super().next_action()
        return max(min(self.next_sample, next_action), self.tick)

    def run(self, *args, **kwargs):
        if len(self.ledger) > 2:
            self.log_rates()
        if self.should_sample():
            self.sample()
        super().run(*args, **kwargs)

    def log_rates(self):
        self.tick_data['awaiting_dispatch'] = self.awaiting_dispatch
        self.tick_data['demand_rate'] = float(self.demand_rate)
        self.tick_data['storage_completed_rate'] = float(self.storage_complete_rate)
        self.tick_data['awd_rate'] = float(self.awd_rate)
        self.tick_data['cnc_rate'] = float(self.cnc_rate)

    def sample(self):
        self.period.length = self.tick - self.period.tick
        period = PIDPrefetchInterval(tick=self.tick, rate=self.period.rate,
                                     completed=self.completed,
                                     awaiting_dispatch=self.awaiting_dispatch,
                                     inflight=self.inflight,
                                     lt_demanded=self.lifetime_demands,
                                     lt_completed=self.lifetime_completes)

        self.next_sample = self.tick + self.sample_period

        self.ledger.append(period)
