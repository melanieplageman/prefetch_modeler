from prefetch_modeler.core import RateBucket, Rate, Interval, Duration
from dataclasses import dataclass
from fractions import Fraction
import itertools


@dataclass(kw_only=True)
class PIDPrefetchInterval(Interval):
    completed: int
    awaiting_dispatch: int
    inflight: int
    lt_demanded: int
    lt_completed: int

@dataclass
class RateLogItem:
    tick: int
    raw_rate: Fraction
    demand_rate: Fraction

class ControlPrefetcher(RateBucket):
    name = 'remaining'
    og_rate = Rate(per_second=6000)
    sample_period = 1000
    raw_lookback = 66
    avg_lookback = 10

    def __init__(self, *args, **kwargs):
        self.ledger = [PIDPrefetchInterval(tick=0, rate=self.og_rate.value,
                                           completed=0, awaiting_dispatch=0,
                                           inflight=0, lt_demanded=0,
                                           lt_completed=0)]

        self.rate_log = [RateLogItem(tick=0, raw_rate=0, demand_rate=0)]
        self.next_sample = self.sample_period
        super().__init__(*args, **kwargs)
        self._rate = self.rate()
        print(f"Initial Rate: {self._rate} {float(self._rate)}")

    def get_periods(self):
        if len(self.ledger) < self.raw_lookback:
            return None, None

        period_newer = self.ledger[-1]
        period_older = self.ledger[-1 * self.raw_lookback]

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
        move_record = list(reversed(self.pipeline['completed'].move_record))
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

        # time_elapsed += self.tick - intervals[0][0].tick

        self.avg_lookback = 4
        total = sum([item.raw_rate for item in itertools.islice(reversed(self.rate_log), self.avg_lookback)])
        total += raw_rate
        demand_rate = Fraction(total, self.avg_lookback + 1)
        rlog = RateLogItem(tick=self.tick, raw_rate=raw_rate, demand_rate=demand_rate)
        self.rate_log.append(rlog)


        print(f'Tick: {self.tick}. number_moved: {number_moved}. time_elapsed: {time_elapsed}. rate: {raw_rate} total: {total}, length: {self.avg_lookback + 1}, demand_rate: {demand_rate}')

        # period_older, period_newer = self.get_periods()
        # if period_older is None or period_newer is None:
        #     return 0

        # length = period_newer.tick - period_older.tick
        # num_demanded = period_newer.lt_demanded - period_older.lt_demanded
        # print(f'Tick: {self.tick}. period length: {length}. number demanded: {num_demanded}')
        # return Fraction(num_demanded, length)

        return demand_rate

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
        return next_action
        # return max(min(self.next_sample, next_action), self.tick)

    def run(self, *args, **kwargs):
        self.log_rates()
        self.tick_data['awaiting_dispatch'] = self.awaiting_dispatch
        # if len(self.ledger) > 2:
        #     self.log_rates()
        # if self.should_sample():
        #     self.sample()
        super().run(*args, **kwargs)

    def log_rates(self):
        self.tick_data['demand_rate'] = float(self.demand_rate)
        self.tick_data['max_iops'] = float(self.pipeline['submitted'].storage_speed)
        # self.tick_data['storage_completed_rate'] = float(self.storage_complete_rate)
        # self.tick_data['awd_rate'] = float(self.awd_rate)
        # self.tick_data['cnc_rate'] = float(self.cnc_rate)

    def adjust(self):
        pass

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
