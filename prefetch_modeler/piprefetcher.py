from prefetch_modeler.core import RateBucket, Rate, Interval, Duration
from dataclasses import dataclass
from fractions import Fraction
import itertools
import math

@dataclass(kw_only=True)
class LedgerEntry:
    tick: int
    cnc_headroom: int
    completed: int
    awaiting_dispatch: int
    inflight: int
    lt_demanded: int
    lt_completed: int

@dataclass
class RateLogItem:
    tick: int
    raw_rate: Fraction

def humanify(rate):
    return math.ceil(float(rate) * 1000 * 1000)

class PIPrefetcher(RateBucket):
    name = 'remaining'
    og_rate = Rate(per_second=6000)
    raw_lookback = 66
    avg_lookback = 10
    awd_lookback = 2
    kp = 0.5
    ki_awd = -Rate(per_second=20).value
    ki_cnc = -Rate(per_second=40).value
    kh = Rate(per_second=10).value
    cnc_headroom = 8
    min_cnc_headroom = 3

    def __init__(self, *args, **kwargs):
        self.demand_rate_log = [RateLogItem(tick=0, raw_rate=0)]
        self.ledger = [LedgerEntry(tick=0, completed=0,
                                           awaiting_dispatch=0, inflight=0,
                                           cnc_headroom=self.cnc_headroom,
                                           lt_demanded=0, lt_completed=0)]
        self.current_rate_value = self.og_rate.value
        super().__init__(*args, **kwargs)
        self._rate = self.rate()
        print(f"Initial Rate: {self._rate} {float(self._rate)}")

    def rate(self):
        return self.current_rate_value

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
    def in_progress(self):
        return self.counter - len(self.pipeline['consumed'])

    @property
    def awaiting_dispatch(self):
        return self.in_progress - self.inflight - self.completed - len(self)

    @property
    def inflight(self):
        return len(self.pipeline['inflight'])

    @property
    def cnc_rate(self):
        if len(self.ledger) < 2:
            return 0

        period_newer = self.ledger[-1]
        period_older = self.ledger[-2]

        length = period_newer.tick - period_older.tick

        return Fraction(period_newer.completed - period_older.completed,
                        length)

    @property
    def cnc_acceleration(self):
        if len(self.ledger) < 3:
            return 0

        period_newest = self.ledger[-1]
        period_mid = self.ledger[-2]
        period_oldest = self.ledger[-3]

        newer_length = period_newest.tick - period_mid.tick
        cnc_dt_newer = Fraction(period_newest.completed - period_mid.completed,
                                newer_length)

        older_length = period_mid.tick - period_oldest.tick
        cnc_dt_older = Fraction(period_mid.completed - period_oldest.completed,
                                older_length)

        return Fraction(cnc_dt_newer - cnc_dt_older, newer_length + older_length)

    @property
    def awd_rate(self):
        if len(self.ledger) < 2:
            return 0

        period_newer = self.ledger[-1]
        period_older = self.ledger[-2]

        length = period_newer.tick - period_older.tick

        return Fraction(period_newer.awaiting_dispatch - period_older.awaiting_dispatch,
                        length)

    @property
    def awd_acceleration(self):
        if len(self.ledger) < 3:
            return 0

        period_newest = self.ledger[-1]
        period_mid = self.ledger[-2]
        period_oldest = self.ledger[-3]

        newer_length = period_newest.tick - period_mid.tick
        awd_dt_newer = Fraction(period_newest.awaiting_dispatch - period_mid.awaiting_dispatch,
                                newer_length)

        older_length = period_mid.tick - period_oldest.tick
        awd_dt_older = Fraction(period_mid.awaiting_dispatch - period_oldest.awaiting_dispatch,
                                older_length)

        return Fraction(awd_dt_newer - awd_dt_older, newer_length + older_length)

    @property
    def smoothed_awd_rate(self):
        if len(self.ledger) < 3:
            return self.awd_rate

        ledger = list(reversed(self.ledger))
        intervals = list(zip(ledger, ledger[1:]))

        total = 0
        num_periods = 0
        for newer_entry, older_entry in intervals:
            change_awd = newer_entry.awaiting_dispatch - older_entry.awaiting_dispatch
            period = newer_entry.tick - older_entry.tick
            total += Fraction(change_awd, period)
            num_periods += 1
            if num_periods > self.awd_lookback:
                break

        smoothed_awd_rate = Fraction(total, num_periods)
        if smoothed_awd_rate < 0:
            smoothed_awd_rate = 0

        return smoothed_awd_rate

    @property
    def recent_awd(self):
        if len(self.ledger) < 2:
            return self.awaiting_dispatch
        awds = [item.awaiting_dispatch for item in itertools.islice(reversed(self.ledger), self.awd_lookback)]
        return math.ceil(sum(awds)/len(awds))

    @property
    def raw_demand_rate(self):
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

        # print(f'raw demand rate: {raw_rate}')
        return raw_rate

    @property
    def demand_rate(self):
        raw_demand_rate = self.raw_demand_rate

        if raw_demand_rate == 0:
            return 0

        start_idx = 0
        for i, entry in enumerate(self.demand_rate_log):
            if entry.raw_rate != 0:
                start_idx = i
                break

        usable_demand_rate_log = self.demand_rate_log[start_idx:]

        if len(usable_demand_rate_log) < self.avg_lookback:
            return raw_demand_rate

        total = sum([item.raw_rate for item in itertools.islice(reversed(usable_demand_rate_log), self.avg_lookback)])
        return Fraction(total, self.avg_lookback)

    def run(self, *args, **kwargs):
        self.log_rates()
        super().run(*args, **kwargs)

    def log_rates(self):
        self.tick_data['demand_rate'] = float(self.demand_rate)
        self.tick_data['awaiting_dispatch'] = self.awaiting_dispatch
        self.tick_data['max_iops'] = float(self.pipeline['submitted'].storage_speed)
        self.tick_data['proportional_term'] = float(self.proportional_term)
        self.tick_data['proportional_term_w_coefficient'] = float(self.proportional_term * self.kp)
        self.tick_data['cnc_integral_term_w_coefficient'] = float(self.cnc_integral_term * self.ki_cnc)
        self.tick_data['cnc_integral_term'] = float(self.cnc_integral_term)
        self.tick_data['awd_integral_term_w_coefficient'] = float(self.awd_integral_term * self.ki_awd)
        self.tick_data['awd_integral_term'] = float(self.awd_integral_term)
        self.tick_data['cnc_headroom'] = self.cnc_headroom

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
        # iterm = self.completed - self.cnc_headroom + self.awaiting_dispatch
        iterm = self.completed - self.cnc_headroom
        return iterm

    @property
    def awd_integral_term(self):
        if self.demand_rate == 0:
            return 0

        awd_sq = self.awaiting_dispatch * self.awaiting_dispatch
        return awd_sq

    def adjust(self):
        demand_rate = self.demand_rate
        prefetch_rate = self.rate()

        p = self.proportional_term
        cnc_i = self.cnc_integral_term

        if self.completed < self.cnc_headroom and self.cnc_rate == 0 and self.awd_rate > 0:
            cnc_i = 0

        awd_i = self.awd_integral_term

        pc = p * self.kp
        cnc_ic = cnc_i * self.ki_cnc
        awd_ic = awd_i * self.ki_awd

        if self.completed < self.cnc_headroom and self.recent_awd > 0:
            adjustment = self.recent_awd * self.kh
            self.cnc_headroom = max(self.min_cnc_headroom, math.ceil(self.cnc_headroom - adjustment))

        new_rate = prefetch_rate
        new_rate += pc
        new_rate += cnc_ic
        new_rate += awd_ic
        if new_rate < 0:
            new_rate = 0
        self.current_rate_value = new_rate

        if pc <= 0.0001 and cnc_ic + awd_ic <= 0.01:
            self.cnc_headroom = max(math.ceil(self.cnc_headroom * 0.5), 2)

        backlog_log = f'cnc: {self.completed}. awd: {self.awaiting_dispatch}. cnc headroom: {self.cnc_headroom}. '
        roc_log = f'cnc_rate: {self.cnc_rate}. awd_rate: {self.awd_rate}. cnc_acc: {self.cnc_acceleration}. awd_acc: {self.awd_acceleration}'

        completions = self.lifetime_completes - self.ledger[-1].lt_completed
        clen = self.tick - self.ledger[-1].tick
        if self.lifetime_completes > 1 and completions <= 1:
            self.cnc_headroom = max(self.min_cnc_headroom, math.ceil(self.cnc_headroom * 0.5))

        completion_info_log = f'completions this period: {completions}. period length: {clen}'

        prlog = f'pr: {humanify(prefetch_rate)}. '
        drlog = f'demand rate: {demand_rate}, {humanify(demand_rate)}. '
        plog = f'P: {humanify(p)}, PwC: {humanify(pc)}. '
        cnc_ilog = f'CNC I: {cnc_i}. CNC_IwC: {humanify(cnc_ic)}. '
        awd_ilog = f'AWD I: {awd_i}. AWD_IwC: {humanify(awd_ic)}. '
        nprlog = f'new pr: {humanify(new_rate)}. '

        print(f'Tick: {self.tick}. {backlog_log}')
        print(f'Tick: {self.tick}. {roc_log}')
        print(f'Tick: {self.tick}. {completion_info_log}')
        print(f'Tick: {self.tick}. ' + prlog + drlog + plog + cnc_ilog + awd_ilog + nprlog)

        self.demand_rate_log.append(RateLogItem(tick=self.tick, raw_rate=self.raw_demand_rate))
        self.ledger.append(LedgerEntry(tick=self.tick, completed=self.completed,
                                           awaiting_dispatch=self.awaiting_dispatch,
                                       inflight=self.inflight,
                                           cnc_headroom=self.cnc_headroom,
                                           lt_demanded=self.lifetime_demands,
                                       lt_completed=self.lifetime_completes))
