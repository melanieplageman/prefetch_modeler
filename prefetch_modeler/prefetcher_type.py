from enum import Enum
from prefetch_modeler.core import GateBucket, ContinueBucket, \
GlobalCapacityBucket, RateBucket, Rate
from fractions import Fraction


class Prefetcher(GateBucket):
    name = 'remaining'

    """
    A bucket that will move the number of IOs specified by an algorithm, with
    the option of modifying the algorithm on each run.
    """

    target_in_progress = 200
    completion_target_distance = 7
    min_dispatch = 1

    def __init__(self, *args, **kwargs):
        if self.completion_target_distance > self.target_in_progress:
            raise ValueError(f'Value {self.completion_target_distance} for completion_target_distance exceeds target_in_progress value of {self.target_in_progress}.')
        super().__init__(*args, **kwargs)

    def adjust(self):
        pass

    def wanted_move_size(self):
        self.tick_data['completion_target_distance'] = self.completion_target_distance
        self.tick_data['min_dispatch'] = self.min_dispatch

        if self.in_progress + self.min_dispatch > self.target_in_progress:
            return 0

        if self.in_progress + self.min_dispatch > self.completion_target_distance:
            return 0

        to_submit = max(self.min_dispatch,
                        self.completion_target_distance - self.in_progress)

        will_submit = min(len(self), to_submit)

        print(f'ctd: {self.completion_target_distance}. completed: {self.completed}. target_in_progress: {self.target_in_progress}. in_progress: {self.in_progress}. consumed: {self.consumed}. min_dispatch: {self.min_dispatch}. to_submit is {to_submit}. will_submit: {will_submit}.')

        return to_submit


    def run(self, *args, **kwargs):
        if not self.tick == 0:
            self.adjust()
        super().run(*args, **kwargs)

    @property
    def consumed(self):
        return len(self.pipeline['consumed'])

    @property
    def completed(self):
        return len(self.pipeline['completed'])

    @property
    def submitted(self):
        return len(self.pipeline['submitted'])

    @property
    def inflight(self):
        return len(self.pipeline['inflight'])

    @property
    def in_progress(self):
        return self.target.counter - self.consumed

    @staticmethod
    def bounded_bump(value, ratio, caps):
        new_val = max(1, int(value * ratio))
        for cap in caps:
            new_val = min(new_val, cap)
        return new_val


class BaselineSync(GlobalCapacityBucket):
    name = 'remaining'

    def max_buffers(self):
        return 1


class BaselineFetchAll(ContinueBucket):
    name = 'remaining'

class Correction(Enum):
    UNKNOWN = 0
    NONE = 1
    UP = 2
    DOWN = 3

class PeriodRate:
    def __init__(self, rate, required_correction):
        self.rate = rate
        self.required_correction = required_correction


class CoolPrefetcher(RateBucket):
    name = 'remaining'

    def rate(self):
        return self.period_rate.rate

    @property
    def completed(self):
        return len(self.pipeline['completed'])

    def __init__(self, *args, **kwargs):
        self.period_rate = PeriodRate(rate=Rate(per_second=200).value,
                                      required_correction=Correction.UNKNOWN)
        self.sample_io = None
        self.rates = [self.period_rate]
        self.completed_history = [0]
        self.last_adjust_tick = 0
        super().__init__(*args, **kwargs)

    def judge_old_rate(self, old_rate, completed, completed_history):
        completed_headroom = 2
        if completed < completed_history[-1] or completed < completed_headroom:
            return Correction.UP
        return Correction.DOWN

    def calculate_new_rate1(self, completed, completed_history, rate_history):
        low_rates = [pr for pr in rate_history[-8:] if pr.required_correction == Correction.UP]
        high_rates = [pr for pr in rate_history[-8:] if pr.required_correction == Correction.DOWN]
        max_low_rate = max(low_rates, key=lambda x: x.rate) if low_rates else None
        min_high_rate = min(high_rates, key=lambda x: x.rate) if high_rates else None

        if max_low_rate is None and min_high_rate is None:
            raise ValueError()

        elif max_low_rate is None:
            new_rate = PeriodRate(rate = min_high_rate.rate / 2,
                                        required_correction=Correction.UNKNOWN)

        elif min_high_rate is None:
            new_rate = PeriodRate(rate = max_low_rate.rate * 2,
                                        required_correction=Correction.UNKNOWN)

        else:
            new_rate = PeriodRate(rate = (min_high_rate.rate + max_low_rate.rate) / 2,
                                            required_correction=Correction.UNKNOWN)
        new_rate.rate = (new_rate.rate + rate_history[-1].rate) / 2
        return new_rate

    def decrease_rate(self, interval, completed, completed_history, rate_history):
        completed_change_rate = Fraction(completed - completed_history[-1],
                                         interval)

        old_rate = rate_history[-2]
        new_rate = PeriodRate(rate = old_rate.rate - completed_change_rate,
                              required_correction=Correction.UNKNOWN)

        if new_rate.rate < 0:
            new_rate.rate = old_rate / 2
        return new_rate

    def adjust(self):
        if self.sample_io is None:
            return

        if self.sample_io not in self.pipeline['completed'] and \
                self.sample_io not in self.pipeline['consumed']:
            return

        if len(self.rates) < 2:
            self.rates.append(self.period_rate)
            self.completed_history.append(self.completed)
            return

        old_rate = self.rates[-2]
        old_rate.required_correction = self.judge_old_rate(old_rate,
                                                           self.completed,
                                                           self.completed_history)

        # # if len(self.rates) > 10 and len(self.rates) < 15:
        # adjustment_str = f"completed: {self.completed}. "
        # for rate in self.rates:
        #     adjustment_str += f". {float(rate.rate)}-{rate.required_correction}"
        # print("ADJUSTMENT:" + adjustment_str)

        if old_rate.required_correction == Correction.UP:
            new_rate = PeriodRate(rate=old_rate.rate * 2,
                                   required_correction=Correction.UNKNOWN)
            # new_rate1 = self.calculate_new_rate1(self.completed,
            #                                 self.completed_history, self.rates)

        else:
            interval = self.tick - self.last_adjust_tick
            new_rate = self.decrease_rate(interval, self.completed,
                                                 self.completed_history,
                                                 self.rates)

        self.tick_data['pf_rate'] = float(new_rate.rate)


        self.last_adjust_tick = self.tick
        self.period_rate = new_rate
        self.rates.append(self.period_rate)
        self.completed_history.append(self.completed)
        self.sample_io = None

    def to_move(self):
        to_move = super().to_move()

        if not to_move:
            return to_move

        if self.sample_io is None:
            self.sample_io = next(iter(to_move))

        return to_move

    def run(self, *args, **kwargs):
        self.adjust()
        super().run(*args, **kwargs)


prefetcher_list = [
    # [BaselineFetchAll],
    # [BaselineSync],
    [CoolPrefetcher],
    # [AdjustedPrefetcher2],
]
