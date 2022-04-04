from prefetch_modeler.core import GateBucket, ContinueBucket, \
GlobalCapacityBucket, RateBucket, Rate


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

class CoolPrefetcher(RateBucket):
    name = 'remaining'

    def rate(self):
        return self.period_rate

    @property
    def completed(self):
        return len(self.pipeline['completed'])

    def __init__(self, *args, **kwargs):
        self.period_rate = Rate(per_second=2000).value
        self.sample_io = None
        self.rates = [self.period_rate]
        super().__init__(*args, **kwargs)

    def adjust(self):
        if self.sample_io is None:
            return

        if self.sample_io not in self.pipeline['completed'] and \
                self.sample_io not in self.pipeline['consumed']:
            return

        if len(self.rates) < 2:
            self.rates.append(self.period_rate)
            return

        old_rate = self.rates[-2]

        if self.completed == 0:
            new_rate = old_rate * 2
        else:
            new_rate = old_rate / 2

        self.rates.append(new_rate)
        self.period_rate = new_rate
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
    [BaselineFetchAll],
    [BaselineSync],
    [AdjustedPrefetcher2],
]
