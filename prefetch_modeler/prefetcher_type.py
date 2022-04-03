from prefetch_modeler.core import GateBucket


class Prefetcher(GateBucket):
    name = 'remaining'

    """
    A bucket that will move the number of IOs specified by an algorithm, with
    the option of modifying the algorithm on each run.
    """

    cap_in_progress = 200
    completion_target_distance = 7
    min_dispatch = 1

    def __init__(self, *args, **kwargs):
        if self.completion_target_distance > self.cap_in_progress:
            raise ValueError(f'Value {self.completion_target_distance} for completion_target_distance exceeds cap_in_progress value of {self.cap_in_progress}.')
        super().__init__(*args, **kwargs)

    def adjust(self):
        pass

    def wanted_move_size(self):
        self.tick_data['completion_target_distance'] = self.completion_target_distance
        self.tick_data['min_dispatch'] = self.min_dispatch

        if self.in_progress + self.min_dispatch > self.cap_in_progress:
            return 0

        if self.in_progress + self.min_dispatch > self.completion_target_distance:
            return 0

        to_submit = max(self.min_dispatch,
                        self.completion_target_distance - self.in_progress)

        will_submit = min(len(self), to_submit)

        print(f'ctd: {self.completion_target_distance}. min_dispatch: {self.min_dispatch}. cap_in_progress: {self.cap_in_progress}. in_progress: {self.in_progress}. to_submit is {to_submit}. will_submit: {will_submit}.')
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


class AdjustedPrefetcher2(Prefetcher):
    def adjust(self):
        if self.submitted == 0 or self.consumed == 0:
            return

        ctd = self.completion_target_distance
        # if submitted > 1.2 * inflight:
        #     self.completion_target_distance = self.bounded_bump(ctd, 0.8, caps)

        caps = [self.cap_in_progress]
        # if self.completed < 0.8 * ctd:
        #     self.completion_target_distance = self.bounded_bump(ctd, 1.2, caps)

        # if self.in_progress < 0.5 * ctd:
        #     self.completion_target_distance = self.bounded_bump(self.in_progress, 1.2, caps)


class BaselineSync(Prefetcher):
    def wanted_move_size(self):
        if not self.pipeline['completed'].movement_size:
            return 0

        if self.pipeline['completed'].movement_size > 0:
            return 1
        return 0


class BaselineFetchAll(Prefetcher):
    def wanted_move_size(self):
        return len(self)

prefetcher_list = [
    [AdjustedPrefetcher2],
]
