from prefetch_modeler.core import GateBucket


def prefetcher_type(prefetch_num_ios_func, adjust_func, min_dispatch,
                    initial_completion_target_distance, cap_in_progress):
    if initial_completion_target_distance > cap_in_progress:
        raise ValueError(f'Value {initial_completion_target_distance} for ' f'completion_target_distance exceeds cap_in_progress ' f'value of {cap_in_progress}.')

    class remaining(GateBucket):
        """
        A bucket that will move the number of IOs specified by an algorithm, with
        the option of modifying the algorithm on each run.
        """

        def __init__(self, *args, **kwargs):
            self.cap_in_progress = cap_in_progress
            self.completion_target_distance = initial_completion_target_distance
            self.min_dispatch = min_dispatch
            super().__init__(*args, **kwargs)

        def adjust(self):
            if adjust_func is not None:
                adjust_func(self)

        def wanted_move_size(self):
            return prefetch_num_ios_func(self)

        def run(self):
            if not self.tick == 0:
                self.adjust()
            super().run()

    return [remaining]


def bounded_bump(value, ratio, caps):
    new_val = max(1, int(value * ratio))
    for cap in caps:
        new_val = min(new_val, cap)
    return new_val


def prefetch_num_ios(self):
    consumed = len(self.pipeline['consumed'])
    submitted = len(self.pipeline['submitted'])
    inflight = len(self.pipeline['inflight'])
    in_progress = self.target.counter - consumed

    self.tick_data['completion_target_distance'] = self.completion_target_distance
    if in_progress + self.min_dispatch >= self.cap_in_progress:
        return 0

    if in_progress + self.min_dispatch >= self.completion_target_distance:
        return 0

    ctd = self.completion_target_distance

    to_submit = min(
        self.completion_target_distance - in_progress,
        self.cap_in_progress - in_progress)

    # to_submit = min(len(self), to_submit)
    self.tick_data['min_dispatch'] = self.min_dispatch
    self.tick_data['completion_target_distance'] = self.completion_target_distance

    print(f'ctd: {ctd}. min_dispatch: {self.min_dispatch}. cap_in_progress: {self.cap_in_progress}. in_progress: {in_progress}. to_submit is {to_submit}')
    return to_submit

# TODO: how to make it so that I can iterate through different early stage
# handlers and plot each one
# and then iterate through "steps" in the algorithm and feature them in the
# plot too
def adjust1(self):
    consumed = len(self.pipeline['consumed'])
    submitted = len(self.pipeline['submitted'])
    inflight = len(self.pipeline['inflight'])
    completed = len(self.pipeline['completed'])
    in_progress = self.target.counter - consumed

    if submitted == 0:
        return

    if consumed == 0:
        return

    ctd = self.completion_target_distance

    caps = [self.cap_in_progress]
    if completed < 0.9 * ctd:
        self.completion_target_distance = bounded_bump(ctd, 1.2, caps)

    if in_progress < 0.5 * ctd:
        self.completion_target_distance = bounded_bump(in_progress, 1.2, caps)


def adjust2(self):
    consumed = len(self.pipeline['consumed'])
    submitted = len(self.pipeline['submitted'])
    inflight = len(self.pipeline['inflight'])
    completed = len(self.pipeline['completed'])
    in_progress = self.target.counter - consumed

    if submitted == 0:
        return

    if consumed == 0:
        return

    ctd = self.completion_target_distance
    # if submitted > 1.2 * inflight:
    #     self.completion_target_distance = bounded_bump(ctd, 0.8, caps)

    caps = [self.cap_in_progress]
    if completed < 0.8 * ctd:
        self.completion_target_distance = bounded_bump(ctd, 1.2, caps)

    if in_progress < 0.5 * ctd:
        self.completion_target_distance = bounded_bump(in_progress, 1.2, caps)


def adjust3(self):
    consumed = len(self.pipeline['consumed'])
    submitted = len(self.pipeline['submitted'])
    inflight = len(self.pipeline['inflight'])
    completed = len(self.pipeline['completed'])
    in_progress = self.target.counter - consumed

    if submitted == 0:
        return

    if consumed == 0:
        return

    ctd = self.completion_target_distance
    # if submitted > 1.2 * inflight:
    #     self.completion_target_distance = bounded_bump(ctd, 0.8, caps)

    caps = [self.cap_in_progress]
    if completed < 0.3 * ctd:
        self.completion_target_distance = bounded_bump(ctd, 1.2, caps)

    if in_progress < 0.5 * ctd:
        self.completion_target_distance = bounded_bump(in_progress, 1.2, caps)

def adjust4(self):
    consumed = len(self.pipeline['consumed'])
    submitted = len(self.pipeline['submitted'])
    inflight = len(self.pipeline['inflight'])
    completed = len(self.pipeline['completed'])
    in_progress = self.target.counter - consumed

    if submitted == 0:
        return

    if consumed == 0:
        return

    ctd = self.completion_target_distance

    caps = [self.cap_in_progress]
    if completed < 0.90 * ctd:
        self.completion_target_distance = bounded_bump(ctd, 1.2, caps)
        self.min_dispatch = int(self.min_dispatch * 2)

    if completed > 1.5 * ctd:
        self.completion_target_distance = bounded_bump(ctd, 0.8, caps)


adjuster_list = [
    adjust1,
    adjust2,
    adjust3,
    adjust4,
]

prefetcher_list = [
    prefetcher_type(
        prefetch_num_ios_func = prefetch_num_ios,
        adjust_func = adjuster,
        min_dispatch=2,
        initial_completion_target_distance=20,
        cap_in_progress=200,
    ) for i, adjuster in enumerate(adjuster_list)
]
