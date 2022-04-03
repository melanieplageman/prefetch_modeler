from prefetch_modeler.configurer import Prefetcher

def bounded_bump(value, ratio, caps):
    new_val = max(1, int(value * ratio))
    for cap in caps:
        new_val = min(new_val, cap)
    return new_val


def prefetch_num_ios(self, original):
    consumed = len(self.pipeline['consumed'])
    submitted = len(self.pipeline['submitted'])
    inflight = len(self.pipeline['inflight'])
    in_progress = self.target.counter - consumed

    self.tick_data['completion_target_distance'] = self.pipeline.completion_target_distance
    if in_progress + self.pipeline.min_dispatch >= self.pipeline.cap_in_progress:
        return 0

    if in_progress + self.pipeline.min_dispatch >= self.pipeline.completion_target_distance:
        return 0

    ctd = self.pipeline.completion_target_distance

    to_submit = min(
        self.pipeline.completion_target_distance - in_progress,
        self.pipeline.cap_in_progress - in_progress)

    # to_submit = min(len(self), to_submit)
    self.tick_data['min_dispatch'] = self.pipeline.min_dispatch
    self.tick_data['completion_target_distance'] = self.pipeline.completion_target_distance

    print(f'ctd: {ctd}. min_dispatch: {self.pipeline.min_dispatch}. cap_in_progress: {self.pipeline.cap_in_progress}. in_progress: {in_progress}. to_submit is {to_submit}')
    return to_submit

# TODO: how to make it so that I can iterate through different early stage
# handlers and plot each one
# and then iterate through "steps" in the algorithm and feature them in the
# plot too
def adjust1(self, original):
    consumed = len(self.pipeline['consumed'])
    submitted = len(self.pipeline['submitted'])
    inflight = len(self.pipeline['inflight'])
    completed = len(self.pipeline['completed'])
    in_progress = self.target.counter - consumed

    if submitted == 0:
        return

    if consumed == 0:
        return

    ctd = self.pipeline.completion_target_distance

    caps = [self.pipeline.cap_in_progress]
    if completed < 0.9 * ctd:
        self.pipeline.completion_target_distance = bounded_bump(ctd, 1.2, caps)

    if in_progress < 0.5 * ctd:
        self.pipeline.completion_target_distance = bounded_bump(in_progress, 1.2, caps)


def adjust2(self, original):
    consumed = len(self.pipeline['consumed'])
    submitted = len(self.pipeline['submitted'])
    inflight = len(self.pipeline['inflight'])
    completed = len(self.pipeline['completed'])
    in_progress = self.target.counter - consumed

    if submitted == 0:
        return

    if consumed == 0:
        return

    ctd = self.pipeline.completion_target_distance
    # if submitted > 1.2 * inflight:
    #     self.pipeline.completion_target_distance = bounded_bump(ctd, 0.8, caps)

    caps = [self.pipeline.cap_in_progress]
    if completed < 0.8 * ctd:
        self.pipeline.completion_target_distance = bounded_bump(ctd, 1.2, caps)

    if in_progress < 0.5 * ctd:
        self.pipeline.completion_target_distance = bounded_bump(in_progress, 1.2, caps)


def adjust3(self, original):
    consumed = len(self.pipeline['consumed'])
    submitted = len(self.pipeline['submitted'])
    inflight = len(self.pipeline['inflight'])
    completed = len(self.pipeline['completed'])
    in_progress = self.target.counter - consumed

    if submitted == 0:
        return

    if consumed == 0:
        return

    ctd = self.pipeline.completion_target_distance
    # if submitted > 1.2 * inflight:
    #     self.pipeline.completion_target_distance = bounded_bump(ctd, 0.8, caps)

    caps = [self.pipeline.cap_in_progress]
    if completed < 0.3 * ctd:
        self.pipeline.completion_target_distance = bounded_bump(ctd, 1.2, caps)

    if in_progress < 0.5 * ctd:
        self.pipeline.completion_target_distance = bounded_bump(in_progress, 1.2, caps)

def adjust4(self, original):
    consumed = len(self.pipeline['consumed'])
    submitted = len(self.pipeline['submitted'])
    inflight = len(self.pipeline['inflight'])
    completed = len(self.pipeline['completed'])
    in_progress = self.target.counter - consumed

    if submitted == 0:
        return

    if consumed == 0:
        return

    ctd = self.pipeline.completion_target_distance

    caps = [self.pipeline.cap_in_progress]
    if completed < 0.90 * ctd:
        self.pipeline.completion_target_distance = bounded_bump(ctd, 1.2, caps)
        self.pipeline.min_dispatch = int(self.pipeline.min_dispatch * 2)

    if completed > 1.5 * ctd:
        self.pipeline.completion_target_distance = bounded_bump(ctd, 0.8, caps)



adjuster_list = [
    adjust1,
    adjust2,
    adjust3,
    adjust4,
]

prefetcher_list = [
    Prefetcher(
        id = i + 1,
        prefetch_num_ios_func = prefetch_num_ios,
        adjust_func = adjuster,
        min_dispatch=2,
        initial_completion_target_distance=20,
        cap_in_progress=200,
    ) for i, adjuster in enumerate(adjuster_list)
]
