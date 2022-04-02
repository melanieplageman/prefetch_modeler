
def bounded_bump(value, ratio, caps):
    new_val = max(1, int(value * ratio))
    for cap in caps:
        new_val = min(new_val, cap)
    return new_val

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


adjusters = [
    adjust1,
    adjust2,
    adjust3
]
