from prefetch_modeler.core import RateBucket, Rate, TargetGroupCapacityBucket
from fractions import Fraction
import itertools
import math


TRANSFER_COEFFICIENT = 0.5


class RateLimiter(TargetGroupCapacityBucket):
    name = 'ratelimiter'
    sinusoid_period = 800

    def __init__(self, *args, **kwargs):
        self.inflight_scores = {}
        self.latency = 0
        super().__init__(*args, **kwargs)

    @property
    def period(self):
        return self.log[-1]

    @property
    def in_storage(self):
        return len(self.pipeline['minimum_latency']) + \
            len(self.pipeline['inflight']) + \
            len(self.pipeline['deadline'])

    @property
    def adjustment(self):
        return None

    def group_size(self):
        return self.in_storage

    def target_group_capacity(self):
        if len(self.inflight_scores) < 4 and self.tick < 10000:
            return 1

        scores = {
            ios: ios / math.pow(latency, 2) for ios, latency in self.inflight_scores.items()
        }
        best_score = max([ios for ios in self.inflight_scores], key=lambda ios: scores[ios])

        amplitude = max(best_score / 10, 1)
        sinusoid = amplitude * math.sin(self.tick / (2 * math.pi * self.sinusoid_period))
        result = max(math.ceil(best_score + sinusoid), 1)

        return result

    def to_move(self):
        to_move = super().to_move()
        for io in to_move:
            io.submitted = self.tick
            io.contention = self.in_storage + len(to_move)
        return to_move

    def reaction(self):
        num_ios = 0
        total_latency = 0
        total_contention = 0

        for io in self.pipeline['completed']:
            if getattr(io, 'completed', None) is None:
                io.completed = self.tick
            latency = io.completed - io.submitted
            total_latency += latency
            total_contention += io.contention
            num_ios += 1

        # If consumption rate is fast enough, IOs might always be moved to
        # consumed right away, so we need to find them and count them
        for io in self.pipeline['consumed']:
            if getattr(io, 'completed', None) is not None:
                continue
            io.completed = self.tick
            latency = io.completed - io.submitted
            total_latency += latency
            total_contention += io.contention
            num_ios += 1

        if num_ios == 0:
            return

        latency = total_latency / num_ios
        contention = int(math.ceil(total_contention / num_ios))

        # Put this measurement into the database
        old_latency = self.inflight_scores.setdefault(
            contention, latency)
        self.inflight_scores[contention] = (0.5 * latency + 0.5 * old_latency)

        maximum_contention_to_consider = contention * 2
        for i, next_contention in enumerate(range(contention, maximum_contention_to_consider), start=2):
            if next_contention not in self.inflight_scores:
                self.inflight_scores[next_contention] = latency
            else:
                coefficient = math.pow(TRANSFER_COEFFICIENT, i)
                self.inflight_scores[next_contention] = (
                    coefficient * latency + (1 - coefficient) * self.inflight_scores[next_contention]
                )

        self.latency = latency
