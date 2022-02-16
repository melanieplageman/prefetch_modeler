from dataclasses import dataclass
from typing import Callable
from model import TestPipeline
from fractions import Fraction

class Duration:
    def __init__(self, microseconds=0, milliseconds=0, seconds=0):
        self.total = microseconds + (milliseconds * 1000) + (seconds * 1000 * 1000)

class Rate:
    def __init__(self, per_microsecond=0, per_millisecond=0, per_second=0):
        if per_microsecond:
            if per_millisecond or per_second:
                raise ValueError('Can only specify one Rate unit')
            self.value = Fraction(per_microsecond)
        elif per_millisecond:
            if per_microsecond or per_second:
                raise ValueError('Can only specify one Rate unit')
            self.value = Fraction(per_millisecond, 1000)

            if self.value.denominator != 1 and self.value.numerator != 1:
                raise ValueError(f"per_millisecond={per_millisecond} must be divisible by 1000")
        elif per_second:
            if per_millisecond or per_microsecond:
                raise ValueError('Can only specify one Rate unit')
            self.value = Fraction(per_second, 1000 * 1000)

            if self.value.denominator != 1 and self.value.numerator != 1:
                raise ValueError(f"per_second={per_second} must be divisible by 1,000,000")


@dataclass(frozen=True)
class PipelineConfiguration:
    inflight_cap: int
    submission_overhead: Duration
    max_iops: int
    base_completion_latency: Duration
    consumption_rate: Rate
    prefetch_distance_algorithm: Callable
    completed_cap: int
    completion_target_distance: int
    min_dispatch: int
    max_inflight: int

    def generate_pipeline(self, *args, **kwargs):
        pipeline = TestPipeline()

        pipeline.prefetched_bucket.inflight_cap = self.inflight_cap

        pipeline.submitted_bucket.LATENCY = self.submission_overhead.total

        pipeline.inflight_bucket.MAX_IOPS = self.max_iops
        pipeline.inflight_bucket.BASE_COMPLETION_LATENCY = self.base_completion_latency.total

        pipeline.completed_bucket.consumption_rate = self.consumption_rate.value

        pipeline.prefetched_bucket.desired_move_size_override = self.prefetch_distance_algorithm
        pipeline.prefetched_bucket.completed_cap = self.completed_cap

        if self.completion_target_distance > self.completed_cap:
            raise ValueError(f'Value {self.completion_target_distance} for '
                             f'completion_target_distance exceeds completed_cap '
                             f'value of {self.completed_cap}.')

        pipeline.prefetched_bucket.completion_target_distance = self.completion_target_distance

        pipeline.prefetched_bucket.min_dispatch = self.min_dispatch

        if self.max_inflight > self.inflight_cap:
            raise ValueError(f'Value {self.max_inflight} for max_inflight '
                             f'exceeds inflight_cap of {self.inflight_cap}.')

        pipeline.prefetched_bucket.max_inflight = self.max_inflight

        return pipeline
