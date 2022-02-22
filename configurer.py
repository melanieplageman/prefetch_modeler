from dataclasses import dataclass, asdict
from typing import Callable
from model import TestPipeline
from fractions import Fraction
from enum import Enum, auto

class Unit(Enum):
    MICROSECOND = auto()
    MILLISECOND = auto()
    SECOND = auto()

class Duration:
    def __init__(self, microseconds=0, milliseconds=0, seconds=0):
        self.total = microseconds + (milliseconds * 1000) + (seconds * 1000 * 1000)

    # TODO: make this say per unit
    def __str__(self):
        return f'{self.total} microseconds'

class Rate:
    def __init__(self, per_microsecond=0, per_millisecond=0, per_second=0):
        if per_microsecond:
            if per_millisecond or per_second:
                raise ValueError('Can only specify one Rate unit')
            self.value = Fraction(per_microsecond)
            self.original_unit = Unit.MICROSECOND

        elif per_millisecond:
            if per_microsecond or per_second:
                raise ValueError('Can only specify one Rate unit')
            self.value = Fraction(per_millisecond, 1000)
            self.original_unit = Unit.MILLISECOND

            if self.value.denominator != 1 and self.value.numerator != 1:
                raise ValueError(f"per_millisecond={per_millisecond} must be divisible by 1000")

        elif per_second:
            if per_millisecond or per_microsecond:
                raise ValueError('Can only specify one Rate unit')
            self.value = Fraction(per_second, 1000 * 1000)
            self.original_unit = Unit.SECOND

            if self.value.denominator != 1 and self.value.numerator != 1:
                raise ValueError(f"per_second={per_second} must be divisible by 1,000,000")

    def __str__(self):
        extension = f' per {(self.original_unit.name).lower()}'

        if self.original_unit == Unit.MICROSECOND:
            return f'{int(self.value)}' + extension
        if self.original_unit == Unit.MILLISECOND:
            return f'{int(self.value * 1000)}' + extension
        if self.original_unit == Unit.SECOND:
            return f'{int(self.value * 1000 * 1000)}' + extension

@dataclass
class PrefetchConfiguration:
    inflight_cap: int = 100
    completed_cap: int = 200
    min_dispatch: int = 10
    initial_completion_target_distance: int = 12
    initial_max_inflight: int = 10

@dataclass(frozen=True)
class PipelineConfiguration:
    prefetch_configuration: PrefetchConfiguration
    submission_overhead: Duration
    max_iops: int
    base_completion_latency: Duration

    def __str__(self):
        return '\n'.join(f'{k}: {str(v)}' for k, v in asdict(self).items())

    def generate_pipeline(self, *args, **kwargs):
        pipeline = TestPipeline()

        pipeline.submitted_bucket.LATENCY = self.submission_overhead.total

        pipeline.inflight_bucket.MAX_IOPS = self.max_iops
        pipeline.inflight_bucket.BASE_COMPLETION_LATENCY = self.base_completion_latency.total

        pipeline.prefetched_bucket.inflight_cap = self.prefetch_configuration.inflight_cap
        pipeline.prefetched_bucket.completed_cap = self.prefetch_configuration.completed_cap
        pipeline.prefetched_bucket.min_dispatch = self.prefetch_configuration.min_dispatch

        if self.prefetch_configuration.initial_completion_target_distance > self.prefetch_configuration.completed_cap:
            raise ValueError(f'Value {self.completion_target_distance} for '
                             f'completion_target_distance exceeds completed_cap '
                             f'value of {self.completed_cap}.')

        pipeline.prefetched_bucket.completion_target_distance = self.prefetch_configuration.initial_completion_target_distance


        if self.prefetch_configuration.initial_max_inflight > self.prefetch_configuration.inflight_cap:
            raise ValueError(f'Value {self.max_inflight} for max_inflight '
                             f'exceeds inflight_cap of {self.inflight_cap}.')

        pipeline.prefetched_bucket.max_inflight = self.prefetch_configuration.initial_max_inflight

        return pipeline
