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

class Iteration:
    def __init__(self, assignments):
        self.assignments = assignments

    def configure_pipeline(self, pipeline):
        for k, v in self.assignments.items():
            pipeline.registry[k] = v

@dataclass
class PrefetchConfiguration:
    cap_inflight: int = 100
    cap_in_progress: int = 200
    min_dispatch: int = 10
    initial_completion_target_distance: int = 12
    initial_target_inflight: int = 10

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

        pipeline.cap_inflight = self.prefetch_configuration.cap_inflight
        pipeline.cap_in_progress = self.prefetch_configuration.cap_in_progress
        pipeline.prefetched_bucket.min_dispatch = self.prefetch_configuration.min_dispatch

        if self.prefetch_configuration.initial_completion_target_distance > self.prefetch_configuration.cap_in_progress:
            raise ValueError(f'Value {self.prefetch_configuration.initial_completion_target_distance} for ' f'completion_target_distance exceeds cap_in_progress ' f'value of {self.prefetch_configuration.cap_in_progress}.')

        pipeline.completion_target_distance = self.prefetch_configuration.initial_completion_target_distance


        if self.prefetch_configuration.initial_target_inflight > self.prefetch_configuration.cap_inflight:
            raise ValueError(f'Value {self.prefetch_configuration.initial_target_inflight} for target_inflight ' f'exceeds cap_inflight of {self.prefetch_configuration.cap_inflight}.')

        pipeline.target_inflight = self.prefetch_configuration.initial_target_inflight

        return pipeline
