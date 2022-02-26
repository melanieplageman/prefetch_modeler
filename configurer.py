from dataclasses import dataclass, asdict
from typing import Callable
from model import TestPipeline
from units import Rate, Duration

class Iteration:
    def __init__(self, assignments):
        self.assignments = assignments

    def configure_pipeline(self, pipeline):
        for k, v in self.assignments.items():
            pipeline.registry[k] = v

class Storage:
    def __init__(self, max_iops, cap_inflight, cap_in_progress,
                 submission_overhead, base_completion_latency):
        self.max_iops = max_iops
        self.cap_inflight = cap_inflight
        self.cap_in_progress = cap_in_progress
        self.submission_overhead = submission_overhead
        self.base_completion_latency = base_completion_latency

    def __str__(self):
        return ', '.join([f'{k}: {v}' for k, v in self.__dict__.items()])

class Workload:
    def __init__(self, volume, duration=None, consumption_rate_func=None):
        self.volume = volume
        self.duration = duration.total if duration else None
        self.consumption_rate_func = consumption_rate_func

    def configure_pipeline(self, pipeline):
        pipeline.registry['CompleteBucket.consumption_rate'] = self.consumption_rate_func


@dataclass
class PrefetchConfiguration:
    prefetch_size_func: Callable
    min_dispatch: int = 10
    initial_completion_target_distance: int = 12
    initial_target_inflight: int = 10

    def configure_pipeline(self, pipeline):
        pipeline.registry['PrefetchBucket.wanted_move_size'] = self.prefetch_size_func

@dataclass(frozen=True)
class PipelineConfiguration:
    prefetch_configuration: PrefetchConfiguration
    storage: Storage

    def __str__(self):
        return '\n'.join(f'{k}: {str(v)}' for k, v in asdict(self).items())

    def generate_pipeline(self, *args, **kwargs):
        pipeline = TestPipeline()

        pipeline.submitted_bucket.LATENCY = self.storage.submission_overhead.total

        pipeline.inflight_bucket.MAX_IOPS = self.storage.max_iops
        pipeline.inflight_bucket.BASE_COMPLETION_LATENCY = self.storage.base_completion_latency.total

        pipeline.cap_inflight = self.storage.cap_inflight
        pipeline.cap_in_progress = self.storage.cap_in_progress
        pipeline.prefetched_bucket.min_dispatch = self.prefetch_configuration.min_dispatch

        if self.prefetch_configuration.initial_completion_target_distance > self.storage.cap_in_progress:
            raise ValueError(f'Value {self.prefetch_configuration.initial_completion_target_distance} for ' f'completion_target_distance exceeds cap_in_progress ' f'value of {self.storage.cap_in_progress}.')

        pipeline.completion_target_distance = self.prefetch_configuration.initial_completion_target_distance


        if self.prefetch_configuration.initial_target_inflight > self.storage.cap_inflight:
            raise ValueError(f'Value {self.prefetch_configuration.initial_target_inflight} for target_inflight ' f'exceeds cap_inflight of {self.storage.cap_inflight}.')

        pipeline.target_inflight = self.prefetch_configuration.initial_target_inflight

        return pipeline
