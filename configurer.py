from dataclasses import dataclass, asdict
from typing import Callable
from model import TestPipeline
from units import Rate, Duration

class Storage:
    def __init__(self, completion_latency_func, base_completion_latency,
                 submission_overhead,
                 max_iops, cap_inflight, cap_in_progress):
        self.completion_latency_func = completion_latency_func
        self.base_completion_latency = base_completion_latency
        self.submission_overhead = submission_overhead
        self.max_iops = max_iops
        self.cap_inflight = cap_inflight
        self.cap_in_progress = cap_in_progress

    def configure_pipeline(self, pipeline):
        pipeline.registry['InflightBucket.latency'] = self.completion_latency_func

    # TODO: add inheritance here and also make functions print names
    def __str__(self):
        return ', '.join([f'{k}: {v}' for k, v in self.__dict__.items()])

class Workload:
    def __init__(self, volume, duration=None, consumption_rate_func=None):
        self.volume = volume
        self.duration = duration.total if duration else None
        self.consumption_rate_func = consumption_rate_func

    def __str__(self):
        return ', '.join([f'{k}: {v}' for k, v in self.__dict__.items()])

    def configure_pipeline(self, pipeline):
        pipeline.registry['CompleteBucket.consumption_rate'] = self.consumption_rate_func


class Prefetcher:
    def __init__(self, prefetch_size_func, adjusters, min_dispatch,
                 initial_completion_target_distance, initial_target_inflight):
        self.prefetch_size_func = prefetch_size_func
        self.adjusters = adjusters
        self.min_dispatch = min_dispatch
        self.completion_target_distance = initial_completion_target_distance
        self.target_inflight = initial_target_inflight

    def __str__(self):
        return ', '.join([f'{k}: {v}' for k, v in self.__dict__.items()])

    def configure_pipeline(self, pipeline):
        pipeline.registry['PrefetchBucket.wanted_move_size'] = self.prefetch_size_func
        for k, v in self.adjusters.items():
            pipeline.registry[k] = v

@dataclass(frozen=True)
class PipelineConfiguration:
    storage: Storage
    workload: Workload
    prefetcher: Prefetcher

    def __str__(self):
        return '\n'.join(f'{k}: {str(v)}' for k, v in asdict(self).items())

    def generate_pipeline(self, *args, **kwargs):
        pipeline = TestPipeline()

        pipeline.submitted_bucket.LATENCY = self.storage.submission_overhead.total

        pipeline.inflight_bucket.MAX_IOPS = self.storage.max_iops
        pipeline.inflight_bucket.base_completion_latency = self.storage.base_completion_latency.total

        pipeline.cap_inflight = self.storage.cap_inflight
        pipeline.cap_in_progress = self.storage.cap_in_progress
        pipeline.prefetched_bucket.min_dispatch = self.prefetcher.min_dispatch

        if self.prefetcher.completion_target_distance > self.storage.cap_in_progress:
            raise ValueError(f'Value {self.prefetcher.completion_target_distance} for ' f'completion_target_distance exceeds cap_in_progress ' f'value of {self.storage.cap_in_progress}.')

        pipeline.completion_target_distance = self.prefetcher.completion_target_distance


        if self.prefetcher.target_inflight > self.storage.cap_inflight:
            raise ValueError(f'Value {self.prefetcher.target_inflight} for target_inflight ' f'exceeds cap_inflight of {self.storage.cap_inflight}.')

        pipeline.target_inflight = self.prefetcher.target_inflight

        self.storage.configure_pipeline(pipeline)
        self.workload.configure_pipeline(pipeline)
        self.prefetcher.configure_pipeline(pipeline)

        return pipeline
