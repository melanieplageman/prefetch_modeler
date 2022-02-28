from dataclasses import dataclass, asdict
from typing import Callable
from model import TestPipeline
from units import Rate, Duration

class Configuration:
    # TODO: Make this print names of functions for functions
    def __str__(self):
        return ', '.join([f'{k}: {v}' for k, v in self.__dict__.items()])

@dataclass
class Storage(Configuration):
    completion_latency_func : Callable
    base_completion_latency : Duration
    submission_overhead : Duration
    max_iops : int
    cap_inflight : int
    cap_in_progress : int

    def configure_pipeline(self, pipeline):
        pipeline.registry['InflightBucket.latency'] = self.completion_latency_func


@dataclass
class Workload(Configuration):
    consumption_rate_func : Callable
    volume : int
    duration : Duration

    @property
    def runtime(self):
        return self.duration.total if self.duration else None

    def configure_pipeline(self, pipeline):
        pipeline.registry['CompleteBucket.consumption_rate'] = self.consumption_rate_func


@dataclass
class Prefetcher(Configuration):
    prefetch_size_func : Callable
    adjusters : dict
    min_dispatch : int
    initial_completion_target_distance : int
    initial_target_inflight : int

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

        pipeline.inflight_bucket.max_iops = self.storage.max_iops
        pipeline.inflight_bucket.base_completion_latency = self.storage.base_completion_latency.total

        pipeline.cap_inflight = self.storage.cap_inflight
        pipeline.cap_in_progress = self.storage.cap_in_progress
        pipeline.prefetched_bucket.min_dispatch = self.prefetcher.min_dispatch

        if self.prefetcher.initial_completion_target_distance > self.storage.cap_in_progress:
            raise ValueError(f'Value {self.prefetcher.initial_completion_target_distance} for ' f'completion_target_distance exceeds cap_in_progress ' f'value of {self.storage.cap_in_progress}.')

        pipeline.completion_target_distance = self.prefetcher.initial_completion_target_distance


        if self.prefetcher.initial_target_inflight > self.storage.cap_inflight:
            raise ValueError(f'Value {self.prefetcher.initial_target_inflight} for target_inflight ' f'exceeds cap_inflight of {self.storage.cap_inflight}.')

        pipeline.target_inflight = self.prefetcher.initial_target_inflight

        self.storage.configure_pipeline(pipeline)
        self.workload.configure_pipeline(pipeline)
        self.prefetcher.configure_pipeline(pipeline)

        return pipeline
