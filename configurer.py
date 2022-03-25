from dataclasses import dataclass
from typing import Callable
from model import TestPipeline
from units import Duration

class Configuration:
    def __str__(self):
        return '\n'.join(f'{k}: {v.__name__ if callable(v) else v}' for k, v in self.__dict__.items())

@dataclass
class Storage(Configuration):
    completion_latency_func : Callable
    base_completion_latency : Duration
    submission_overhead : Duration
    max_iops : int
    cap_inflight : int
    cap_in_progress : int

    def configure_pipeline(self, pipeline):
        pipeline.override('inflight.latency', self.completion_latency_func)


@dataclass
class Workload(Configuration):
    consumption_rate_func : Callable
    volume : int
    duration : Duration

    # TODO: make Duration a timedelta
    def __post_init__(self):
        self.duration = self.duration.total if self.duration else None

    @property
    def tick_duration(self):
        return self.duration.total if self.duration else None

    def configure_pipeline(self, pipeline):
        pipeline.override('completed.consumption_rate', self.consumption_rate_func)


@dataclass
class Prefetcher(Configuration):
    adjusters : dict
    min_dispatch : int
    initial_completion_target_distance : int
    initial_target_inflight : int

    def configure_pipeline(self, pipeline):
        for k, v in self.adjusters.items():
            pipeline.override(k, v)


class PipelineConfiguration:
    def __init__(self, storage, workload, prefetcher):
        self.storage = storage
        self.workload = workload
        self.prefetcher = prefetcher

    def __str__(self):
        return '\n\n'.join(f'{k}:\n{str(v)}' for k, v in self.__dict__.items())

    def generate_pipeline(self, *args, **kwargs):
        pipeline = TestPipeline()

        pipeline.inflight_bucket.max_iops = self.storage.max_iops

        if self.storage.cap_inflight > self.storage.max_iops:
            raise ValueError(f'cap_inflight cannot exceed max_iops')
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
