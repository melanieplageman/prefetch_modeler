from dataclasses import dataclass
from typing import Callable
from fractions import Fraction

from prefetch_modeler.model import TestPipeline
from prefetch_modeler.core import IO, Duration, Tracer
from prefetch_modeler.core.units import BaseRate
import json


class Configuration:
    def __str__(self):
        return '\n'.join(f'{k}: {v.__name__ if callable(v) else v}' for k, v in self.__dict__.items())

    def dictified(self):
        def dictify(mydict):
            newdict = {}
            for k, v in mydict.items():
                if isinstance(v, dict):
                    newdict[k] = dictify(v)
                elif isinstance(v, Duration):
                    newdict[k] = v.total
                elif isinstance(v, BaseRate):
                    newdict[k] = v.value
                elif callable(v):
                    newdict[k] = v.__name__
                else:
                    newdict[k] = v
            return newdict

        return dictify(self.__dict__)

@dataclass
class Storage(Configuration):
    name : str
    completion_latency_func : Callable
    kernel_invoke_batch_size: int
    submission_overhead_func: Callable
    max_iops : Fraction

    def configure_pipeline(self, pipeline):
        pipeline.override('inflight.latency', self.completion_latency_func)
        pipeline.override('kernel_batch.latency', self.submission_overhead_func)


        def threshold(bucket, original):
            return self.kernel_invoke_batch_size

        pipeline.override('w_claimed_buffer.threshold', threshold)

        def calculate_target_capacity(bucket, original):
            capacity = self.completion_latency_func(bucket, original) * self.max_iops
            capacity = int(capacity)
            if capacity < 1:
                raise ValueError("Capacity can't be less than 1")
            return capacity
        pipeline.override('submitted.target_capacity', calculate_target_capacity)


@dataclass
class Workload(Configuration):
    id: int
    consumption_rate_func : Callable
    volume : int
    duration : Duration
    trace_ios : list

    def __post_init__(self):
        self._ios = None
        self.duration = self.duration.total if self.duration else None

    def configure_pipeline(self, pipeline):
        pipeline.override('completed.rate', self.consumption_rate_func)

    @property
    def ios(self):
        if self._ios is None:
            self._ios = [Tracer(i) if i in self.trace_ios else IO() for i in range(self.volume)]
        return self._ios

    @property
    def tracers(self):
        return (self.ios[i] for i in self.trace_ios)

    def reset(self):
        self._ios = None


@dataclass
class Prefetcher(Configuration):
    id: int
    prefetch_num_ios_func : Callable
    adjust_func : Callable
    min_dispatch : int
    initial_completion_target_distance : int
    cap_in_progress : int

    def configure_pipeline(self, pipeline):
        if self.initial_completion_target_distance > self.cap_in_progress:
            raise ValueError(f'Value {self.initial_completion_target_distance} for ' f'completion_target_distance exceeds cap_in_progress ' f'value of {self.cap_in_progress}.')

        pipeline.cap_in_progress = self.cap_in_progress
        pipeline.completion_target_distance = self.initial_completion_target_distance
        pipeline.min_dispatch = self.min_dispatch

        pipeline.override('remaining.wanted_move_size', self.prefetch_num_ios_func)

        def min_dispatch(bucket, original):
            return self.min_dispatch

        pipeline.override('remaining.min_dispatch', min_dispatch)

        if self.adjust_func is not None:
            pipeline.override('remaining.adjust', self.adjust_func)


class PipelineConfiguration:
    def __init__(self, storage, workload, prefetcher):
        self.storage = storage
        self.workload = workload
        self.prefetcher = prefetcher

    def __str__(self):
        return '\n\n'.join(f'{k}:\n{str(v)}' for k, v in self.__dict__.items())

    # TODO: work on figuring out how this will integrate with display
    def to_dict(self):
        # Loop through each configuration object (e.g. storage), dictifying it
        return {k: v.dictified() for k, v in self.__dict__.items()}

    def generate_pipeline(self, *args, **kwargs):
        pipeline = TestPipeline()

        self.storage.configure_pipeline(pipeline)
        self.workload.configure_pipeline(pipeline)
        self.prefetcher.configure_pipeline(pipeline)

        return pipeline
