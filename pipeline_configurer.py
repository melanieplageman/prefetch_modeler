from dataclasses import dataclass
from typing import Callable
from model import TestPipeline


@dataclass(frozen=True)
class PipelineConfiguration:
    inflight_cap: int
    submission_overhead: int
    max_iops: int
    base_completion_latency: int
    consumption_rate: int
    consumption_size: int
    prefetch_distance_algorithm: Callable
    completed_cap: int
    completion_target_distance: int
    min_dispatch: int
    max_inflight: int

    def generate_pipeline(self, *args, **kwargs):
        pipeline = TestPipeline(*args, **kwargs)

        pipeline.prefetched_bucket.inflight_cap = self.inflight_cap

        pipeline.submitted_bucket.LATENCY = self.submission_overhead

        pipeline.inflight_bucket.MAX_IOPS = self.max_iops
        pipeline.inflight_bucket.BASE_COMPLETION_LATENCY = self.base_completion_latency

        pipeline.completed_bucket.CONSUMPTION_RATE = self.consumption_rate
        pipeline.completed_bucket.CONSUMPTION_SIZE = self.consumption_size

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
