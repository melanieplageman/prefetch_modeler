from dataclasses import dataclass
import pandas as pd
from prefetch_modeler.core import IO, Tracer, Pipeline
from typing import List
from collections import OrderedDict


class Metric:
    def __init__(self, name=None):
        self._name = name
        self._data = []

    @property
    def name(self):
        return self._name or type(self).__name__

    def run(self, pipeline):
        result = self.function(pipeline)
        self._data.append({'tick': pipeline.tick, self.name: result})

    @property
    def data(self):
        return pd.DataFrame.from_records(self._data, index='tick')

    @staticmethod
    def function(pipeline):
        raise NotImplementedError()


class Simulation:
    def __init__(self, *args):
        self.schema = args
        self.metrics = []

    def run(self, volume, duration=None, traced=None):
        traced = traced or OrderedDict()
        ios = [Tracer(i) if i in traced else IO() for i in range(volume)]

        for i, io in enumerate(ios):
            if i > volume / 1.5:
                io.cached = True

        pipeline = Pipeline(*[bucket_type(
            getattr(bucket_type, 'name', bucket_type.__name__)
        ) for bucket_type in self.schema])

        for metric in self.metrics:
            pipeline.attach_metric(metric)

        timeline = pipeline.run(ios, duration=duration)

        bucket_sequence = [bucket.name for bucket in pipeline.buckets]
        tracer_list = [io for io in ios if isinstance(io, Tracer)]
        if tracer_list:
            tracer_data = pd.concat(tracer.data for tracer in tracer_list) \
                .dropna(axis='index', subset='interval') \
                .pivot(index='io', columns='bucket', values='interval') \
                .reindex(bucket_sequence, axis='columns', fill_value=0)
        else:
            tracer_data = None

        if hasattr(pipeline, 'ratelimiter'):
            inflight_scores = OrderedDict(sorted(pipeline['ratelimiter'].inflight_scores.items()))

        return SimulationResult(timeline, tracer_data, pipeline)

@dataclass(frozen=True)
class SimulationResult:
    timeline: List
    tracer_data: pd.DataFrame
    pipeline: Pipeline
