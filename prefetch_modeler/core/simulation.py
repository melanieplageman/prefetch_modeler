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

        pipeline = Pipeline(*[bucket_type(
            getattr(bucket_type, 'name', bucket_type.__name__)
        ) for bucket_type in self.schema])

        for metric in self.metrics:
            pipeline.attach_metric(metric)

        timeline = pipeline.run(ios, duration=duration)

        bucket_sequence = [bucket.name for bucket in pipeline.buckets]
        tracer_list = [io for io in ios if isinstance(io, Tracer)]
        tracer_data = pd.concat(tracer.data for tracer in tracer_list) \
            .dropna(axis='index', subset='interval') \
            .pivot(index='io', columns='bucket', values='interval') \
            .reindex(bucket_sequence, axis='columns', fill_value=0)

        inflight_scores = OrderedDict(sorted(pipeline['newfetcher'].inflight_scores.items()))

        max_inflight = 0
        seen_lo = False
        for inflight, scores in inflight_scores.items():
            print(f'In Storage: {inflight}. HI: {scores[0]} LO: {scores[1]}')
        for inflight, scores in inflight_scores.items():
            his = scores[0]
            los = scores[1]
            if los > 0 and not seen_lo:
                seen_lo = True
                continue
            if seen_lo and his > los:
                max_inflight = inflight
                break

        print(max_inflight)
        return SimulationResult(timeline, tracer_data)

@dataclass(frozen=True)
class SimulationResult:
    timeline: List
    tracer_data: pd.DataFrame
