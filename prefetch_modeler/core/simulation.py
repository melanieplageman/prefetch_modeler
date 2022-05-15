from dataclasses import dataclass
import pandas as pd
from prefetch_modeler.core import IO, Tracer, Pipeline


class Metric:
    def __init__(self, function):
        self.data = []
        self.function = function

    def run(self, pipeline):
        self.data.append(self.function(pipeline))


class Simulation:
    def __init__(self, *args):
        self.schema = args
        self.metric_schema = {}

    def run(self, volume, duration=None, traced=None):
        traced = traced or OrderedDict()
        ios = [Tracer(i) if i in traced else IO() for i in range(volume)]

        pipeline = Pipeline(*[bucket_type(
            getattr(bucket_type, 'name', bucket_type.__name__)
        ) for bucket_type in self.schema], **self.metric_schema)

        bucket_data, metric_index = pipeline.run(ios, duration=duration)

        metric_data = pd.DataFrame({
            name: metric.data for name, metric in self.metric_schema.items()
        }, index=metric_index).rename_axis('tick')

        bucket_sequence = [bucket.name for bucket in pipeline.buckets]
        tracer_list = [io for io in ios if isinstance(io, Tracer)]
        tracer_data = pd.concat(tracer.data for tracer in tracer_list) \
            .dropna(axis='index', subset='interval') \
            .pivot(index='io', columns='bucket', values='interval') \
            .reindex(bucket_sequence, axis='columns', fill_value=0)

        return SimulationResult(bucket_data, tracer_data, metric_data)

    def metric(self, name):
        def decorator(function):
            metric = Metric(function)
            self.metric_schema[name] = metric
            return function
        return decorator



@dataclass(frozen=True)
class SimulationResult:
    bucket_data: pd.DataFrame
    tracer_data: pd.DataFrame
    metric_data: pd.DataFrame
