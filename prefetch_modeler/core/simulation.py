from prefetch_modeler.core import IO, Tracer, Pipeline
import pandas as pd


class Simulation:
    def __init__(self, *args):
        self.schema = args

    def run(self, volume, duration=None, traced=None):
        traced = traced or frozenset()
        ios = [Tracer(i) if i in traced else IO() for i in range(volume)]

        pipeline = Pipeline(*[bucket_type(bucket_type.__name__) for bucket_type in self.schema])

        bucket_data = pipeline.run(ios, duration=duration)

        bucket_sequence = [bucket.name for bucket in pipeline.buckets]
        tracer_list = [io for io in ios if isinstance(io, Tracer)]
        tracer_data = pd.concat(tracer.data for tracer in tracer_list) \
            .dropna(axis='index', subset='interval') \
            .pivot(index='io', columns='bucket', values='interval') \
            .reindex(bucket_sequence, axis='columns', fill_value=0)

        return SimulationResult(bucket_data, tracer_data)


@dataclass(frozen=True)
class SimulationResult:
    bucket_data: pd.DataFrame
    tracer_data: pd.DataFrame