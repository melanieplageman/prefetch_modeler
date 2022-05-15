from prefetch_modeler.storage_type import fast_local1, slow_cloud1
from prefetch_modeler.core import Duration, Rate, Simulation
from prefetch_modeler.plot import io_data
import matplotlib.pyplot as plt

class Member:
    def __init__(self, storage, workload, prefetcher):
        self.data = None
        self.tracer_data = None
        self._io_view = None
        self.storage = storage
        self.workload = workload
        self.prefetcher = prefetcher
        self.schema = None

    def run(self):
        simulation = Simulation(*self.prefetcher, *self.storage, *self.workload)

        @simulation.metric('proportional_term')
        def metric(self):
            return float(getattr(self['remaining'], 'proportional_term', 0))

        @simulation.metric('proportional_term_w_coefficient')
        def metric(self):
            term = float(getattr(self['remaining'], 'proportional_term', 0))
            gain = float(getattr(self['remaining'], 'kp', 0))
            return term * gain

        @simulation.metric('cnc_integral_term')
        def metric(self):
            return float(getattr(self['remaining'], 'cnc_integral_term', 0))

        @simulation.metric('cnc_integral_term_w_coefficient')
        def metric(self):
            term = float(getattr(self['remaining'], 'cnc_integral_term', 0))
            gain = float(getattr(self['remaining'], 'ki_cnc', 0))
            return term * gain

        @simulation.metric('awd_integral_term')
        def metric(self):
            return float(getattr(self['remaining'], 'awd_integral_term', 0))

        @simulation.metric('awd_integral_term_w_coefficient')
        def metric(self):
            term = float(getattr(self['remaining'], 'awd_integral_term', 0))
            gain = float(getattr(self['remaining'], 'ki_awd', 0))
            return term * gain

        @simulation.metric('demand_rate')
        def metric(self):
            return float(getattr(self['remaining'], 'demand_rate', 0))

        @simulation.metric('awaiting_dispatch')
        def metric(self):
            return self['remaining'].awaiting_dispatch

        @simulation.metric('cnc_headroom')
        def metric(self):
            return getattr(self['remaining'], 'cnc_headroom', 0)

        @simulation.metric('wait_consume')
        def metric(self):
            completed = self['completed']
            return completed.tick_data['want_to_move'] > completed.tick_data['to_move']

        @simulation.metric('remaining_ios')
        def metric(self):
            return len(self['remaining'])

        @simulation.metric('max_iops')
        def metric(self):
            return float(self['submitted'].storage_speed)

        # @simulation.metric('consumption_rate')
        # def metric(self):
        #     return float(self['completed'].rate())

        # @simulation.metric('prefetch_rate')
        # def metric(self):
        #     return float(self['remaining'].rate())

        self.schema = simulation.schema

        result = simulation.run(1000, duration=Duration(seconds=0.2), traced=[1, 5, 100])
        data = result.bucket_data
        self.data = data.reindex(data.index.union(data.index[1:] - 1), method='ffill')
        self.tracer_data = result.tracer_data
        self.metric_data = result.metric_data

    @property
    def io_view(self):
        if self._io_view is None:
            self._io_view = io_data(self.data)
        return self._io_view

    @property
    def tracer_view(self):
        return self.tracer_data
