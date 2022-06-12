from prefetch_modeler.storage_type import fast_local1, slow_cloud1
from prefetch_modeler.workload_type import even_wl, uneven_wl1, uneven_wl2
from prefetch_modeler.prefetcher_type import PIPrefetcher
from prefetch_modeler.core import Rate, Simulation
from prefetch_modeler.ratelimiter_type import RateLimiter
from plot import ChartGroup, Chart, MetaChartGroup
from metric import *


class LocalPrefetcher1(PIPrefetcher):
    og_rate = Rate(per_second=1000)
    raw_lookback = 10
    avg_lookback = 4
    kp = 0.23
    ki_cnc = -Rate(per_second=20).value
    cnc_headroom = 2

simulation1 = Simulation(LocalPrefetcher1, RateLimiter, *slow_cloud1, *even_wl)
simulation2 = Simulation(LocalPrefetcher1, RateLimiter, *slow_cloud1, *uneven_wl1)

def ChartType(*args, plot_type='line', **kwargs):
    class chart_type(Chart):
        _plot_type = plot_type
        _kwargs = kwargs
        _metric_schema = {metric_type.__name__ : metric_type for metric_type in args}

    return chart_type


Wait = ChartType(wait_consume, plot_type='area', stacked=False)

class Group1(ChartGroup, metaclass=MetaChartGroup):
    Drain = ChartType(remaining, done)
    Rates = ChartType(prefetch_rate, max_iops, demand_rate, consumption_rate)
    InStorage = ChartType(in_storage, capacity)
    Latency = ChartType(io_latency)
    Wait = Wait

output = [Group1(simulation1), Group1(simulation2)]
