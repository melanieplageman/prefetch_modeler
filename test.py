from prefetch_modeler.storage_type import fast_local1, slow_cloud1
from prefetch_modeler.workload_type import even_wl, uneven_wl1, uneven_wl2
from prefetch_modeler.prefetcher_type import PIPrefetcher, BufferMarkerBucket, \
BufferChecker, BaselineFetchAll
from prefetch_modeler.core import Rate, Simulation
from prefetch_modeler.ratelimiter_type import RateLimiter
from constant_distance_prefetcher import ConstantDistancePrefetcher, \
CachedBaselineFetchAll, CachedBaselineSync
from cdvar_prefetcher import CDVariableHeadroom
from plot import ChartGroup, Chart, MetaChartGroup, ChartType
from partially_cached_prefetcher import PartiallyCachedPIPrefetcher
from metric import *


class LocalPrefetcher1(PIPrefetcher):
    og_rate = Rate(per_second=1000)
    raw_lookback = 10
    avg_lookback = 4
    kp = 0.23
    ki_cnc = -Rate(per_second=20).value
    cnc_headroom = 2

class CachePrefetcher(PartiallyCachedPIPrefetcher):
    og_rate = Rate(per_second=1000)
    raw_lookback = 10
    avg_lookback = 4
    kp = 0.23
    ki_cnc = -Rate(per_second=20).value
    cnc_headroom = 10

class CDPrefetcher(CDVariableHeadroom):
    cnc_headroom = 10
    raw_lookback = 10

class PCPRaw(PartiallyCachedPIPrefetcher):
    raw_lookback = 10
    cnc_headroom = 2
    avg_lookback = 10
    use_raw = True

class PCP(PartiallyCachedPIPrefetcher):
    raw_lookback = 10
    cnc_headroom = 2
    avg_lookback = 10
    use_raw = False

class ConstantPrefetcher(ConstantDistancePrefetcher):
    cnc_headroom = 2

simulation1 = Simulation(PCP, RateLimiter, *fast_local1, *even_wl)
simulation2 = Simulation(PCPRaw, RateLimiter, *fast_local1, *even_wl)

simulation6 = Simulation(CachedBaselineFetchAll, *fast_local1, *even_wl)

simulation7 = Simulation(CDPrefetcher, *fast_local1, *even_wl)

Wait = ChartType(wait_consume, plot_type='area', stacked=False)

class RateGroup(ChartGroup, metaclass=MetaChartGroup):
    Drain = ChartType(remaining, done)
    Rates = ChartType(prefetch_rate, max_iops, demand_rate, consumption_rate)
    InStorage = ChartType(in_storage, capacity)
    Latency = ChartType(io_latency)
    Fetched = ChartType(do_prefetch)
    Wait = Wait


simulation3 = Simulation(ConstantDistancePrefetcher, *fast_local1, *uneven_wl1)

simulation4 = Simulation(CachedBaselineFetchAll, *fast_local1, *uneven_wl1)
simulation5 = Simulation(CachedBaselineSync, *fast_local1, *uneven_wl1)

simulation8 = Simulation(ConstantPrefetcher, *fast_local1, *uneven_wl1)

class ConstantGroup(ChartGroup, metaclass=MetaChartGroup):
    Drain = ChartType(cd_remaining, done)
    Rates = ChartType(max_iops, consumption_rate)
    InStorage = ChartType(in_storage, constant_cnc_headroom, completed_not_consumed)
    Fetched = ChartType(do_cd_fetch)
    Wait = Wait

class BaseGroup(ChartGroup, metaclass=MetaChartGroup):
    Drain = ChartType(remaining, done)
    Rates = ChartType(max_iops, consumption_rate)
    InStorage = ChartType(in_storage)
    Wait = Wait


# output = [ConstantGroup(simulation3), BaseGroup(simulation4)]
# output = [RateGroup(simulation1), RateGroup(simulation2)]
# output = [RateGroup(simulation1), BaseGroup(simulation6), ConstantGroup(simulation7)]

# output = [RateGroup(simulation2), ConstantGroup(simulation7)]

# output = [ConstantGroup(simulation8), BaseGroup(simulation6)]
output = [ConstantGroup(simulation8), ]
