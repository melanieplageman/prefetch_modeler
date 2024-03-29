from prefetch_modeler.storage_type import fast_local1, slow_cloud1
from prefetch_modeler.workload_type import even_wl, uneven_wl1, uneven_wl2, sinusoid_workload_type
from prefetch_modeler.prefetcher_type import PIPrefetcher, BufferMarkerBucket, \
BufferChecker, BaselineFetchAll, BaselineSync
from prefetch_modeler.core import Rate, Simulation
from prefetch_modeler.ratelimiter_type import RateLimiter
from constant_distance_prefetcher import ConstantDistancePrefetcher, \
TestConstantPrefetcher
from cdvar_prefetcher import CDVariableHeadroom
from plot import ChartGroup, Chart, MetaChartGroup, ChartType
from partially_cached_prefetcher import PartiallyCachedPIPrefetcher
from metric import *
from prefetch_modeler.core.bucket_type import SequenceMarkerBucket, OrderEnforcerBucket
from variable_distance_ratelimiter import VariableDistanceLimitPrefetcher
from change_log_fetcher import ChangeLogFetcher
from hypothesis import HistoryLimitFetcher
from blended import BlendedPrefetcher
from variable_distance_prefetcher import VariableDistancePrefetcher
from control_fetcher import ControlFetcher
from rollback_fetcher import RollbackFetcher
from periodic_fetcher import PeriodicFetcher
from simple_fetcher import SimpleFetcher, ClampFetcher


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
    prefetch_distance = 3

class VariablePrefetcher(VariableDistancePrefetcher):
    pass

class TestPrefetcher(TestConstantPrefetcher):
    og_rate = Rate(per_second=2000)

simulation1 = Simulation(PCP, RateLimiter, *fast_local1, *even_wl)
simulation2 = Simulation(PCPRaw, RateLimiter, *fast_local1, *even_wl)

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

simulation4 = Simulation(BaselineFetchAll, *fast_local1, *uneven_wl1)
simulation5 = Simulation(BaselineSync, *fast_local1, *uneven_wl1)

completed, consumed = uneven_wl1
simulation8 = Simulation(ConstantPrefetcher, SequenceMarkerBucket, BufferChecker, *fast_local1,
                         completed, OrderEnforcerBucket, consumed)

simulation8 = Simulation(VariablePrefetcher, SequenceMarkerBucket, BufferChecker, *fast_local1,
                         completed, OrderEnforcerBucket, consumed)

simulation9 = Simulation(ConstantPrefetcher, SequenceMarkerBucket,
                         BufferChecker, *slow_cloud1, completed,
                         OrderEnforcerBucket, consumed)

simulation11 = Simulation(VariablePrefetcher, SequenceMarkerBucket,
                         BufferChecker, *slow_cloud1, completed,
                         OrderEnforcerBucket, consumed)

simulation12 = Simulation(VariableDistanceLimitPrefetcher, SequenceMarkerBucket,
                         BufferChecker, *slow_cloud1, completed,
                         OrderEnforcerBucket, consumed)

class TestGroup(ChartGroup, metaclass=MetaChartGroup):
    Drain = ChartType(remaining, done)
    Rates = ChartType(prefetch_rate, max_iops, consumption_rate)
    InStorage = ChartType(in_storage)
    AvgTotalLatency = ChartType(avg_total_latency_completed_ios)
    Wait = Wait

simulation10 = Simulation(TestPrefetcher, SequenceMarkerBucket, BufferChecker,
                          *slow_cloud1, completed, OrderEnforcerBucket,
                          consumed)

class ConstantGroup(ChartGroup, metaclass=MetaChartGroup):
    Drain = ChartType(cd_remaining, done)
    Rates = ChartType(max_iops, consumption_rate)
    InStorage = ChartType(in_storage, prefetch_distance, completed_not_consumed)
    AvgTotalLatency = ChartType(avg_total_latency_completed_ios)
    LatencyDT = ChartType(latency_dt)
    InStorageDT = ChartType(in_storage_dt)
    WaitDT = ChartType(wait_dt)
    WaitBenefit = ChartType(wait_benefit)
    WaitBenefitDT = ChartType(wait_benefit_dt)
    LatencyCost = ChartType(latency_cost)
    LatencyCostDT = ChartType(latency_cost_dt)
    Fetched = ChartType(do_cd_fetch)
    WaitIdle = ChartType(wait_time, idle_time)
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
# output = [ConstantGroup(simulation11), ConstantGroup(simulation8)]
# output = [TestGroup(simulation10)]
# output = [ConstantGroup(simulation12)]

# completed, consumed = even_wl
# simulation13 = Simulation(ChangeLogFetcher, SequenceMarkerBucket,
#                          BufferChecker, *fast_local1, completed,
#                          OrderEnforcerBucket, consumed)

# class ChangeLogGroup(ChartGroup, metaclass=MetaChartGroup):
#     Drain = ChartType(cd_remaining, done)
#     Rates = ChartType(max_iops, consumption_rate)
#     InStorage = ChartType(in_storage, prefetch_distance, completed_not_consumed)
#     AvgTotalLatency = ChartType(avg_total_latency_completed_ios)
#     Fetched = ChartType(do_cd_fetch)
#     WaitIdle = ChartType(wait_time, idle_time)
#     Wait = Wait

# output = [ChangeLogGroup(simulation13)]

# simulation4 = Simulation(BaselineFetchAll, SequenceMarkerBucket, *slow_cloud1,
#         BufferChecker, completed, OrderEnforcerBucket, consumed)

# simulation5 = Simulation(BaselineSync, SequenceMarkerBucket, *slow_cloud1,
#         BufferChecker, completed, OrderEnforcerBucket, consumed)

# class BaseGroup2(ChartGroup, metaclass=MetaChartGroup):
#     Drain = ChartType(remaining, done)
#     Rates = ChartType(max_iops, consumption_rate)
#     InStorage = ChartType(in_storage)
#     AvgTotalLatency = ChartType(avg_total_latency_completed_ios)
#     Wait = Wait
#     WaitIdle = ChartType(wait_time, idle_time)

class SoloLimitFetcher(HistoryLimitFetcher):
    name = 'cd_fetcher'

completed, consumed = uneven_wl1
# completed, consumed = even_wl
# completed, consumed = sinusoid_workload_type("Sinusoid Workload", 0.003, 0.2, 10000)
current_storage = slow_cloud1
# current_storage = fast_local1

simulation_blend = Simulation(BlendedPrefetcher, SequenceMarkerBucket,
                         BufferChecker, *current_storage, completed,
                         OrderEnforcerBucket, consumed)

simulation_history_limiter = Simulation(SoloLimitFetcher, SequenceMarkerBucket,
                         BufferChecker, *current_storage, completed,
                         OrderEnforcerBucket, consumed)

simulation_control = Simulation(ControlFetcher, SequenceMarkerBucket,
                         BufferChecker, *current_storage, completed,
                         OrderEnforcerBucket, consumed)

simulation_rollback = Simulation(RollbackFetcher, SequenceMarkerBucket,
                         BufferChecker, *current_storage, completed,
                         OrderEnforcerBucket, consumed)

simulation_variable = Simulation(VariableDistancePrefetcher, SequenceMarkerBucket,
                         BufferChecker, *current_storage, completed,
                         OrderEnforcerBucket, consumed)

simulation_simple = Simulation(SimpleFetcher, SequenceMarkerBucket,
                         BufferChecker, *current_storage, completed,
                         OrderEnforcerBucket, consumed)

simulation_clamped = Simulation(ClampFetcher, SequenceMarkerBucket,
                         BufferChecker, *current_storage, completed,
                         OrderEnforcerBucket, consumed)

class SingleFetcherGroup(ChartGroup, metaclass=MetaChartGroup):
    Drain = ChartType(cd_remaining, done)
    Rates = ChartType(max_iops, consumption_rate)
    # CompletionRate = ChartType(completion_rate)
    AvgTotalLatency = ChartType(avg_total_latency_completed_ios)
    Wait = Wait
    WaitIdle = ChartType(wait_time, idle_time)
    InStorage = ChartType(in_storage, prefetch_distance, completed_not_consumed)

class SingleFetcherGroup2(ChartGroup, metaclass=MetaChartGroup):
    Drain = ChartType(cd_remaining, done)
    Rates = ChartType(max_iops, consumption_rate)
    CompletionRate = ChartType(completion_rate)
    AvgTotalLatency = ChartType(avg_total_latency_completed_ios)
    Wait = Wait
    WaitIdle = ChartType(wait_time, idle_time)
    InStorage = ChartType(in_storage, prefetch_distance, completed_not_consumed)
    Stats = ChartType(latency)
    Throughput = ChartType(through)
    # InStorage = ChartType(in_storage, completed_not_consumed)
    # PrefetchDistanceThroughput = ChartType(prefetch_throughput)
    # ThroughputWithCached = ChartType(throughput_w_cached)

output = [
        # SingleFetcherGroup2(simulation_simple),
        SingleFetcherGroup2(simulation_clamped),
        # SingleFetcherGroup2(simulation_periodic),
        ]
