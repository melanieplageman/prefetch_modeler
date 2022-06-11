from prefetch_modeler.storage_type import fast_local1, slow_cloud1
from prefetch_modeler.workload_type import even_wl, uneven_wl1, uneven_wl2
from prefetch_modeler.prefetcher_type import PIPrefetcher
from prefetch_modeler.core import Duration, Rate, Simulation
from prefetch_modeler.ratelimiter_type import RateLimiter
from plot import ChartGroup, Chart
from metric import *


class LocalPrefetcher1(PIPrefetcher):
    og_rate = Rate(per_second=1000)
    raw_lookback = 10
    avg_lookback = 4
    kp = 0.23
    ki_cnc = -Rate(per_second=20).value
    cnc_headroom = 2

io_args = ['IO',
           completed_not_consumed,
           # awaiting_dispatch,
           ]
rate_args = ['Rate',
             prefetch_rate,
             max_iops,
             consumption_rate,
             demand_rate,
             storage_rate,
             ]
integral_args = ['Integral',
          cnc_integral_term, cnc_integral_term_w_coefficient,
                 ]
proportional_args = ['Proportional', proportional_term,
                     proportional_term_w_coefficient]

wait_args = ['Wait', wait_consume]
wait_kwargs = {'plot_type':'area', 'stacked':False}

duration_seconds = 0.8

simulation1 = Simulation(LocalPrefetcher1, RateLimiter, *slow_cloud1, *even_wl)

group1 = ChartGroup(
    simulation1,
    Chart('Drain', remaining, done),

    # Chart('Overview', completed_not_consumed, in_storage),
    # Chart(*wait_args, **wait_kwargs),

    # Chart('Rates', prefetch_rate, max_iops, demand_rate,
    #       consumption_rate, prefetch_rate_limit),

    Chart('Rates', prefetch_rate, max_iops, demand_rate, consumption_rate,
          ),

    # Chart('Capacity', capacity),

    # Chart('CompletionInflight', completion_inflight_ratio),

    # Chart('Completion', raw_completion_rate),
    Chart('InStorage', in_storage, capacity),
    # Chart('InStorageRate', in_storage_rate),

    # Chart('Rates', prefetch_rate, max_iops,
    #       demand_rate,
    #       consumption_rate),

    # Chart(*integral_args),

    # Chart(*proportional_args),

    Chart('Latency', io_latency),

    # Chart('Integral', lat_integral),
    # Chart('Rates', prefetch_rate, demand_rate, consumption_rate, max_iops),
    # Chart('InStorage', in_storage),
    # Chart('Gain', gain),
)

result1 = simulation1.run(1000, duration=Duration(seconds=duration_seconds), traced=[1, 5, 100])


# simulation2 = Simulation(FasterPrefetcher, ConstantFetcher, *slow_cloud1, *even_wl)

# group2 = ChartGroup(
#     simulation2,
#     Chart('Latency', io_latency),
#     Chart('InStorage', in_storage),
#     Chart('Ratio', io_ratio),
#     Chart(*rate_args),
#     Chart('StorageRateChange', storage_rate_change),
#     Chart('LatencyChange', latency_change),
#     Chart('StorageRatio', storage_latency_ratio),
#     Chart('StorageRatio2', storage_latency_ratio2),
# )


# result2 = simulation2.run(1000, duration=Duration(seconds=duration_seconds), traced=[1, 5, 100])


# simulation3 = Simulation(SlowerPrefetcher, ConstantFetcher, *slow_cloud1, *even_wl)

# group3 = ChartGroup(
#     simulation3,
#     Chart('Latency', io_latency),
#     Chart('InStorage', in_storage),
#     Chart('Ratio', io_ratio),
#     Chart(*rate_args),
#     Chart('StorageRateChange', storage_rate_change),
#     Chart('LatencyChange', latency_change),
#     Chart('StorageRatio', storage_latency_ratio),
#     Chart('StorageRatio2', storage_latency_ratio2),
# )

# result3 = simulation3.run(1000, duration=Duration(seconds=duration_seconds), traced=[1, 5, 100])


# import matplotlib.pyplot as plt
# result1.pipeline['newfetcher'].diffs[2] = -20
# xs = result1.pipeline['newfetcher'].diffs.keys()
# ys = result1.pipeline['newfetcher'].diffs.values()
# plt.plot(xs, ys)
# plt.show()
ChartGroup.show(result1.timeline, group1)
# ChartGroup.show(result3.timeline, group3)
