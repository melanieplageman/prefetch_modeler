from prefetch_modeler.storage_type import fast_local1, slow_cloud1
from prefetch_modeler.workload_type import even_wl, uneven_wl
from prefetch_modeler.prefetcher_type import PIPrefetcher
from prefetch_modeler.core import Duration, Rate, Simulation
from plot import ChartGroup, Chart
from metric import *


class PIPrefetcher1(PIPrefetcher):
    og_rate = Rate(per_second=500)
    raw_lookback = 4
    avg_lookback = 3
    awd_lookback = 3
    kp = 0.5
    kh = 0.4
    ki_awd = -Rate(per_second=20).value
    ki_cnc = -Rate(per_second=20).value
    cnc_headroom = 50
    min_cnc_headroom = 15

class PF_headroom_3(PIPrefetcher1):
    min_cnc_headroom = 3

class PF_headroom_15(PIPrefetcher1):
    min_cnc_headroom = 15

io_args = ['IO', completed_not_consumed, awaiting_dispatch]
rate_args = ['Rate', prefetch_rate, max_iops, consumption_rate, demand_rate]
integral_args = ['Integral',
          cnc_integral_term, cnc_integral_term_w_coefficient,
          awd_integral_term, awd_integral_term_w_coefficient]
proportional_args = ['Proportional', proportional_term,
                     proportional_term_w_coefficient]

wait_args = ['Wait', wait_consume]
wait_kwargs = {'plot_type':'area', 'stacked':False}

simulation1 = Simulation(PF_headroom_3, *slow_cloud1, *uneven_wl)

group1 = ChartGroup(
    simulation1,
    Chart(*io_args),
    Chart(*wait_args, **wait_kwargs),
    Chart(*proportional_args),
    Chart(*integral_args),
    Chart(*rate_args),
)

result1 = simulation1.run(1000, duration=Duration(seconds=0.2), traced=[1, 5, 100])


# simulation2 = Simulation(PF_headroom_3, *fast_local1, *uneven_wl)

# group2 = ChartGroup(
#     simulation2,
    # Chart(*io_args),
    # Chart(*wait_args, **wait_kwargs),
    # Chart(*proportional_args),
    # Chart(*integral_args),
    # Chart(*rate_args),
# )

# result2 = simulation2.run(1000, duration=Duration(seconds=0.2), traced=[1, 5, 100])


simulation3 = Simulation(PF_headroom_15, *slow_cloud1, *uneven_wl)

group3 = ChartGroup(
    simulation3,
    Chart(*io_args),
    Chart(*wait_args, **wait_kwargs),
    Chart(*proportional_args),
    Chart(*integral_args),
    Chart(*rate_args),
)

result3 = simulation3.run(1000, duration=Duration(seconds=0.2), traced=[1, 5, 100])


ChartGroup.show(result1.timeline, group1, group3)
