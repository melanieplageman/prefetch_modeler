from prefetch_modeler.storage_type import fast_local1, slow_cloud1
from prefetch_modeler.workload_type import even_wl, uneven_wl
from prefetch_modeler.prefetcher_type import piprefetchera
from prefetch_modeler.core import Duration, Rate, Simulation
from plot import ChartGroup, Chart
from metric import *


simulation = Simulation(*piprefetchera, *slow_cloud1, *uneven_wl)

group = ChartGroup(
    simulation,
    Chart('IO', completed_not_consumed, awaiting_dispatch),
    Chart('Wait', wait_consume, plot_type='area', stacked=False),
    Chart('Proportional', proportional_term, proportional_term_w_coefficient),
    Chart('Integral',
          cnc_integral_term, cnc_integral_term_w_coefficient,
          awd_integral_term, awd_integral_term_w_coefficient),
    Chart('Rate', prefetch_rate, max_iops, consumption_rate, demand_rate),
)

result = simulation.run(1000, duration=Duration(seconds=0.2), traced=[1, 5, 100])


simulation2 = Simulation(*piprefetchera, *fast_local1, *uneven_wl)

group2 = ChartGroup(
    simulation2,
    Chart('IO', completed_not_consumed, awaiting_dispatch),
    Chart('Wait', wait_consume, plot_type='area', stacked=False),
    Chart('Proportional', proportional_term, proportional_term_w_coefficient),
    Chart('Integral',
          cnc_integral_term, cnc_integral_term_w_coefficient,
          awd_integral_term, awd_integral_term_w_coefficient),
    Chart('Rate', prefetch_rate, max_iops, consumption_rate, demand_rate),
)

result2 = simulation2.run(1000, duration=Duration(seconds=0.2), traced=[1, 5, 100])


ChartGroup.show(result.timeline, group, group2)
