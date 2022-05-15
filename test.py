from prefetch_modeler.storage_type import fast_local1, slow_cloud1, empty_storage
from prefetch_modeler.workload_type import even_wl, uneven_wl
from prefetch_modeler.prefetcher_type import piprefetchera
from prefetch_modeler.plot import dump_plots
from metric import default_metrics
from prefetch_modeler.core import Duration, Rate, Simulation


simulation = Simulation(*slow_cloud1, *uneven_wl, *piprefetchera)
default_metrics(simulation)
result = simulation.run(1000, duration=Duration(seconds=0.2), traced=[1, 5, 100])

dump_plots(simulation, result, storage_name='slow_cloud1',
           workload_name='uneven_wl', prefetcher_name='piprefetcher1')
