import os
import sys

import matplotlib.pyplot as plt
import pandas as pd

from prefetch_modeler.storage_type import fast_local1, slow_cloud1, empty_storage
from prefetch_modeler.workload_type import even_wl, uneven_wl
from prefetch_modeler.prefetcher_type import prefetcher_list
from prefetch_modeler.cohort import Member, Cohort


cohort1 = Cohort()

# cohort1.run(slow_cloud1, even_wl, prefetcher_list)
# cohort1.dump_plots(storage_name='slow_cloud1', workload_name='even_wl')

# cohort1.run(fast_local1, even_wl, prefetcher_list)
# cohort1.dump_plots(storage_name='fast_local1', workload_name='even_wl')

cohort1.run(slow_cloud1, uneven_wl, prefetcher_list)
cohort1.dump_plots(storage_name='slow_cloud1', workload_name='uneven_wl')

# cohort1.run(fast_local1, uneven_wl, prefetcher_list)
# cohort1.dump_plots(storage_name='fast_local1', workload_name='uneven_wl')

# cohort1.run(empty_storage, uneven_wl, prefetcher_list)
# cohort1.dump_plots(storage_name='empty_storage', workload_name='uneven_wl')
