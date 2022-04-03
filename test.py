import os
import sys

import matplotlib.pyplot as plt
import pandas as pd

from prefetch_modeler.storage_type import fast_local1, slow_cloud1
from prefetch_modeler.workload_type import even_wl, uneven_wl
from prefetch_modeler.prefetcher_type import prefetcher_list
from prefetch_modeler.cohort import Member, Cohort


cohort1 = Cohort()
cohort1.run(slow_cloud1, even_wl, prefetcher_list)
# cohort2 = Cohort().run(fast_local1, workload1, prefetcher_list)
cohort1.dump_plots(storage_name='slow_cloud1', workload_name='even_wl')
