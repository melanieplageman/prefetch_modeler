import os
import sys

import matplotlib.pyplot as plt

from prefetch_modeler.storage_type import fast_local1, slow_cloud1, empty_storage
from prefetch_modeler.workload_type import even_wl, uneven_wl
from prefetch_modeler.prefetcher_type import piprefetchera
from prefetch_modeler.cohort import Member
from prefetch_modeler.plot import dump_plots



member1 = Member(slow_cloud1, uneven_wl, piprefetchera)
member1.run()
dump_plots(member1, storage_name='slow_cloud1', workload_name='uneven_wl')
