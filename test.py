import os
import sys

import matplotlib.pyplot as plt
import pandas as pd

from prefetch_modeler.core import Duration, Rate, Simulation
from prefetch_modeler.storage_type import fast_local1, slow_cloud1
from prefetch_modeler.workload_type import workload1, workload2
from prefetch_modeler.prefetcher_type import prefetcher_list
from prefetch_modeler.plot import io_title, plot_io_data, plot_wait_data


def get_limits(results):
    xlim, xwaitlim, xtracelim = 0, 0, 0
    for result in results:
        max_x = max(result['view'].index)
        max_wait_x = max(result['wait_view'].index)
        # max_trace_x = max(result['tracer_view'].index)
        max_trace_x = 0

        xlim = max_x if max_x > xlim else xlim
        xwaitlim = max_wait_x if max_wait_x > xwaitlim else xwaitlim
        xtracelim = max_trace_x if max_trace_x > xtracelim else xtracelim

    return {'xlim': xlim, 'xwaitlim': xwaitlim, 'xtracelim': xtracelim}

all_results = []
for storage in [slow_cloud1, fast_local1]:
    for workload in [workload1, workload2]:
        wl_storage_results = []
        for prefetcher in prefetcher_list:
            simulation = Simulation(*prefetcher, *storage, *workload)

            ### Run
            result = simulation.run(200, duration=Duration(seconds=10), traced=[1, 5, 100])

            data = result.bucket_data
            data = data.reindex(data.index.union(data.index[1:] - 1), method='ffill')

            ### Plot

            # IO data
            view = plot_io_data(data)
            # print(view)

            # Wait data
            wait_view = plot_wait_data(data)
            # print(wait_view)

            wl_storage_results.append({
                'pipeline_config': None,
                'view': view,
                'wait_view': wait_view,
                'tracer_view': result.tracer_data,
            })
        limits = get_limits(wl_storage_results)
        all_results.append({
            'wl_storage_results': wl_storage_results,
            'limits': limits
        })


def plot_views(pipeline_config, limits, view, wait_view, tracer_view):
    figure, axes = plt.subplots(2)
    axes[0].set_xlim([0, limits['xlim'] ])
    # view.plot(ax=axes[0],
    #                     title=io_title(pipeline_config)
    #         )
    axes[1].get_yaxis().set_visible(False)
    axes[1].set_xlim([0, limits['xwaitlim'] ])
    wait_view.astype(int).plot.area(ax=axes[1], stacked=False)

    if not tracer_view.empty:
        tracer_view.plot(kind='barh', stacked=True)

    # show = sys.argv[1]

    # if show == 'show':
    plt.show()
    # else:
    #     storage_name = pipeline_config.storage.name
    #     wl_name = f'wl{str(pipeline_config.workload.id)}'
    #     prefetcher_name = f'pf{str(pipeline_config.prefetcher.id)}'
    #     directory = f'images/{storage_name}/{wl_name}/'
    #     if not os.path.exists(directory):
    #         os.makedirs(directory)
    #     else:
    #         for filename in os.listdir(directory):
    #             file_path = os.path.join(directory, filename)
    #             try:
    #                 if os.path.isfile(file_path) or os.path.islink(file_path):
    #                     os.unlink(file_path)
    #                 elif os.path.isdir(file_path):
    #                     shutil.rmtree(file_path)
    #             except Exception as e:
    #                 print('Failed to delete %s. Reason: %s' % (file_path, e))

    #     plt.savefig(f'{directory}/{prefetcher_name}.png')



for wl_storage_results in all_results:
    for result in wl_storage_results['wl_storage_results']:
        plot_views(result['pipeline_config'],
                wl_storage_results['limits'],
                result['view'],
                result['wait_view'],
                result['tracer_view']
                )
