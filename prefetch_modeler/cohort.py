from prefetch_modeler.storage_type import fast_local1, slow_cloud1
from prefetch_modeler.core import Duration, Rate, Simulation
from prefetch_modeler.plot import io_data, wait_data, rate_data, pid_data
import matplotlib.pyplot as plt

class Member:
    def __init__(self, storage, workload, prefetcher):
        self.data = None
        self.tracer_data = None
        self._wait_view = None
        self._io_view = None
        self._rate_view = None
        self._pid_view = None
        self.storage = storage
        self.workload = workload
        self.prefetcher = prefetcher
        self.schema = None

    def run(self):
        simulation = Simulation(*self.prefetcher, *self.storage, *self.workload)
        self.schema = simulation.schema

        result = simulation.run(1000, duration=Duration(seconds=20), traced=[1, 5, 100])
        data = result.bucket_data
        self.data = data.reindex(data.index.union(data.index[1:] - 1), method='ffill')
        self.tracer_data = result.tracer_data

    @property
    def wait_view(self):
        if self._wait_view is None:
            self._wait_view = wait_data(self.data)
        return self._wait_view

    @property
    def rate_view(self):
        if self._rate_view is None:
            self._rate_view = rate_data(self.data)
        return self._rate_view

    @property
    def pid_view(self):
        if self._pid_view is None:
            self._pid_view = pid_data(self.data)
        return self._pid_view

    @property
    def io_view(self):
        if self._io_view is None:
            self._io_view = io_data(self.data)
        return self._io_view

    @property
    def tracer_view(self):
        return self.tracer_data


def calc_upper_ylim(df, columns, current_ylim):
    max_y = 0
    for column in columns:
        if column not in df.columns:
            continue
        column_max_y = max(df[column])
        max_y = column_max_y if column_max_y > max_y else max_y

    return max_y if max_y > current_ylim else current_ylim

def calc_lower_ylim(df, columns, current_ylim):
    min_y = 0
    for column in columns:
        if column not in df.columns:
            continue
        column_min_y = min(df[column])
        min_y = column_min_y if column_min_y < min_y else min_y

    return min_y if min_y < current_ylim else current_ylim

class Cohort:
    """
    DataFrames generated by running a list of prefetch algorithms and baseline
    fetchers for a given workload and storage type
    """
    def __init__(self):
        self.members = []

    def run(self, storage, workload, prefetchers):
        for prefetcher in prefetchers:
            self.members.append(Member(storage, workload, prefetcher))

        for member in self.members:
            member.run()

    def dump_plots(self, storage_name, workload_name):
        xlim = 0
        yiolim, yratelim = 0, 0
        ypid_integral_lowerlim, ypid_integral_upperlim = 0, 0
        ypid_proportional_lowerlim, ypid_proportional_upperlim = 0, 0
        ypid_derivative_lowerlim, ypid_derivative_upperlim = 0, 0
        for member in self.members:
            max_x = max(max(member.rate_view.index),
                        max(member.pid_view.index),
                        max(member.io_view.index))
            xlim = max_x if max_x > xlim else xlim

            yiolim = calc_upper_ylim(member.io_view, member.io_view.columns, yiolim)

            yratelim = calc_upper_ylim(member.rate_view,
                                       member.rate_view.columns, yratelim)

            columns = ['integral_term', 'integral_term_w_coefficient']
            ypid_integral_upperlim = calc_upper_ylim(member.pid_view, columns, ypid_integral_upperlim)
            ypid_integral_lowerlim = calc_lower_ylim(member.pid_view, columns, ypid_integral_lowerlim)

            columns = ['derivative_term', 'derivative_term_w_coefficient']
            ypid_derivative_upperlim = calc_upper_ylim(member.pid_view, columns, ypid_derivative_upperlim)
            ypid_derivative_lowerlim = calc_lower_ylim(member.pid_view, columns, ypid_derivative_lowerlim)

            columns = ['proportional_term', 'proportional_term_w_coefficient']
            ypid_proportional_upperlim = calc_upper_ylim(member.pid_view, columns, ypid_proportional_upperlim)
            ypid_proportional_lowerlim = calc_lower_ylim(member.pid_view, columns, ypid_proportional_lowerlim)

        directory = f'images/{storage_name}/{workload_name}/'

        ypid_integral_upperlim *= 1.1
        ypid_derivative_upperlim *= 1.1
        ypid_proportional_upperlim *= 1.1
        yratelim *= 1.05
        for member in self.members:
            title_str = ", ".join(hint for i, hint in
                sorted(bucket_type.hint() for bucket_type in member.schema if
                       bucket_type.hint() is not None)
            )

            figure, axes = plt.subplots(6)
            figure.set_size_inches(15, 25)

            axes[0].set_xlim([0, xlim])
            axes[0].set_ylim([0, yiolim])
            member.io_view.plot(ax=axes[0], title=title_str)

            axes[1].get_yaxis().set_visible(False)
            axes[1].set_xlim([0, xlim])
            member.wait_view.astype(int).plot.area(ax=axes[1], stacked=False)

            axes[2].set_xlim([0, xlim])
            axes[2].set_ylim([0, yratelim])
            member.rate_view.plot(ax=axes[2])

            prefetcher_name = '_'.join([bucket.__name__ for bucket in member.prefetcher])
            if prefetcher_name in ['BaselineFetchAll', 'BaselineSync']:
                plt.savefig(f'{directory}/{prefetcher_name}.png')
                continue

            axes[3].set_xlim([0, xlim])
            axes[3].set_ylim([ypid_integral_lowerlim, ypid_integral_upperlim])

            member.pid_view.plot(y=['integral_term'], ax=axes[3])
            member.pid_view.plot(y=['integral_term_w_coefficient'], ax=axes[3])

            axes[4].set_xlim([0, xlim])
            axes[4].set_ylim([ypid_derivative_lowerlim, ypid_derivative_upperlim])

            member.pid_view.plot(y=['derivative_term'], ax=axes[4])
            member.pid_view.plot(y=['derivative_term_w_coefficient'], ax=axes[4])

            axes[5].set_xlim([0, xlim])
            axes[5].set_ylim([ypid_proportional_lowerlim, ypid_proportional_upperlim])

            member.pid_view.plot(y=['proportional_term'], ax=axes[5])
            member.pid_view.plot(y=['proportional_term_w_coefficient'], ax=axes[5])

            plt.savefig(f'{directory}/{prefetcher_name}.png')
            # plt.show()

            if not member.tracer_view.empty:
                member.tracer_view.plot(kind='barh', stacked=True)
                plt.savefig(f'{directory}/{prefetcher_name}_tracer.png')

