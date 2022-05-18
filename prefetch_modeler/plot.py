import matplotlib.pyplot as plt
import pandas as pd

class ChartGroup:
    def __init__(self, simulation, *args):
        self.simulation = simulation
        self.charts = args

        for chart in self.charts:
            chart.attach(self.simulation)

    @property
    def title(self):
        return ', '.join(hint for i, hint in
            sorted(bucket_type.hint() for bucket_type in self.simulation.schema if
                    bucket_type.hint() is not None)
        )

    @classmethod
    def show(cls, timeline, *args):
        chart_groups = args
        stripe_ylimits = {}
        xlimit = Limit(0, 0)
        for chart_group in chart_groups:
            for chart in chart_group.charts:
                data = chart.data
                xlimit.set(chart.xlimit.lower, chart.xlimit.upper)

                stripe_ylimit = stripe_ylimits.setdefault(chart.name, Limit())
                stripe_ylimit.set(chart.ylimit.lower, chart.ylimit.upper)

        max_nrows = max(len(instance.charts) for instance in chart_groups)
        ncols = len(chart_groups)
        width = 11 * ncols
        height = 5 * max_nrows

        figure = plt.figure(figsize=(width, height))
        axes = figure.subplots(max_nrows, ncols, squeeze=False)
        plt.subplots_adjust(bottom=0.1, right=0.8, top=0.9)

        for col, instance in enumerate(chart_groups):
            for row, chart in enumerate(instance.charts):
                axes[row][col].set_ylim(
                    stripe_ylimits[chart.name].lower,
                    stripe_ylimits[chart.name].upper
                )
                axes[row][col].set_xlim(xlimit.lower, xlimit.upper)
                chart.plot(axes[row][col], timeline)

        for i, chart_group in enumerate(chart_groups):
            axes[0][i].set_title(chart_group.title)


        plt.savefig('current.png')

class Limit:
    def __init__(self, lower=None, upper=None):
        self.lower = lower
        self.upper = upper

    def set_upper(self, val):
        if self.upper is None:
            self.upper = val
            return
        if self.upper < val:
            self.upper = val

    def set_lower(self, val):
        if self.lower is None:
            self.lower = val
            return
        if self.lower > val:
            self.lower = val

    def set(self, lower, upper):
        self.set_lower(lower)
        self.set_upper(upper)

    def merge(self, other):
        limit = Limit()
        limit.set(other.lower, other.upper)
        limit.set(self.lower, self.upper)
        return limit

    def __str__(self):
        return f'Limit({self.lower}, {self.upper})'

class Chart:
    plot_type = 'line'

    def __init__(self, name, *args, plot_type='line', **kwargs):
        self.name = name
        self.ylimit = Limit()
        self.xlimit = Limit()
        self.metric_schema = {
            metric_type.__name__: metric_type() for metric_type in args
        }
        self.plot_type = plot_type
        self.kwargs = kwargs
        self._data = None

    def attach(self, simulation):
        simulation.metrics.extend(self.metric_schema.values())

    @property
    def data(self):
        if self._data is None:
            data = pd.DataFrame()
            for metric in self.metric_schema.values():
                data = data.join(metric.data, how='outer')
            self._data = data

            self.xlimit.set(0, max(self._data.index))
            for column in self._data.columns:
                self.ylimit.set(min(self._data[column]), max(self._data[column]))

        return self._data

    def plot(self, ax, timeline):
        metric_data = self.data
        metric_data = metric_data.reindex(metric_data.index.union(metric_data.index[1:] - 1), method='ffill')

        columns = self.metric_schema.keys()
        getattr(metric_data.plot, self.plot_type)(y=columns, ax=ax, **self.kwargs)

