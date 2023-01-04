import matplotlib.pyplot as plt
import pandas as pd
import math

class MetaChartGroup(type):
    def __init__(self, name, bases, namespace):
        super().__init__(name, bases, namespace)

        self._charts = []
        for attr_name, attr_value in namespace.items():
            try:
                is_chart = issubclass(attr_value, Chart)
            except TypeError:
                continue

            if is_chart:
                attr_value.name = attr_name
                self._charts.append(attr_value)


class ChartGroup:
    def __init__(self, simulation):
        self.simulation = simulation
        self.charts = [chart() for chart in self._charts]

        for chart in self.charts:
            chart.attach(self.simulation)

    @property
    def title(self):
        return ', '.join(hint for i, hint in
            sorted(bucket_type.hint() for bucket_type in self.simulation.schema if
                    bucket_type.hint() is not None)
        )

    @classmethod
    def show(cls, *args):
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

        for col, instance in enumerate(chart_groups):
            for row, chart in enumerate(instance.charts):
                lower = stripe_ylimits[chart.name].lower
                upper = stripe_ylimits[chart.name].upper
                if lower is not None and lower < 0:
                    lower = lower * 1.15
                elif lower is not None and lower > 0:
                    lower = lower * 0.85
                elif lower is not None and lower == 0:
                    lower = 0
                else:
                    lower = None
                upper = upper * 1.15 if upper is not None else upper
                axes[row][col].set_ylim(lower, upper)
                axes[row][col].set_xlim(xlimit.lower, xlimit.upper)
                try:
                    chart.plot(axes[row][col])
                except Exception:
                    print(f"Can't plot metric {chart.name}")

        for i, chart_group in enumerate(chart_groups):
            axes[0][i].set_title(chart_group.title)

        # plt.show()
        plt.savefig('current.png')

class Limit:
    def __init__(self, lower=None, upper=None):
        if lower is not None and math.isnan(lower):
            lower = None
        if upper is not None and math.isnan(upper):
            upper = None
        self.lower = lower
        self.upper = upper

    def __repr__(self):
        return f'Limit({self.lower!r}, {self.upper!r})'

    def set_upper(self, val):
        if val is None or math.isnan(val):
            return
        if self.upper is None:
            self.upper = val
            return
        if self.upper < val:
            self.upper = val

    def set_lower(self, val):
        if val is None or math.isnan(val):
            return
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


def ChartType(*args, plot_type='line', **kwargs):
    class chart_type(Chart):
        _plot_type = plot_type
        _kwargs = kwargs
        _metric_schema = {metric_type.__name__ : metric_type for metric_type in args}

    return chart_type

class Chart:
    name = None
    _plot_type = 'line'
    _metric_schema = {}
    _kwargs = {}

    def __init__(self):
        if not self._metric_schema:
            raise Exception("Chart must have metrics")

        self.metric_schema = {
            metric_name: metric_type() for metric_name, metric_type in self._metric_schema.items()
        }
        self._data = None

    @property
    def kwargs(self):
        return self._kwargs

    @property
    def plot_type(self):
        return self._plot_type

    def attach(self, simulation):
        simulation.metrics.extend(self.metric_schema.values())

    @property
    def data(self):
        if self._data is None:
            data = pd.DataFrame()
            for metric in self.metric_schema.values():
                data = data.join(metric.data, how='outer')
            self._data = data
        return self._data

    @property
    def xlimit(self):
        xlimit = Limit()
        xlimit.set(0, self._data.index.max(skipna=True))
        return xlimit

    @property
    def ylimit(self):
        ylimit = Limit()
        for column in self._data.columns:
            upper = self._data[column].max(skipna=True)
            lower = self._data[column].min(skipna=True)
            ylimit.set(lower, upper)
        return ylimit


    def plot(self, ax):
        metric_data = self.data
        metric_data = metric_data.reindex(metric_data.index.union(metric_data.index[1:] - 1), method='ffill')

        columns = self.metric_schema.keys()
        getattr(metric_data.plot, self.plot_type)(y=columns, ax=ax, **self.kwargs)

