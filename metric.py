class Metric:
    def __init__(self, measurer):
        self.measurer = measurer

    @property
    def data(self):
        return self.measurer.before_ios

    def __repr__(self):
        return f'{type(self)}: {self.data}'

class SubmittedMetric(Metric):
    @property
    def data(self):
        difference = []
        for i in range(len(self.measurer.after_ios)):
            difference.append(self.measurer.after_ios[i] -
                              self.measurer.before_ios[i])
        return difference

class InflightMetric(Metric):
    @property
    def data(self):
        return self.measurer.before_ios

class CompletedMetric(Metric):
    @property
    def data(self):
        return self.measurer.before_ios

class TryConsumeMetric(Metric):
    @property
    def data(self):
        return self.measurer.tried_consumed

class WaitedMetric(Metric):
    @property
    def data(self):
        return self.measurer.waited

class AcquiredMetric(Metric):
    @property
    def data(self):
        return self.measurer.acquired

