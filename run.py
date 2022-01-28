from modeler import *
from measurer import *
from metric import *
import pprint

import matplotlib.pyplot as plt
import pandas as pd

class Model:
    def __init__(self, config):
        submitted_bucket = SubmittedBucket()
        self.inflight_bucket = InFlightBucket()
        self.completed_bucket = CompletedBucket()

        self.pipeline = Pipeline([
            submitted_bucket, self.inflight_bucket, self.completed_bucket
        ])

        self.submitted_measurer = SubmittedMeasurer(self.submitted_bucket)
        self.inflight_measurer = InflightMeasurer(self.inflight_bucket)
        self.completed_measurer = CompletedMeasurer(self.completed_bucket)

        self.bucket_measurers = [self.submitted_measurer,
                                 self.inflight_measurer, self.completed_measurer]

        nblocks = config['run']['nblocks']
        self.scan = Scan(self.pipeline, nblocks)
        self.scan_measurer = ScanMeasurer(self.scan)

        self.total_ticks = config['run']['nticks']
        self.labeled_metrics = None

    def run(self):
        for i in range(self.total_ticks):
            if self.scan.consumed >= self.scan.nblocks:
                break

            self.scan_measurer.measure()

            for bucket_measurer in self.bucket_measurers:
                bucket_measurer.measure_before_scan()

            self.scan.run(i, self.completed_bucket, self.inflight_bucket)

            for bucket_measurer in self.bucket_measurers:
                bucket_measurer.measure_after_scan()

            self.pipeline.run(i)

        submitted_metric = SubmittedMetric(self.submitted_measurer)
        inflight_metric = InflightMetric(self.inflight_measurer)
        completed_metric = CompletedMetric(self.completed_measurer)
        tryconsume_metric = TryConsumeMetric(self.scan_measurer)
        waited_metric = WaitedMetric(self.scan_measurer)
        acquired_metric = AcquiredMetric(self.scan_measurer)

        metrics = [submitted_metric, inflight_metric, completed_metric,
                tryconsume_metric, waited_metric, acquired_metric]

        self.labeled_metrics = {
            'submitted': submitted_metric.data,
            'inflight': inflight_metric.data,
            'completed': completed_metric.data,
            'tryconsume': tryconsume_metric.data,
            'waited': waited_metric.data,
            'acquired': acquired_metric.data
        }

    def plot(self, figure):
        ax = figure.add_subplot()
        df = pd.DataFrame(self.labeled_metrics)

        df.plot( ax=ax)
