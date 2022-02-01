from modeler import *
from measurer import *
import pprint
from measurer import TotalMeasurer
from modeler import Pipeline, Scan, IO, Prefetcher
from custom import SeqScan, SeqScanPrefetcher, SeqScanPipeline

import matplotlib.pyplot as plt
import pandas as pd

def completion_latency_func(num_ios):
    # TODO: make this change with the number of requests inflight
    base_completion_latency = 1.2
    completion_latency = (0.01 * num_ios) + base_completion_latency
    return completion_latency

def submission_overhead_func(num_ios):
    return 0.1

class ConfiguredSeqScanPrefetcher(SeqScanPrefetcher):
    def __init__(self, scan, pipeline):
        super().__init__(scan, pipeline)
        self.min_dispatch = 2
        self.max_in_flight = 10
        self.completion_target_distance = 512

    def submit_size(self):
        if self.completed_bucket.num_ios >= self.completion_target_distance - self.min_dispatch:
            self.completion_target_distance += 1
            return 0

        if self.inflight_bucket.num_ios >= self.max_in_flight:
            return 0

        # Submit the lesser of the number of blocks left in the scan and
        # min_dispatch
        return min(self.scan.nblocks - self.submitted,
                         self.min_dispatch)

class ConfiguredSeqScan(SeqScan):
    def __init__(self, pipeline):
        super().__init__(pipeline)

        self.consumption_rate = 1
        self.nblocks = 10

class Storage(SeqScanPipeline):
    def __init__(self):
        super().__init__()

        self.max_iops = 10000
        self.submitted_bucket.latency = submission_overhead_func
        self.inflight_bucket.latency = completion_latency_func


def main(figure, nticks):
    pipeline = Storage()
    scan = ConfiguredSeqScan(pipeline)
    prefetcher = ConfiguredSeqScanPrefetcher(scan, pipeline)
    measurer = TotalMeasurer(pipeline, scan)
    model = Model(pipeline, scan, prefetcher, measurer)

    labeled_metrics = model.run(nticks)

    ax = figure.add_subplot()
    df = pd.DataFrame(labeled_metrics)
    df.plot(ax=ax)
