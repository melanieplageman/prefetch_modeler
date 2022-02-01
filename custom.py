from modeler import Scan, Prefetcher, IO, Pipeline, Bucket

class SeqScan(Scan):
    def __init__(self, pipeline):
        super().__init__(pipeline)

        self.submitted_bucket = self.pipeline.buckets[0]
        self.inflight_bucket = self.pipeline.buckets[1]
        self.completed_bucket = self.pipeline.buckets[2]

class SeqScanPrefetcher(Prefetcher):
    def __init__(self, scan, pipeline):
        super().__init__(scan, pipeline)

        self.submitted_bucket = pipeline.buckets[0]
        self.inflight_bucket = pipeline.buckets[1]
        self.completed_bucket = pipeline.buckets[2]


class SeqScanPipeline(Pipeline):
    def __init__(self):
        buckets = [Bucket(), Bucket(), Bucket()]
        super().__init__(buckets)

        self.submitted_bucket = buckets[0]
        self.inflight_bucket = buckets[1]
        self.completed_bucket = buckets[2]
