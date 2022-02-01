class BucketMeasurer:
    def __init__(self, bucket):
        self.before_ios = []
        self.after_ios = []
        self.bucket = bucket

    def run_before_scan(self):
        self.before_ios.append(self.bucket.num_ios)

    def run_after_scan(self):
        self.after_ios.append(self.bucket.num_ios)

    def __repr__(self):
        return f'{self.bucket!r}:\n' + f'Before: {self.before_ios}\nAfter:  {self.after_ios}\n'


class ScanMeasurer:
    def __init__(self, scan):
        self.scan = scan
        self.tried_consumed = []
        self.waited = []
        self.acquired = []

    def __repr__(self):
        return f'TryConsume:{self.tried_consumed}\nAcquired:  {self.acquired}\nWaited:    {self.waited}\n'

    def run_before_scan(self):
        self.tried_consumed.append(int(self.scan.tried_consumed))
        self.acquired.append(int(self.scan.acquired_io is not None))
        self.waited.append(int(self.scan.acquired_io is None))

    def run_after_scan(self):
        pass

class TotalMeasurer:
    def __init__(self, pipeline, scan):
        self.submitted_measurer = BucketMeasurer(pipeline.buckets[0])
        self.inflight_measurer = BucketMeasurer(pipeline.buckets[1])
        self.completed_measurer = BucketMeasurer(pipeline.buckets[2])

        self.scan_measurer = ScanMeasurer(scan)

    def run_before_scan(self):
        self.scan_measurer.run_before_scan()
        self.submitted_measurer.run_before_scan()
        self.inflight_measurer.run_before_scan()
        self.completed_measurer.run_before_scan()

    def run_after_scan(self):
        self.scan_measurer.run_after_scan()
        self.submitted_measurer.run_after_scan()
        self.inflight_measurer.run_after_scan()
        self.completed_measurer.run_after_scan()

    def analyze(self):
        submitted_difference = []
        for i in range(len(self.submitted_measurer.after_ios)):
            submitted_difference.append(self.submitted_measurer.after_ios[i] -
                                        self.submitted_measurer.before_ios[i])

        labeled_metrics = {
            'submitted': submitted_difference,
            'inflight': self.inflight_measurer.before_ios,
            'completed': self.completed_measurer.before_ios,
            'tryconsume': self.scan_measurer.tried_consumed,
            'waited': self.scan_measurer.waited,
            'acquired': self.scan_measurer.acquired,
        }
        return labeled_metrics
