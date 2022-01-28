class BucketMeasurer:
    def __init__(self, bucket):
        self.before_ios = []
        self.after_ios = []
        self.bucket = bucket

    def measure_before_scan(self):
        self.before_ios.append(self.bucket.num_ios)

    def measure_after_scan(self):
        self.after_ios.append(self.bucket.num_ios)

    def __repr__(self):
        return f'Before: {self.before_ios}\nAfter:  {self.after_ios}\n'


class SubmittedMeasurer(BucketMeasurer):
    def __repr__(self):
        return 'Submitted:\n' + super().__repr__()

class InflightMeasurer(BucketMeasurer):
    def __repr__(self):
        return 'InFlight:\n' + super().__repr__()

class CompletedMeasurer(BucketMeasurer):
    def __repr__(self):
        return 'Completed:\n' + super().__repr__()

class ScanMeasurer:
    def __init__(self, scan):
        self.scan = scan
        self.tried_consumed = []
        self.waited = []
        self.acquired = []

    def __repr__(self):
        return f'TryConsume:{self.tried_consumed}\nAcquired:  {self.acquired}\nWaited:    {self.waited}\n'

    def measure(self):
        self.tried_consumed.append(int(self.scan.tried_consumed))
        self.acquired.append(int(self.scan.acquired_io is not None))
        self.waited.append(int(self.scan.acquired_io is None))
