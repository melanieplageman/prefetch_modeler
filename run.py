from modeler import *
from measurer import *
from metric import *
from config import *
import pprint

submitted_bucket = SubmittedBucket()
inflight_bucket = InFlightBucket()
completed_bucket = CompletedBucket()

buckets = [submitted_bucket, inflight_bucket, completed_bucket]
pipeline = Pipeline(buckets)

submitted_measurer = SubmittedMeasurer(submitted_bucket)
inflight_measurer = InflightMeasurer(inflight_bucket)
completed_measurer = CompletedMeasurer(completed_bucket)

bucket_measurers = [submitted_measurer, inflight_measurer, completed_measurer]

nblocks = 50
total_ticks = 50

scan = Scan(pipeline, nblocks)
scan_measurer = ScanMeasurer(scan)

for i in range(total_ticks):
    if scan.consumed >= scan.nblocks:
        break

    scan_measurer.measure()

    for bucket_measurer in bucket_measurers:
        bucket_measurer.measure_before_scan()

    scan.run(i, completed_bucket, inflight_bucket)

    for bucket_measurer in bucket_measurers:
        bucket_measurer.measure_after_scan()

    pipeline.run(i)

submitted_metric = SubmittedMetric(submitted_measurer)
inflight_metric = InflightMetric(inflight_measurer)
completed_metric = CompletedMetric(completed_measurer)
tryconsume_metric = TryConsumeMetric(scan_measurer)
waited_metric = WaitedMetric(scan_measurer)
acquired_metric = AcquiredMetric(scan_measurer)

metrics = [submitted_metric, inflight_metric, completed_metric,
           tryconsume_metric, waited_metric, acquired_metric]

labeled_metrics = {
    'submitted': submitted_metric.data,
    'inflight': inflight_metric.data,
    'completed': completed_metric.data,
    'tryconsume': tryconsume_metric.data,
    'waited': waited_metric.data,
    'acquired': acquired_metric.data
}
