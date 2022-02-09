import logging

from model import SubmittedDialBucket, InflightDialBucket, CompletedGateBucket, ConsumedGateBucket, PrefetchedGateBucket

from bucket import GateBucket, IO

class Pipeline:
    def __init__(self, nblocks, completion_target_distance=512, min_dispatch=2,
                 max_inflight=10, submission_overhead=1, max_iops=10,
                 base_completion_latency=1, consumption_rate=1,
                 consumption_size=1):

        self.nblocks_bucket = GateBucket()
        self.submitted_bucket = SubmittedDialBucket()
        self.inflight_bucket = InflightDialBucket()
        self.completed_bucket = CompletedGateBucket()
        self.consumed_bucket = ConsumedGateBucket()

        self.prefetched_bucket = PrefetchedGateBucket(
            self.completed_bucket, self.inflight_bucket)

        self.prefetched_bucket.completion_target_distance = completion_target_distance
        self.prefetched_bucket.min_dispatch = min_dispatch
        self.prefetched_bucket.max_inflight = max_inflight

        self.submitted_bucket.SUBMISSION_OVERHEAD = submission_overhead

        self.inflight_bucket.MAX_IOPS = max_iops
        self.inflight_bucket.BASE_COMPLETION_LATENCY = base_completion_latency

        self.completed_bucket.CONSUMPTION_RATE = consumption_rate
        self.completed_bucket.CONSUMPTION_SIZE = consumption_size

        for i in range(nblocks):
            self.nblocks_bucket.add(IO(), 0)

        buckets = [self.nblocks_bucket, self.prefetched_bucket,
                   self.submitted_bucket, self.inflight_bucket,
                   self.completed_bucket, self.consumed_bucket]

        for i in range(len(buckets) - 1):
            buckets[i].target_bucket = buckets[i + 1]

        self.buckets = buckets

    def run(self, nticks, nblocks, log_level=logging.WARNING):
        logging.basicConfig(filename='prefetch.log', filemode='w',
                            level=log_level)

        logging.info('Started')

        nticks = max(1, nticks)
        for tick in range(nticks):
            for bucket in self.buckets:
                bucket.run(tick)

        while self.consumed_bucket.num_ios < nblocks:
            tick += 1
            for bucket in self.buckets:
                bucket.run(tick)

        logging.info('Finished')


    def measure(self, nticks):
        to_plot = {}
        to_plot['wait'] = []
        to_plot['completed'] = []
        to_plot['inflight'] = []
        to_plot['consumed'] = []

        for completed in self.completed_bucket.data:
            num_ios = completed['num_ios']
            to_plot['completed'].append(num_ios)

            to_move = completed['to_move']
            want_to_move = completed['want_to_move']
            waited = 1 if want_to_move > num_ios + to_move else 0
            to_plot['wait'].append(waited)

        for inflight in self.inflight_bucket.data:
            to_plot['inflight'].append(inflight['num_ios'])

        for consumed in self.consumed_bucket.data:
            to_plot['consumed'].append(consumed['num_ios'])

        return to_plot
