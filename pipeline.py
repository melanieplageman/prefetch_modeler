import logging

from model import SubmittedDialBucket, InflightDialBucket, CompletedGateBucket, ConsumedGateBucket, PrefetchedGateBucket
from pipeline_configurer import *

from bucket import GateBucket, IO

class Pipeline:
    def __init__(self):
        self.nblocks_bucket = GateBucket()
        self.submitted_bucket = SubmittedDialBucket()
        self.inflight_bucket = InflightDialBucket()
        self.completed_bucket = CompletedGateBucket()
        self.consumed_bucket = ConsumedGateBucket()

        self.prefetched_bucket = PrefetchedGateBucket(
            self.completed_bucket, self.inflight_bucket)

        self.buckets = [self.nblocks_bucket, self.prefetched_bucket,
                   self.submitted_bucket, self.inflight_bucket,
                   self.completed_bucket, self.consumed_bucket]

        for i in range(len(self.buckets) - 1):
            self.buckets[i].target_bucket = self.buckets[i + 1]

        self.configurer = PipelineConfigurer(self)

    def configure_environment(self, **kwargs):
        self.configurer.environment(**kwargs)

    def configure_prefetcher(self, **kwargs):
        self.configurer.prefetcher(**kwargs)

    def run(self, nticks, nblocks, log_level=logging.INFO):
        logging.basicConfig(filename='prefetch.log', filemode='w',
                            level=log_level)

        for i in range(nblocks):
            self.nblocks_bucket.add(IO(), 0)

        # TODO: add time mode and size mode
        logging.info('Started')
        if nticks < 2 or nblocks < 2:
            raise ValueError('nticks and nblocks must be > 2 to gather enough results to chart.')

        for tick in range(nticks):
            if self.consumed_bucket.num_ios >= nblocks:
                break
            for bucket in self.buckets:
                bucket.run(tick)

        logging.info('Finished')


    def measure(self, nticks):
        to_plot = {}
        to_plot['wait'] = []
        to_plot['completed'] = []
        to_plot['inflight'] = []
        to_plot['consumed'] = []
        to_plot['completion_target_distance'] = []
        to_plot['max_inflight'] = []

        for completed in self.completed_bucket.data:
            num_ios = completed['num_ios']
            to_plot['completed'].append(num_ios)

            to_move = completed['to_move']
            want_to_move = completed['want_to_move']
            waited = True if want_to_move > num_ios + to_move else False
            to_plot['wait'].append(waited)

        for inflight in self.inflight_bucket.data:
            to_plot['inflight'].append(inflight['num_ios'])

        for consumed in self.consumed_bucket.data:
            to_plot['consumed'].append(consumed['num_ios'])

        for prefetched in self.prefetched_bucket.data:
            to_plot['completion_target_distance'].append(prefetched['completion_target_distance'])
            to_plot['max_inflight'].append(prefetched['max_inflight'])

        return to_plot
