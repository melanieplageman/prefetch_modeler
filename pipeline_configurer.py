class PipelineConfigurer:
    def __init__(self, pipeline):
        self.pipeline = pipeline

    def environment(self, inflight_cap, submission_overhead, max_iops,
                    base_completion_latency, consumption_rate,
                    consumption_size):

        self.pipeline.prefetched_bucket.inflight_cap = inflight_cap

        self.pipeline.submitted_bucket.SUBMISSION_OVERHEAD = submission_overhead

        self.pipeline.inflight_bucket.MAX_IOPS = max_iops
        self.pipeline.inflight_bucket.BASE_COMPLETION_LATENCY = base_completion_latency

        self.pipeline.completed_bucket.CONSUMPTION_RATE = consumption_rate
        self.pipeline.completed_bucket.CONSUMPTION_SIZE = consumption_size

    def prefetcher(self, prefetch_distance_algorithm, completed_cap,
                   completion_target_distance, min_dispatch, max_inflight):

        pbucket = self.pipeline.prefetched_bucket

        pbucket.desired_move_size_override = prefetch_distance_algorithm
        pbucket.completed_cap = completed_cap

        if completion_target_distance > pbucket.completed_cap:
            print(f'Cannot set completion_target_distance to {completion_target_distance}. Setting instead to completed_cap {pbucket.completed_cap}.')
            pbucket.completion_target_distance = pbucket.completed_cap
        else:
            pbucket.completion_target_distance = completion_target_distance

        # TODO: add some boundaries for min_dispatch
        pbucket.min_dispatch = min_dispatch

        if max_inflight > pbucket.inflight_cap:
            print(f'Cannot set max_inflight to {max_inflight}. Setting instead to inflight_cap {inflight_cap}.')
            pbucket.max_inflight = pbucket.inflight_cap
        else:
            pbucket.max_inflight = max_inflight
