from prefetch_modeler.core import DialBucket, TargetCapacityBucket, \
    ThresholdBucket, GlobalCapacityBucket, Rate, Duration, ContinueBucket, \
    RateBucket, DeadlineBucket


def io_uring(hint,
                 max_buffers,
                 kernel_invoke_batch_size,
                 submission_overhead_func,
                 completion_latency_func,
                 max_iops):

    class awaiting_buffer(GlobalCapacityBucket):
        @classmethod
        def hint(cls):
            return (0, hint)

        def max_buffers(self):
            return max_buffers

    class w_claimed_buffer(ThresholdBucket):
        def threshold(self):
            return kernel_invoke_batch_size

    class kernel_batch(DialBucket):
        def latency(self):
            return submission_overhead_func(self)

    class submitted(TargetCapacityBucket):
        @property
        def storage_speed(self):
            return max_iops

        def target_capacity(self):
            capacity = completion_latency_func(self) * max_iops
            capacity = int(capacity)
            if capacity < 1:
                raise ValueError("Capacity can't be less than 1")
            return capacity

    class inflight(DialBucket):
        def latency(self):
            return completion_latency_func(self)

    return [awaiting_buffer, w_claimed_buffer, kernel_batch, submitted, inflight]

def simple_storage(hint,
                 max_buffers,
                 kernel_invoke_batch_size,
                 submission_overhead_func,
                 completion_latency_func,
                 max_iops):

    class submitted(TargetCapacityBucket):
        @classmethod
        def hint(cls):
            return (0, hint)

        @property
        def storage_speed(self):
            return max_iops

        def target_capacity(self):
            capacity = completion_latency_func(self) * max_iops
            capacity = int(capacity)
            if capacity < 1:
                raise ValueError("Capacity can't be less than 1")
            return capacity

    class inflight(DialBucket):
        def latency(self):
            return completion_latency_func(self)

        def add(self, *args, **kwargs):
            print(f"Tick: {self.tick}, IO Added")
            super().add(*args, **kwargs)

    return [submitted, inflight]


def simple_storage2(hint,
                 max_buffers,
                 kernel_invoke_batch_size,
                 submission_overhead_func,
                 completion_latency_func,
                 max_iops):

    class minimum_latency(ContinueBucket):
        def add(self, io):
            io.move_at = self.tick + self.latency()
            super().add(io)

        def latency(self):
            return completion_latency_func(self)

    class inflight(RateBucket):
        def rate(self):
            return max_iops

    class deadline(DeadlineBucket):
        pass

    return [minimum_latency, inflight, deadline]


def submission_latency(self):
    return int(Duration(microseconds=10).total)

def local_storage_latency(self):
    return int(Duration(microseconds=500).total)

fast_local1 = simple_storage2(
    'Local Storage',
    max_buffers = 500,
    kernel_invoke_batch_size = 1,
    submission_overhead_func = submission_latency,
    completion_latency_func = local_storage_latency,
    max_iops=Rate(per_second=20000).value,
)

def cloud_storage_latency(self):
    return int(Duration(milliseconds=3).total)

slow_cloud1 = simple_storage2(
    'Cloud Storage',
    max_buffers = 200,
    kernel_invoke_batch_size = 1,
    submission_overhead_func = submission_latency,
    completion_latency_func = cloud_storage_latency,
    max_iops=Rate(per_second=1200).value,
)

empty_storage = []
