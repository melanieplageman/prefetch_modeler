from prefetch_modeler.core import DialBucket, TargetCapacityBucket, \
    ThresholdBucket, GlobalCapacityBucket, Rate, Duration


def storage_type(name,
                 max_buffers,
                 kernel_invoke_batch_size,
                 submission_overhead_func,
                 completion_latency_func,
                 max_iops):

    class awaiting_buffer(GlobalCapacityBucket):
        def max_buffers(self):
            return max_buffers

    class w_claimed_buffer(ThresholdBucket):
        def threshold(self):
            return kernel_invoke_batch_size

    class kernel_batch(DialBucket):
        def latency(self):
            return submission_overhead_func(self)

    class submitted(TargetCapacityBucket):
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


def submission_latency(self):
    return int(Duration(microseconds=10).total)

def local_storage_latency(self):
    return int(Duration(microseconds=500).total)

fast_local1 = storage_type(
    name = 'fast_local1',
    max_buffers = 200,
    kernel_invoke_batch_size = 5,
    submission_overhead_func = submission_latency,
    completion_latency_func = local_storage_latency,
    max_iops=Rate(per_second=20000).value,
)

def cloud_storage_latency(self):
    return int(Duration(milliseconds=3).total)

slow_cloud1 = storage_type(
    name = 'slow_cloud1',
    max_buffers = 200,
    kernel_invoke_batch_size = 5,
    submission_overhead_func = submission_latency,
    completion_latency_func = cloud_storage_latency,
    max_iops=Rate(per_second=2000).value,
)
