from configurer import Storage
from units import Duration, Rate

def submission_latency(self, original):
    return int(Duration(microseconds=10).total)

def local_storage_latency(self, original):
    return int(Duration(microseconds=500).total)

# TODO: fix it so I can have units like 17000 with this without getting errors
fast_local = Storage(
    completion_latency_func = local_storage_latency,
    kernel_invoke_batch_size = 5,
    submission_overhead_func = submission_latency,
    max_iops=Rate(per_second=20000).value,
)

def cloud_storage_latency(self, original):
    return int(Duration(milliseconds=3).total)

slow_cloud = Storage(
    completion_latency_func = cloud_storage_latency,
    kernel_invoke_batch_size = 5,
    submission_overhead_func = submission_latency,
    max_iops=Rate(per_second=2000).value,
)