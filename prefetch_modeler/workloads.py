from prefetch_modeler.configurer import Workload
from prefetch_modeler.core import Duration, Rate

def test_consumption_rate(self, original):
    return Rate(per_second=5000).value

workload1 = Workload(
    id=1,
    consumption_rate_func=test_consumption_rate,
    volume=200,
    duration=Duration(seconds=10),
    trace_ios = [1, 5, 100]
)


def consumption_rate_func2(self, original):
    if self.tick <= 5000:
        return Rate(per_second=5000).value
    else:
        rate = Rate(per_second=20000).value
        return rate

workload2 = Workload(
    id=2,
    consumption_rate_func=consumption_rate_func2,
    volume=200,
    duration=Duration(seconds=10),
    trace_ios = [1, 5, 100]
)

workload_list = [
    # workload1,
    workload2,
]
