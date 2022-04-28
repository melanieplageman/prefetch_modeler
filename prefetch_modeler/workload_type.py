from prefetch_modeler.core import RateBucket, StopBucket, Rate, Interval
from dataclasses import dataclass

@dataclass(kw_only=True)
class SuperInterval(Interval):
    completed: int
    # Note that this is in-progress IOs which are not in flight and not
    # completed -- waiting to be inflight
    awaiting_dispatch: int
    inflight: int

def workload_type(hint, consumption_rate_func):
    class completed(RateBucket):
        cnc_headroom = 2

        @classmethod
        def hint(cls):
            return (1, hint)

        def rate(self):
            return consumption_rate_func(self)

    class consumed(StopBucket):
        pass

    return [completed, consumed]

def test_consumption_rate(self):
    return Rate(per_second=5000).value

even_wl = workload_type('Even Workload', test_consumption_rate)

def consumption_rate_func2(self):
    if getattr(self, 'tick', 0) <= 5000:
        return Rate(per_second=2000).value
    elif getattr(self, 'tick', 0) <= 20000:
        return Rate(per_second=5000).value
    else:
        return Rate(per_second=10000).value

def consumption_rate_func3(self):
    if getattr(self, 'tick', 0) <= 10000:
        return Rate(per_second=1000).value
    elif getattr(self, 'tick', 0) <= 20000:
        return Rate(per_second=500).value
    else:
        return Rate(per_second=10000).value

def consumption_rate_func4(self):
    if getattr(self, 'tick', 0) <= 10000:
        return Rate(per_second=1000).value
    elif getattr(self, 'tick', 0) <= 50000:
        return Rate(per_second=2000).value
    else:
        return Rate(per_second=500).value

uneven_wl = workload_type('Uneven Workload', consumption_rate_func4)
