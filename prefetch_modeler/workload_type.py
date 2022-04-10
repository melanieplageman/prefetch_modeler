from prefetch_modeler.core import RateBucket, StopBucket, Rate


def workload_type(hint, consumption_rate_func):
    class completed(RateBucket):
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

uneven_wl = workload_type('Uneven Workload', consumption_rate_func2)
