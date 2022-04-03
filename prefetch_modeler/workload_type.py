from prefetch_modeler.core import RateBucket, StopBucket, Rate


def workload_type(consumption_rate_func):
    class completed(RateBucket):
        def rate(self):
            return consumption_rate_func(self)

    class consumed(StopBucket):
        pass

    return [completed, consumed]

def test_consumption_rate(self):
    return Rate(per_second=5000).value

even_wl = workload_type(test_consumption_rate)

def consumption_rate_func2(self):
    if self.tick <= 5000:
        return Rate(per_second=5000).value
    else:
        rate = Rate(per_second=20000).value
        return rate

uneven_wl = workload_type(consumption_rate_func2)
