class workload_type(consumption_rate_func):
    class completed(RateBucket):
        def rate(self):
            return consumption_rate_func(self)

    class consumed(StopBucket):
        pass

    return [completed, consumed]

def test_consumption_rate(self, original):
    return Rate(per_second=5000).value

workload1 = Workload(test_consumption_rate)

def consumption_rate_func2(self, original):
    if self.tick <= 5000:
        return Rate(per_second=5000).value
    else:
        rate = Rate(per_second=20000).value
        return rate

workload2 = Workload(consumption_rate_func2)
