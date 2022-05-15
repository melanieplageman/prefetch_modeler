from prefetch_modeler.core import ContinueBucket, GlobalCapacityBucket, RateBucket, \
Rate, Duration
from prefetch_modeler.slow_prefetcher import SlowPIDPrefetcher
from prefetch_modeler.fast_prefetcher import FastPIDPrefetcher
from prefetch_modeler.test_prefetcher import ControlPrefetcher
from prefetch_modeler.piprefetcher import PIPrefetcher


class BaselineSync(GlobalCapacityBucket):
    name = 'remaining'

    def max_buffers(self):
        return 1


class BaselineFetchAll(ContinueBucket):
    name = 'remaining'


class ConstantPrefetcher(RateBucket):
    name = 'remaining'

    og_rate = Rate(per_second=2000)

    @classmethod
    def hint(cls):
        return (2, f"Constant Rate: {cls.og_rate}")

    def rate(self):
        return self.og_rate.value


class PIDPrefetcher2(FastPIDPrefetcher):
    ki = -Rate(per_second=20).value       # should be in units of per-second
    kp = -0.3                               # should be dimensionless
    kd = -Duration(microseconds=2).total  # should be in units of seconds
    og_rate = Rate(per_second=2000)

class PIDPrefetcher2(FastPIDPrefetcher):
    cnc_headroom = 16
    ki = -Rate(per_second=50).value
    kp = -0.6
    kd = -Duration(microseconds=20).total
    og_rate = Rate(per_second=2000)

class PIDPrefetcher4(SlowPIDPrefetcher):
    ki = -Rate(per_second=2000).value
    kp = -0.9
    kd = -Duration(microseconds=10).total
    og_rate = Rate(per_second=8000)

class PIDPrefetcher1(SlowPIDPrefetcher):
    aw_headroom = 8
    ki = -Rate(per_second=10).value
    kp = -0.8
    kd = -Duration(microseconds=2).total
    og_rate = Rate(per_second=4000)

class PIDPrefetcher3(FastPIDPrefetcher):
    cnc_headroom = 100
    ki = -Rate(per_second=20).value
    kp = -0.6
    kd = -Duration(microseconds=20).total
    og_rate = Rate(per_second=1000)

class TestPrefetcher1(ControlPrefetcher):
    og_rate = Rate(per_second=6000)
    sample_period = 200
    raw_lookback = 2
    avg_lookback = 2

class ControlPrefetcher2(ControlPrefetcher):
    og_rate = Rate(per_second=2000)
    sample_period = 200
    raw_lookback = 5
    avg_lookback = 3

class PIPrefetcher1(PIPrefetcher):
    og_rate = Rate(per_second=500)
    raw_lookback = 4
    avg_lookback = 3
    awd_lookback = 3
    kp = 0.5
    kh = 0.4
    ki_awd = -Rate(per_second=20).value
    ki_cnc = -Rate(per_second=20).value
    cnc_headroom = 50
    min_cnc_headroom = 15

class PIPrefetcher2(PIPrefetcher1):
    min_cnc_headroom = 3

piprefetchera = [PIPrefetcher1]

# prefetcher_list = [
    # [BaselineFetchAll],
    # [BaselineSync],
    # [CoolPrefetcher],
    # [PIDPrefetcher3],
    # [PIDPrefetcher2],
    # [PIDPrefetcher1],
    # [SimpleSamplingPrefetcher5],
    # [ConstantPrefetcher],
    # [AdjustedPrefetcher2],
    # [TestPrefetcher1],
    # [ControlPrefetcher2],
    # [PIPrefetcher1],
    # [PIPrefetcher2],
# ]
