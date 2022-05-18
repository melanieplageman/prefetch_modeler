from prefetch_modeler.core import ContinueBucket, GlobalCapacityBucket, RateBucket, \
Rate, Duration
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
