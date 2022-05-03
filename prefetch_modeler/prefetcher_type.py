from prefetch_modeler.core import ContinueBucket, GlobalCapacityBucket, RateBucket, \
Rate, Duration
from prefetch_modeler.slow_prefetcher import SlowPIDPrefetcher
from prefetch_modeler.fast_prefetcher import FastPIDPrefetcher
from prefetch_modeler.test_prefetcher import TestPrefetcher


class BaselineSync(GlobalCapacityBucket):
    name = 'remaining'

    def max_buffers(self):
        return 1

    def to_move(self):
        self.tick_data['awaiting_dispatch'] = self.awaiting_dispatch
        return super().to_move()

    @property
    def completed(self):
        return len(self.pipeline['completed'])

    @property
    def in_progress(self):
        return self.counter - len(self.pipeline['consumed'])

    @property
    def awaiting_dispatch(self):
        return self.in_progress - self.inflight - self.completed - len(self)

    @property
    def inflight(self):
        return len(self.pipeline['inflight'])


class BaselineFetchAll(ContinueBucket):
    name = 'remaining'

    def to_move(self):
        self.tick_data['awaiting_dispatch'] = self.awaiting_dispatch
        return super().to_move()

    @property
    def completed(self):
        return len(self.pipeline['completed'])

    @property
    def in_progress(self):
        return self.counter - len(self.pipeline['consumed'])

    @property
    def awaiting_dispatch(self):
        return self.in_progress - self.inflight - self.completed - len(self)

    @property
    def inflight(self):
        return len(self.pipeline['inflight'])


class ConstantPrefetcher(RateBucket):
    name = 'remaining'

    og_rate = Rate(per_second=2000)

    @classmethod
    def hint(cls):
        return (2, f"Constant Rate: {cls.og_rate}")

    def rate(self):
        return self.og_rate.value

    @property
    def completed(self):
        return len(self.pipeline['completed'])

    @property
    def in_progress(self):
        return self.counter - len(self.pipeline['consumed'])

    @property
    def awaiting_dispatch(self):
        return self.in_progress - self.inflight - self.completed - len(self)

    @property
    def inflight(self):
        return len(self.pipeline['inflight'])


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

class TestPrefetcher1(TestPrefetcher):
    og_rate = Rate(per_second=6000)
    sample_period = 200
    lookback = 16

class TestPrefetcher2(TestPrefetcher):
    og_rate = Rate(per_second=6000)
    sample_period = 200
    lookback = 15


prefetcher_list = [
    # [BaselineFetchAll],
    # [BaselineSync],
    # [CoolPrefetcher],
    # [PIDPrefetcher3],
    # [PIDPrefetcher2],
    # [PIDPrefetcher1],
    # [SimpleSamplingPrefetcher5],
    # [ConstantPrefetcher],
    # [AdjustedPrefetcher2],
    [TestPrefetcher1],
    [TestPrefetcher2],
]
