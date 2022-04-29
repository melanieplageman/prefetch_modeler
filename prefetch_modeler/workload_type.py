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

        def should_adjust(self, before):
            if self.adj_cnc(before) > 0:
                return False


        def adjust(self):
            pass

#         def run(self, *args, **kwargs):
#             before = SuperInterval(tick=self.tick, rate=self.rate(),
#                                    completed=self.completed,
#                                    awaiting_dispatch=self.awaiting_dispatch,
#                                    inflight=self.inflight)
#             super().run(*args, **kwargs)
#             ledger = self.pipeline['prefetcher'].ledger
#             current_sample = ledger[-1]
#             if len(ledger) > 1:
#                 previous_sample = ledger[-2]

#                 elapsed_current_period = self.tick - current_sample.tick
#                 delta_cnc = self.completed - before.completed

#                 depletion_rate = Fraction(delta_cnc, elapsed_current_period)

#                 remaining_period_length = previous_sample.length - elapsed_current_period

#                 expected_depletion = math.ceil(depletion_rate * remaining_period_length)


#             if self.should_adjust(before):
#                 self.adjust()

        def adj_cnc(self, period):
            return period.completed - self.cnc_headroom

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
