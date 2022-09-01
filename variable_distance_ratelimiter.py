from prefetch_modeler.core import ContinueBucket, GlobalCapacityBucket, RateBucket, \
Rate, Duration, ForkBucket, Bucket, GateBucket
from prefetch_modeler.prefetcher_type import ConstantRatePrefetcher
from constant_distance_prefetcher import ConstantDistancePrefetcher
from dataclasses import dataclass
from fractions import Fraction
import itertools
import math
from numpy import mean

@dataclass
class LatencyLogEntry:
    tick: int
    in_storage: int
    latency: float

@dataclass
class WaitLogEntry:
    tick: int
    in_storage: int
    wait: float


class VariableDistanceLimitPrefetcher(ConstantDistancePrefetcher):
    headroom = 20
    target_idle_time = 20

    def __init__(self, *args, **kwargs):
        self.prefetch_distance = self.headroom
        self.latency_log = []
        self.wait_log = []
        self.prefetch_distance_log = []
        self.consumption_log = []
        self.change_log = []

        self.waited_at = None
        self.last_adjusted = 0
        self.adjust = True
        self.printed = False

        super().__init__(*args, **kwargs)

    def remove(self, io):
        io.submission_time = self.tick
        super().remove(io)


    @property
    def in_storage(self):
        return len(self.pipeline['minimum_latency']) + \
            len(self.pipeline['inflight']) + \
            len(self.pipeline['deadline'])

    def max_buffers(self):
        if self.adjust is False:
            return int(self.prefetch_distance)

        if self.waited_at is not None:
            wait_time = self.tick - self.waited_at
        else:
            wait_time = 0

        idle_time = sum(self.tick - io.completion_time for io in self.pipeline['completed'])
        idle_time -= self.headroom * self.target_idle_time

        delta = wait_time - idle_time

        self.adjust = False
        self.prefetch_distance_log.append(self.prefetch_distance)
        self.last_adjusted = self.tick

        self.info['delta'] = delta
        self.info['wait_time'] = wait_time
        self.info['idle_time'] = idle_time

        if self.in_storage > 0:
            wait_benefit = float(wait_time / self.in_storage)
            avg_total_latency = self.avg_real_io_latency(self.pipeline['completed'])
            if avg_total_latency is not None:
                latency_cost = float(avg_total_latency / self.in_storage)
                # if latency_cost > 0 and len(self.wait_log) > 1:
                    # if wait_benefit < 1000:
                    #     self.prefetch_distance = self.prefetch_distance * 0.95
        else:
            wait_benefit = None

        self.info['wait_benefit'] = wait_benefit


        if wait_benefit is not None:
            new_wait_log = WaitLogEntry(tick=self.tick, in_storage=self.in_storage, wait=wait_time)

            if len(self.wait_log) > 1:
                self.info['wait_benefit_dt'] = self.wait_benefit_dt(self.wait_log[-1],
                                                            new_wait_log)

                self.info['wait_dt'] = self.wait_dt(self.wait_log[-1],
                                                            new_wait_log)
            if wait_benefit is not None:
                self.wait_log.append(new_wait_log)

        diff_storage_prefetch = self.prefetch_distance - self.in_storage

        if wait_benefit is not None:
            diff_wait_diff = diff_storage_prefetch - (wait_benefit / 1000)
            # print(f'diff between wait benefit and delta between in storage and prefetch is {diff_wait_diff}. prefetch distance before: {self.prefetch_distance}. prefetch distance after: {self.prefetch_distance + diff_wait_diff}')
            # self.prefetch_distance = diff_wait_diff + self.prefetch_distance

        return int(self.prefetch_distance)

    @property
    def cnc(self):
        return len(self.pipeline['completed'])

    def wait_dt(self, old, new):
        return (new.wait - old.wait) / (new.tick - old.tick)

    def avg_real_io_latency(self, ios):
        real_ios = [io for io in ios if getattr(io, 'cached', None) is None]
        if not real_ios:
            return None
        completion_latencies = [io.completion_time - io.submission_time for io in real_ios]
        return mean(completion_latencies)

    def latency_dt(self, old, new):
        return (new.latency - old.latency) / (new.tick - old.tick)

    def in_storage_dt(self, old, new):
        return (new.in_storage - old.in_storage) / (new.tick - old.tick)

    def wait_benefit_dt(self, old, new):
        old_wait_benefit = 0
        if old.in_storage > 0:
            old_wait_benefit = float(old.wait / old.in_storage)

        new_wait_benefit = 0
        if new.in_storage > 0:
            new_wait_benefit = float(new.wait / new.in_storage)

        time_delta =  new.tick - old.tick
        if time_delta > 0:
            return (new_wait_benefit - old_wait_benefit) / time_delta
        return 0

    def latency_cost_dt(self, old, new):
        old_latency_cost = float(old.latency / old.in_storage)

        new_latency_cost = float(new.latency / new.in_storage)

        time_delta =  new.tick - old.tick
        if time_delta > 0:
            return (new_latency_cost - old_latency_cost) / time_delta
        return 0

    def reaction(self):
        # Determine current wait time
        if self.waited_at is None and self.cnc < self.headroom:
            self.waited_at = self.tick
        elif self.waited_at is not None and self.cnc >= self.headroom:
            self.waited_at = None

        completed = self.pipeline['completed']
        consumed = self.pipeline['consumed']

        # Don't adjust unless we have a consumption
        self.adjust = completed.info['to_move'] > 0


        # Record idle time on each completed not consumed IO
        for io in completed:
            io.completion_time = getattr(io, "completion_time", self.tick)

        avg_total_latency = self.avg_real_io_latency(completed)

        if avg_total_latency is None or self.in_storage == 0:
            return

        latency_cost = float(avg_total_latency / self.in_storage)
        self.info['latency_cost']  = latency_cost

        new_latency_log = LatencyLogEntry(tick=self.tick, in_storage=self.in_storage, latency=avg_total_latency)

        if len(self.latency_log) > 1:
            self.info['latency_cost_dt'] = self.latency_cost_dt(self.latency_log[-1],
                                                    new_latency_log)

            self.info['latency_dt'] = self.latency_dt(self.latency_log[-1],
                                                      new_latency_log)

            self.info['in_storage_dt'] = self.in_storage_dt(self.latency_log[-1],
                                                      new_latency_log)

        self.latency_log.append(new_latency_log)
        if len(self.latency_log) > 1000 and self.printed == False:
            for entry in self.latency_log:
                print(entry)
            self.printed = True


    def next_action(self):
        return self.tick + 1 if self.adjust else super().next_action()

