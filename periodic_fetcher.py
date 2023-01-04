from dataclasses import dataclass
from itertools import takewhile
from prefetch_modeler.core import ContinueBucket, GlobalCapacityBucket, RateBucket, Rate, Duration, ForkBucket, Bucket, GateBucket
from prefetch_modeler.prefetcher_type import ConstantRatePrefetcher

class ConstantDistancePrefetcher(GlobalCapacityBucket):
    name = 'cd_fetcher'
    prefetch_distance = 100

    @classmethod
    def hint(cls):
        return (1, cls.__name__)

    def max_buffers(self):
        return int(self.prefetch_distance)

@dataclass
class ChangeLogEntry:
    change_id: int
    tick: int
    prefetch_distance: int
    consumed: int
    throughput_interval_start: int
    throughput_interval_end: int
    wait_time: int
    idle_time: int
    prefetch_ratio: float
    tput_ratio: float
    max_idle_time: int
    max_wait: int
    times_waited: int

class PeriodicFetcher(ConstantDistancePrefetcher):
    headroom = 2
    target_idle_time = 2000

    def __init__(self, *args, **kwargs):
        self.last_tput = None

        self.waited_at = None
        self.last_adjusted = 0
        self.adjust = False

        self.change_id = 0

        self.change_log = [ChangeLogEntry(change_id=self.change_id, tick=0,
            prefetch_distance=8, consumed=0,
            throughput_interval_start=0, throughput_interval_end=0,
            wait_time=0, idle_time=0, prefetch_ratio=0, tput_ratio=0,
            max_idle_time=0, max_wait=0, times_waited=0)]

        super().__init__(*args, **kwargs)

    def remove(self, io):
        if not getattr(io, "cached", False):
            io.submission_time = self.tick
            io.change_id = self.change_id
        return super().remove(io)

    @property
    def completion_rate(self):
        for entry in reversed(self.change_log):
            if entry.consumed > entry.prefetch_distance:
                return entry.consumed / (entry.throughput_interval_end - entry.throughput_interval_start)
        return None

    @property
    def prefetch_distance(self):
        return self.latest.prefetch_distance

    def max_buffers(self):
        if self.adjust is False:
            return int(self.latest.prefetch_distance)
        self.adjust = False

        new_pfd = self.latest.prefetch_distance

        if self.waited_at:
            time_just_waited = self.tick - self.waited_at
            self.latest.times_waited += 1
            if time_just_waited > self.latest.max_wait:
                self.latest.max_wait = time_just_waited
            # CAUTION: if you don't append a new entry to the change log, then you
            # will double count this wait time once the wait time ends -- and
            # maybe even triple count if you come here again before the period
            # ends -- shouldn't be a problem now tho
            self.latest.wait_time += time_just_waited

        wait_time = self.latest.wait_time
        # We shouldn't have had to wait for the cumulative wait time
        # (self.latest.wait_time) but we also wanted extra headroom # CNC IOs
        # and we can assume we would have to wait max or avg wait time for each
        # one
        avg_wait_time = self.latest.wait_time / self.latest.times_waited if self.latest.times_waited > 0 else 0
        # wait_time += ((self.headroom - 1) * self.latest.max_wait)
        wait_time += ((self.headroom - 1) * avg_wait_time)
        self.info['wait_time'] = wait_time

        # idle_time = self.latest.idle_time
        if len(self.pipeline['completed']) > self.headroom:
            for io in self.pipeline['completed']:
                # If the IO was completed on this tick, then it hasn't been
                # stamped with its completion time
                idle_time = self.tick - getattr(io, "completion_time", self.tick)
                idle_time = max(idle_time - self.target_idle_time, 0)
                # Cap the excess idle time to make sure that it doesn't begin
                # before this window began
                if self.tick - idle_time < self.latest.tick:
                    idle_time = self.tick - self.latest.tick
                self.latest.idle_time += idle_time
            self.info['idle_time'] = self.latest.idle_time

        idle_time = self.latest.idle_time

        # idle_time = self.latest.max_idle_time
        # idle_time -= self.headroom * self.target_idle_time
        # self.info['idle_time'] = idle_time

        # This is the effective wait time. If it's negative, then we idled
        delta = wait_time - idle_time

        # How many IOs were we off by?
        time_since_last_adjustment = self.tick - self.last_adjusted
        adjustment = delta / (time_since_last_adjustment * 0.3)
        new_pfd += adjustment

        total_time = self.tick - self.latest.tick
        if wait_time < total_time:
            wait_time_ratio = total_time / (total_time - wait_time)
        else:
            wait_time_ratio = 2

        if idle_time < total_time:
            idle_time_ratio = (total_time - idle_time) / total_time
        else:
            idle_time_ratio = 0.5

        ratio = wait_time_ratio * idle_time_ratio

        if ratio > 2:
            ratio = 2
        if ratio < 0.5:
            ratio = 0.5
        new_pfd = self.latest.prefetch_distance * ratio

        suggested_new_pfd = new_pfd

        if self.latest.consumed < int(self.latest.prefetch_distance):
            raise ValueError(f"should only adjust after consuming current pfd # IOs. current pfd: {self.latest.prefetch_distance} or {self.latest.prefetch_distance}. # consumed: {self.latest.consumed}")

        # TODO: Need to handle pfd 0 and 1 (to handle 1 need to consider
        # changing duration)
        new_pfd = max(new_pfd, 2)

        # if new_pfd != self.latest.prefetch_distance:
        print(f"tick: {self.tick}. old pfd: {format(self.latest.prefetch_distance, '.2f')}. new pfd: {format(new_pfd, '.2f')}. wait_time: {format(wait_time, '.2f')}. idle_time: {idle_time}. delta: {delta}. suggested_new_pfd: {format(suggested_new_pfd, '.2f')}. time since last adjustment: {time_since_last_adjustment}. adjustment: {format(adjustment, '.2f')}")
        self.change_id += 1
        self.change_log.append(ChangeLogEntry(
            change_id=self.change_id,
            tick=self.tick,
            prefetch_distance=new_pfd,
            consumed=0,
            throughput_interval_start=0, throughput_interval_end=0,
            wait_time=0, idle_time=0, prefetch_ratio=0, tput_ratio=0,
            max_idle_time=0, max_wait=0, times_waited=0))

        if self.waited_at is not None:
            self.waited_at = self.tick

        self.last_adjusted = self.tick

        return int(self.latest.prefetch_distance)

    @property
    def cnc(self):
        return len(self.pipeline['completed'])

    @property
    def latest(self):
        return self.change_log[-1]

    def reaction(self):
        # Set self.waited_at if we just started a wait. If we just concluded a
        # wait, then add that wait time to the latest changelog.
        if self.waited_at is None and self.cnc < self.headroom:
            self.waited_at = self.tick
        elif self.waited_at is not None and self.cnc >= self.headroom:
            wait_time = self.tick - self.waited_at
            self.latest.wait_time += wait_time
            if wait_time > self.latest.max_wait:
                self.latest.max_wait = wait_time
            self.latest.times_waited += 1
            self.waited_at = None

        if self.waited_at is not None:
            self.info['wait_time'] = (self.latest.wait_time + self.tick - self.waited_at) * self.headroom
        else:
            self.info['wait_time'] = self.latest.wait_time * self.headroom

        # Record the completion time of each completed IO
        for io in self.pipeline['completed']:
            io.completion_time = getattr(io, "completion_time", self.tick)
            # if len(self.pipeline['completed']) > self.headroom:
            if self.tick - io.completion_time > self.latest.max_idle_time:
                self.latest.max_idle_time = self.tick - io.completion_time

        # self.info['idle_time'] = self.latest.max_idle_time

        for io in self.pipeline['consumed']:
            # Ensure that we don't account an IO more than once
            if getattr(io, "accounted", None) is not None:
                continue
            io.accounted = True

            # When an IO is consumed, add its idle time to the latest changelog
            if len(self.pipeline['completed']) > self.headroom:
                # If the IO was completed on this tick, then it hasn't been
                # stamped with its completion time
                idle_time = self.tick - getattr(io, "completion_time", self.tick)
                idle_time = max(idle_time - self.target_idle_time, 0)
                # Cap the excess idle time to make sure that it doesn't begin
                # before this window began
                if self.tick - idle_time < self.latest.tick:
                    idle_time = self.tick - self.latest.tick
                self.latest.idle_time += idle_time
                self.info['idle_time'] = self.latest.idle_time

            if getattr(io, "cached", None) is not None:
                continue

            if io.change_id != self.latest.change_id:
                continue
            if self.latest.consumed == 0:
                self.latest.throughput_interval_start = io.submission_time
            self.latest.consumed += 1
            if self.latest.consumed >= int(self.latest.prefetch_distance):
                self.latest.throughput_interval_end = io.completion_time

        nconsumed = self.pipeline['completed'].info['to_move']

        # Don't adjust unless threshhold has been met
        if nconsumed > 0 and self.latest.consumed >= int(self.latest.prefetch_distance):
            duration = self.latest.throughput_interval_end - self.latest.throughput_interval_start
            consumed = self.latest.consumed
            pfd = self.latest.prefetch_distance
            self.last_tput = consumed / duration
            # print(f'tick: {self.tick}. pfd: {pfd}. duration: {duration}. consumed: {consumed}. tput: {consumed / duration * 1000}')
            self.adjust = True

        self.info['throughput'] = self.last_tput

    def next_action(self):
        return self.tick + 1 if self.adjust else super().next_action()
