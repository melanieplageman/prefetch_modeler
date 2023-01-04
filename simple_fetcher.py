from collections.abc import Sequence
from dataclasses import dataclass
from periodic_fetcher import ConstantDistancePrefetcher
import itertools
from numpy import mean

@dataclass
class CompletionLogEntry:
    tick: int
    number: int


class SimpleFetcher(ConstantDistancePrefetcher):
    lo = 2
    hi = 4
    prefetch_distance = lo

    def __init__(self, *args, **kwargs):
        self.consume_log = []
        self.tput_denom = 100000
        self.completion_log = []
        super().__init__(*args, **kwargs)

    def remove(self, io):
        io.prefetch_distance = self.prefetch_distance
        if not getattr(io, "cached", False):
            io.submission_time = self.tick

        return super().remove(io)

    @property
    def completed(self):
        return len(self.pipeline['completed'])

    @property
    def completion_rate(self):
        if not self.completion_log:
            return None
        from itertools import takewhile
        newlog = takewhile(
            lambda entry: entry.tick > self.tick - self.tput_denom,
            reversed(self.completion_log)
        )

        nios = sum(entry.number for entry in newlog)
        cr = float(nios / self.tput_denom)
        return cr

    def on_complete(self, io):
        if not getattr(io, "cached", False):
            io.completion_time = self.tick
        self.completion_log.append(CompletionLogEntry(tick=self.tick, number=1))

    @property
    def in_storage(self):
        return len(self.pipeline['minimum_latency']) + \
            len(self.pipeline['inflight']) + \
            len(self.pipeline['deadline'])

    @property
    def change(self):
        return 1

    def on_consume(self, io):
        """Called each time an IO is consumed"""

        self.consume_log.append(io)

        # TODO: Consider using this instead
        # import math
        # change = 1 / max(1, math.sqrt(self.in_storage))

        if self.completed <= self.lo:
            self.prefetch_distance += self.change

        if self.completed >= self.hi:
            self.prefetch_distance = max(1, self.prefetch_distance - self.change)

    def reaction(self):
        for io in self.pipeline['completed'].info['actual_to_move']:
            self.on_consume(io)

        for io in itertools.chain(self.pipeline['completed'], self.pipeline['completed']):
            # Ensure that we don't account an IO more than once
            if getattr(io, "accounted", None) is not None:
                continue
            io.accounted = True

            self.on_complete(io)

        self.info['completion_rate'] = self.completion_rate


class ClampFetcher(SimpleFetcher):
    def __init__(self, *args, **kwargs):
        self.maximum = None
        self.pfd_at_flip = 0
        self.consecutive_ups = 0
        self.consecutive_downs = 0
        super().__init__(*args, **kwargs)

    def on_consume(self, io):
        super().on_consume(io)

        if getattr(io, "cached", False):
            return

        ios = list(reversed(self.consume_log))[:int(self.prefetch_distance)]

        io_tput = io.prefetch_distance / (io.completion_time - io.submission_time)
        avg_tput = mean([cur.prefetch_distance / (cur.completion_time -
            cur.submission_time) for cur in ios if not getattr(cur, "cached",
                False)])
        tput_ratio = io_tput / avg_tput

        avg_pfd = mean([cur.prefetch_distance for cur in ios if not
            getattr(cur, 'cached', False)])
        max_pfd = max([cur.prefetch_distance for cur in ios if not
            getattr(cur, "cached", False)])

        # if pfd is going up and tput is going up this is a possible value to
        # roll back to
        if io.prefetch_distance > avg_pfd and tput_ratio >= 1.1:
            self.pfd_at_flip = io.prefetch_distance
            self.consecutive_ups += 1
            self.consecutive_downs = 0
            # If we have had enough ups in a row, invalidate our maximum for
            # now
            if self.consecutive_ups >= avg_pfd:
                self.maximum = None

            # if we have previously set a maximum and we haven't had enough ups
            # in a row, anyway increase the max so that we are able to continue
            # increasing the prefetch distance
            if self.maximum:
                self.maximum += 1

        # if pfd is going up and tput is not going up
        elif io.prefetch_distance > avg_pfd and tput_ratio < 1.1:
            self.consecutive_downs += 1
            self.consecutive_ups = 0
            # If we have had enough downs in a row, set a maximum and set pfd
            # to a bit less than what it was last time we had an increase in
            # tput
            if self.consecutive_downs >= avg_pfd:
                # Set maximum to recent max pfd out of consumed IOs. At this
                # point, self.prefetch_distance could be higher but we know
                # that just out of consumed IOs, a pfd that high wasn't helping
                # us. It may be necessary to actually set this to
                # self.prefetch_distance to avoid getting stuck at too low of a
                # value, though
                self.maximum = max_pfd
                self.prefetch_distance = self.pfd_at_flip * 0.7
            # counteract the increase from algo 1 even if we haven't had enuogh
            # consecutive downs
            self.prefetch_distance -= 1

        self.prefetch_distance = max(self.prefetch_distance, 1)
        if self.maximum:
            self.prefetch_distance = min(self.maximum, self.prefetch_distance)

        avg_pfd_w_cached = mean([cur.prefetch_distance for cur in ios])
        max_pfd_w_cached = max([cur.prefetch_distance for cur in ios])

        io_pfd_str = f"{format(io.prefetch_distance, '.2f')}"
        io_tput_str = f"{format(io_tput, '.2f')}"
        avg_tput_str = f"{format(avg_tput, '.2f')}"
        avg_pfd_str = f"{format(avg_pfd, '.2f')}"
        avg_pfd_w_cached_str = f"{format(avg_pfd_w_cached, '.2f')}"
        tput_ratio_str = f"{format(tput_ratio, '.2f')}"
        max_str = f"{format(self.maximum, '.2f')}" if self.maximum else ''
        #print(f"io pfd: {io_pfd_str}. avg pfd: {avg_pfd_str}. avg_pfd_w_cached: {avg_pfd_w_cached_str}. tput ratio: {tput_ratio_str}. max: {max_str}. pfd at flip: {format(self.pfd_at_flip, '.2f')}")
