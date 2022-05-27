from prefetch_modeler.core import RateBucket, Rate
from dataclasses import dataclass
from fractions import Fraction
import itertools
import math
import statistics
from collections import OrderedDict


@dataclass
class LogItem:
    tick: int
    latency: float
    latency_rate: Fraction
    in_storage: int
    awd: int

from collections import namedtuple
Movement = namedtuple('MovementRecord', ['tick', 'number'])

class NewFetcher(RateBucket):
    name = 'newfetcher'
    kd = -Rate(per_second=5).value
    # ki = -0.00000004
    ki = -Rate(per_second=20).value * Rate(per_second=30).value
    k_lat = Rate(per_second=100).value
    # kd = 0

    def __init__(self, *args, **kwargs):
        self.storage_record = []
        self.num_periods = 4
        self.current_period = 0
        self.next_adjustment = 0
        self.raw_lookback = 4
        self.saved_rate = None
        self.log = [LogItem(tick=0, latency=0, latency_rate=0, in_storage=0,
                            awd=0)]
        self.inflight_scores = {}
        self.inflight_scores[0] = (0,0)
        super().__init__(*args, **kwargs)

    @property
    def period(self):
        return self.log[-1]

    @property
    def in_storage(self):
        return len(self.pipeline['minimum_latency']) + \
            len(self.pipeline['inflight']) + \
            len(self.pipeline['deadline'])

    @property
    def recent_gain(self):
        if self.period.latency == 0:
            return 0
        return float(self.period.in_storage / self.period.latency) * 0.3

    @property
    def adjustment(self):
        # return self.derivative_term
        adjustment = self.latency_rate * -self.recent_gain
        adjustment += self.period.awd * self.k_lat
        return adjustment
        # return self.derivative_term * -self.recent_gain
        # return self.derivative_term + self.integral_term

    # @property
    # def integral_term(self):
    #     return self.log[-1].latency * self.ki

    @property
    def latency_rate(self):
        if len(self.log) < 8:
            return 0

        period_newer = self.log[-1]
        period_older = self.log[-8]
        length = period_newer.tick - period_older.tick

        latency_rate_of_change = (period_newer.latency - period_older.latency) / length

        if latency_rate_of_change < 0:
            return 0
        return latency_rate_of_change

    @property
    def regression(self):
        if len(self.log) < 10:
            return 0

        data = self.log[-80:]
        data.sort(key=lambda item: item.in_storage)

        best_sse = None
        best_base_latency_estimate = None
        best_i = None
        for i in range(1, len(data)):
            partition1 = data[:i]
            mean_latency = statistics.fmean(item.latency for item in partition1)
            sse1 = sum(math.pow(item.latency - mean_latency, 2)
                       for item in partition1)
            base_latency_estimate = mean_latency

            partition2 = data[i:]
            try:
                slope, intercept = statistics.linear_regression(
                    list(item.in_storage for item in partition2),
                    list(item.latency for item in partition2))
            except statistics.StatisticsError:
                sse2 = 0
            else:
                sse2 = sum(
                    math.pow(slope * item.in_storage + intercept - item.latency, 2)
                    for item in partition2)

            if best_sse is None or sse1 + sse2 < best_sse:
                best_sse = sse1 + sse2
                best_base_latency_estimate = base_latency_estimate
                best_i = i

        # print(best_i)

        return best_base_latency_estimate


    @property
    def in_storage_rate(self):
        if len(self.log) < 3:
            return 0

        period_newer = self.log[-1]
        period_older = self.log[-3]
        length = period_newer.tick - period_older.tick

        in_storage_rate_of_change = (period_newer.in_storage - period_older.in_storage) / length
        return in_storage_rate_of_change

    @property
    def completion_inflight_ratio(self):
        return self.raw_completion_rate / self.in_storage_rate

    @property
    def integral_term(self):
        if len(self.log) < 2:
            return 0

        period_newer = self.log[-1]
        period_older = self.log[-2]
        length = period_newer.tick - period_older.tick

        latency_integral = (period_newer.latency - period_older.latency) * length
        lat = latency_integral * self.ki
        # print(lat)

        return lat


    @property
    def derivative_term(self):
        # adjustment = self.latency_rate * self.kd
        adjustment = self.latency_rate * -self.pipeline['remaining'].rate() * 1.3
        # adjustment = self.latency_rate * -self.pipeline['remaining'].rate()
        return adjustment

    @property
    def alt_adjustment(self):
        if len(self.log) < 3:
            return 0
        log = list(reversed(self.log))
        intervals = list(zip(log, log[1:]))

        total = 0
        num_periods = 0
        for newer_entry, older_entry in intervals:
            change_latency = newer_entry.latency - older_entry.latency
            period = newer_entry.tick - older_entry.tick
            total += Fraction(int(change_latency), period)
            num_periods += 1
            if num_periods > self.raw_lookback:
                break

        smoothed_lat_rate = Fraction(total, num_periods)
        if smoothed_lat_rate < 0:
            smoothed_lat_rate = 0

        return float(smoothed_lat_rate) * self.kd

    @property
    def raw_completion_rate(self):
        move_record = list(reversed(self.storage_record))
        intervals = list(zip(move_record, move_record[1:]))

        number_moved = 0
        time_elapsed = 0
        number_seen = 0
        for newer_movement, older_movement in intervals:
            number_moved += newer_movement.number
            # print(f'newer movement tick: {newer_movement.tick}. older movement tick: {older_movement.tick}')
            time_elapsed += newer_movement.tick - older_movement.tick
            number_seen += 1
            if number_seen > self.raw_lookback:
                break

        if time_elapsed == 0:
            raw_rate = 0
        else:
            raw_rate = Fraction(number_moved, time_elapsed)

        return raw_rate


    def rate(self):
        # if self.saved_rate == None:
        #     self.saved_rate = self.pipeline['remaining'].rate()

        adjustment = self.period.awd * self.k_lat
        return self.pipeline['remaining'].rate() + adjustment

        # self.saved_rate = self.saved_rate + self.adjustment
        # if self.saved_rate < 0:
        #     self.saved_rate = 0
        # return self.saved_rate

        # elif self.tick >= self.next_adjustment:
        #     self.saved_rate = self.saved_rate + self.adjustment
        #     self.next_adjustment = self.tick + self.period.latency

        if self.saved_rate < 0:
            self.saved_rate = 0

        return self.saved_rate

    def reaction(self):
        # In case the IO is moved immediately to the inflight bucket
        # for io in itertools.chain(self.pipeline['submitted'],
        #                           self.pipeline['inflight']):
        for io in itertools.chain(self.pipeline['minimum_latency'],
                                  self.pipeline['inflight'],
                                  self.pipeline['deadline']):
            if getattr(io, 'submitted', None) is not None:
                continue
            io.submitted = self.tick

        num_ios = 0
        total_latency = 0

        for io in self.pipeline['completed']:
            if getattr(io, 'completed', None) is None:
                io.completed = self.tick
            latency = io.completed - io.submitted
            total_latency += latency
            num_ios += 1

        # If consumption rate is fast enough, IOs might always be moved to
        # consumed right away, so we need to find them and count them
        for io in self.pipeline['consumed']:
            if getattr(io, 'completed', None) is not None:
                continue
            io.completed = self.tick
            latency = io.completed - io.submitted
            total_latency += latency
            num_ios += 1

        if num_ios == 0:
            return

        if self.pipeline['inflight'].info['to_move']:
            movement = Movement(self.tick,
                                self.pipeline['inflight'].info['to_move'])
            self.storage_record.append(movement)

        avg_latency = Fraction(total_latency, num_ios)
        # print(f'Tick: {self.tick}. avg_latency: {avg_latency}')

        # (latency, IOs): pfr
        latency_ios = {
            ('up', 'up') : 'high',
            ('up', 'down'): 'high',
            ('up', 'same'): 'high',
            ('down', 'up'): 'low',
            ('down', 'down'): 'high',
            ('down', 'same'): 'low',
            ('same', 'up'): 'low',
            ('same', 'down'): 'low',
            ('same', 'same'): 'high',
        }

        lat_direction = 'same'
        diff = avg_latency - self.period.latency
        print(f'diff: {diff}')
        # print(f'avg_latency {avg_latency} > previous latency {self.period.latency}')
        if avg_latency > (self.period.latency * 1.04):
            lat_direction = 'up'
        elif avg_latency < (self.period.latency * 0.95):
            lat_direction = 'down'

        inflight_direction = 'same'
        if self.in_storage > self.period.in_storage:
            inflight_direction = 'up'
        elif self.in_storage < self.period.in_storage:
            inflight_direction = 'down'


        # (hi, lo)
        if self.in_storage not in self.inflight_scores:
            self.inflight_scores[self.in_storage] = (0,0)
        his = self.inflight_scores[self.in_storage][0]
        los = self.inflight_scores[self.in_storage][1]
        if latency_ios[(lat_direction, inflight_direction)] == 'high':
            his += 1
        else:
            los += 1

        self.inflight_scores[self.in_storage] = (his, los)
        # print(self.inflight_scores)

        inflight_scores = OrderedDict(sorted(self.inflight_scores.items()))

        max_inflight = 0
        seen_lo = False
        for inflight, scores in inflight_scores.items():
            his = scores[0]
            los = scores[1]
            if his > los:
                max_inflight = inflight
                break

        inflights = list(inflight_scores.keys())
        for i, key in enumerate(inflights):
            if max_inflight == key:
                max_inflight = inflights[i - 1]
                break

        awd = max_inflight - self.in_storage

        # print(f'awd is {awd}')

        self.log.append(LogItem(tick=self.tick,
                                latency=float(avg_latency),
                                latency_rate=self.latency_rate,
                                in_storage=self.in_storage,
                                awd=awd))
