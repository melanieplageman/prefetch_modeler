import itertools
import math
from prefetch_modeler.core.bucket import Bucket


class GateBucket(Bucket):
    """A bucket that will move a specified number of IOs."""

    def wanted_move_size(self):
        """The number of IOs to move on this tick."""
        raise NotImplementedError()

    def to_move(self):
        size = self.wanted_move_size()
        self.tick_data['want_to_move'] = size
        if size == math.inf:
            return frozenset(self.source)
        self.tick_data['wait'] = size > len(self)
        return frozenset(itertools.islice(self.source, size))


class DialBucket(Bucket):
    """A bucket that will retain each IO for a specified amount of time."""

    def add(self, io):
        # In case self.tick is None
        io.move_at = (self.tick or 0) + self.latency()
        super().add(io)

    def discard(self, io):
        del io.move_at
        super().discard(io)

    def latency(self):
        """The amount of time that an IO should be retained in this bucket."""
        raise NotImplementedError()

    def next_action(self):
        if not self.source:
            return math.inf
        return min(io.move_at for io in self.source if io.move_at)

    def to_move(self):
        return frozenset(io for io in self.source if io.move_at <= self.tick)


class ContinueBucket(Bucket):
    """A bucket that will move all its available IOs on each tick."""

    def to_move(self):
        return frozenset(self.source)

    def next_action(self):
        return self.tick + 1 if self.source else math.inf


class StopBucket(Bucket):
    """An immobile bucket."""

    def to_move(self):
        return frozenset()


class RateBucket(Bucket):
    def __init__(self, *args, **kwargs):
        self._rate = self.rate()
        self.volume = self.maximum_volume
        self.last_tick = 0
        super().__init__(*args, **kwargs)

    def rate(self):
        """The rate that the bucket should operate on."""
        raise NotImplementedError()

    @property
    def maximum_volume(self):
        return math.ceil(math.ceil(self._rate) / self._rate) * self._rate

    def to_move(self):
        self.volume += (self.tick - self.last_tick) * self._rate
        self.volume = min(self.volume, self.maximum_volume)

        moveable = max(math.floor(self.volume), 0)

        self.tick_data['want_to_move'] = moveable
        self.tick_data['wait'] = moveable > len(self.source)

        result = frozenset(itertools.islice(self.source, moveable))
        self.volume -= len(result)

        self.tick_data['rate'] = float(self.rate())

        self.last_tick, self._rate = self.tick, self.rate()

        return result

    def next_action(self):
        if not self.source:
            if self.volume >= self.maximum_volume:
                return math.inf
            interval = (self.maximum_volume - self.volume) / self._rate
            return self.tick + math.ceil(interval)

        if self.volume >= 1:
            return self.tick + 1

        # The next tick that an IO will be moveable
        interval = (1 - self.volume) / self._rate
        return self.tick + math.ceil(interval)


class ThresholdBucket(Bucket):
    """
    A bucket which moves all IOs once a threshold is met.
    """
    def threshold(self):
        raise NotImplementedError()

    def to_move(self):
        if len(self) < self.threshold():
            return frozenset()
        return frozenset(self.source)

    def next_action(self):
        if len(self) >= self.threshold():
            return self.tick + 1
        return math.inf


class CapacityBucket(GateBucket):
    """
    A bucket which moves as many IOs as possible, given slack it is interested
    in
    """
    def slack(self):
        raise NotImplementedError()

    def wanted_move_size(self):
        return min(len(self), self.slack())

    def next_action(self):
        if not self.source:
            return math.inf

        if self.slack() > 0:
            return self.tick + 1

        return math.inf


class TargetCapacityBucket(CapacityBucket):
    """
    A bucket which moves as many IOs as possible without exceeding its target's
    capacity
    """
    def target_capacity(self):
        # This expresses the capacity of the target bucket into which IOs are
        # moved by this bucket.
        raise NotImplementedError()

    def slack(self):
        # Target num_ios should not exceed target capacity
        return max(self.target_capacity() - len(self.target), 0)


class GlobalCapacityBucket(CapacityBucket):
    """
    A bucket which moves all its IOs to a max of system slack
    """
    def max_buffers(self):
        raise NotImplementedError()

    def slack(self):
        # This is in_progress from the perspective of this bucket
        # That is, all the IOs that it has seen so far minus the number of IOs
        # the client has consumed
        in_progress = self.target.counter - len(self.pipeline['consumed'])
        # In_progress shouldn't exceed max_buffers
        return max(self.max_buffers() - in_progress, 0)
