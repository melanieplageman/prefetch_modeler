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
    """
    A bucket that will move a number of IOs at a specified rate or interval.

    The rate is specified as a `Fraction` such that the bucket will move a
    number of IOs as represented by the numerator each interval as represented
    by the denominator. For example, with a rate of 2 / 3, 2 IOs are moved
    every 3 ticks.

    Each IO is "independent", in that each number between 1 and the numerator
    is considered a separate "slot". In the earlier example, if, on a
    particular tick, the bucket is only able to move 1 IO, then an incoming IO
    is eligible to move immediately on the subsequent tick (rather than in 3
    ticks) as that "slot" is unoccupied.
    """

    def __init__(self, *args, **kwargs):
        self.slot = []
        self._movement_size = None
        self.interval = None
        super().__init__(*args, **kwargs)

    def rate(self):
        """The rate that the bucket should operate on."""
        raise NotImplementedError()

    @property
    def movement_size(self):
        return self._movement_size

    @movement_size.setter
    def movement_size(self, movement_size):
        # Prepend slots with tick zero when the movement size is increased
        if movement_size > len(self.slot):
            self.slot = [0] * (movement_size - len(self.slot)) + self.slot
        self._movement_size = movement_size

    def to_move(self):
        rate = self.rate()
        # if (rate := self.rate()) < 0:
        #     raise ValueError(f"Bucket rate {rate} can't be negative")
        self.movement_size, self.interval = rate.as_integer_ratio()

        # Only look at the slots up to self.movement_size! We never reduce the
        # length of self.slot. This will be the list of indices into self.slot
        # that represent slots that are usable (self.interval has elapsed since
        # that slot's last activation).
        usable = []
        for i in range(self.movement_size):
            if self.slot[i] + self.interval > self.tick:
                break
            usable.append(i)

        self.tick_data['want_to_move'] = len(usable)
        self.tick_data['wait'] = len(usable) > len(self)
        result = frozenset(itertools.islice(self.source, len(usable)))

        # Only update the slot if it's used. Then ensure that the slot list is
        # sorted.
        for i in usable[:len(result)]:
            self.slot[i] = self.tick
        self.slot.sort()

        return result

    def next_action(self):
        if self.movement_size == 0:
            return math.inf

        # We always need to take action on the next consumption. Otherwise,
        # wait isn't recorded for this bucket.
        if not self.source and self.slot[0] + self.interval < self.tick:
            return math.inf

        # self._last_consumption + self._consumption_interval is the next tick
        # that this bucket should consume, if this bucket has been full since
        # the last time it consumed.
        return max(self.slot[0] + self.interval, self.tick + 1)


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
