import collections.abc
import itertools
import pandas as pd
import math
from override import Overrideable, overrideable


LOG_BUCKETS = False
DEBUG = False

class IO: pass


class Pipeline:
    template = []

    def __init__(self, *args):
        self.buckets = [bucket_type(name, self) for name, bucket_type in self.template]
        self.buckets.extend(args)

        for i in range(len(self.buckets) - 1):
            self.buckets[i].target = self.buckets[i + 1]

    def __getitem__(self, bucket_name):
        for bucket in self.buckets:
            if bucket.name == bucket_name:
                return bucket
        raise KeyError(repr(bucket_name))

    @classmethod
    def bucket(cls, name):
        """Define a bucket to be added to the pipeline on initialization."""
        def call(bucket_type):
            cls.template.append((name, bucket_type))
            return bucket_type
        return call

    @property
    def data(self):
        """Return the tick joined data of each bucket in the pipeline."""
        # TODO: Make a new dataframe with just the index column? So that we
        # don't have to special case the first bucket?
        data = self.buckets[0].data.add_prefix(f"{self.buckets[0].name}_")
        for bucket in self.buckets[1:]:
            data = data.join(bucket.data.add_prefix(f"{bucket.name}_"))
        return data

    def run(self, workload):
        for i in range(workload.volume):
            self.buckets[0].add(IO())

        next_tick = 0
        last_tick = 0
        while next_tick != math.inf:
            for bucket in self.buckets:
                bucket.tick = next_tick

            # Not possible to run some buckets and not others because one
            # bucket may move IOs into another bucket which affect whether or
            # not that bucket needs to run -- especially with infinity
            for bucket in self.buckets:
                bucket.run()

            if len(self.buckets[-1]) == workload.volume:
                break

            actionable = {
                bucket.name: bucket.next_action() for bucket in self.buckets
            }
            bucket_name, next_tick = min(
                actionable.items(),
                key=lambda item: item[1])

            if DEBUG and next_tick - last_tick == 1:
                print(last_tick, actionable)

            if next_tick <= last_tick:
                raise ValueError(f'Next action tick request {next_tick} (from {bucket_name}) is older than last action tick {last_tick}.')
            last_tick = next_tick

            if workload.duration and next_tick > workload.duration:
                break

        return self.data

    def override(self, name, function):
        bucket_name, function_name = name.split('.')

        for bucket in self.buckets:
            if bucket.name == bucket_name:
                break
        else:
            raise ValueError(f'No such bucket {bucket_name!r}')

        bucket.override[function_name] = function


class Bucket(Overrideable, collections.abc.MutableSet):
    def __init__(self, name, pipeline):
        self.name = name
        self.pipeline = pipeline

        self.source = set()
        self.target = self

        self.counter = 0

        self._tick = None

        self._data = []
        self.tick_data = None

        super().__init__()

    def __repr__(self):
        return f"{type(self).__name__}({self.name!r})"

    def __contains__(self, io):
        return io in self.source

    def __iter__(self):
        return iter(self.source)

    def __len__(self):
        return len(self.source)

    def add(self, io):
        self.counter += 1
        self.source.add(io)

    def discard(self, io):
        self.source.discard(io)

    @property
    def tick(self):
        return self._tick

    @tick.setter
    def tick(self, tick):
        self._tick = tick
        if self.tick_data is not None:
            self._data.append(self.tick_data)
        self.tick_data = {'tick': tick}

    @property
    def data(self):
        return pd.DataFrame(self._data + [self.tick_data]).set_index('tick')

    def to_move(self):
        raise NotImplementedError()

    def next_action(self):
        return math.inf

    @overrideable
    def adjust_before(self):
        pass

    @overrideable
    def adjust_after(self):
        pass

    @overrideable
    def run(self):
        self.adjust_before()

        to_move = self.to_move()
        self.tick_data['to_move'] = len(to_move)

        if len(to_move) and LOG_BUCKETS:
            print(f'{self.tick}: moving {len(to_move)} IOs from {self} to {self.target}')
        for io in to_move:
            self.remove(io)
            self.target.add(io)

        self.tick_data['num_ios'] = len(self)

        self.adjust_after()


class GateBucket(Bucket):
    """A bucket that will move a specified number of IOs."""

    MOVEMENT_SIZE = None

    def wanted_move_size(self):
        """The number of IOs to move on this tick."""

        if self.MOVEMENT_SIZE is not None:
            return self.MOVEMENT_SIZE
        raise NotImplementedError()

    def to_move(self):
        size = self.wanted_move_size()
        self.tick_data['want_to_move'] = size
        if size == math.inf:
            return frozenset(self.source)
        return frozenset(itertools.islice(self.source, size))


class FlipFlopGateBucket(GateBucket):
    """A bucket will either move a specified number of IOs or none of them."""

    def is_on(self):
        raise NotImplementedError()

    def wanted_move_size(self):
        return self.wanted_move_size() if self.is_on() else 0


class DialBucket(Bucket):
    """A bucket that will retain each IO for a specified amount of time."""

    def add(self, io):
        # In case self.tick is None
        io.move_at = (self.tick or 0) + self.latency()
        super().add(io)

    def discard(self, io):
        del io.move_at
        super().discard(io)

    @overrideable
    def latency(self):
        """The amount of time that an IO should be retained in this bucket."""
        raise NotImplementedError()

    def next_action(self):
        if not self.source:
            return math.inf
        return min(io.move_at for io in self.source if io.move_at)

    def to_move(self):
        return frozenset(io for io in self.source if io.move_at <= self.tick)


class IntakeBucket(Bucket):
    """A bucket that will move each IO on each tick."""

    def next_action(self):
        return self.tick + 1 if len(self.source) else math.inf

    def to_move(self):
        return frozenset(self.source)


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

    @overrideable
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


class CapacityBucket(GateBucket):
    """
    A bucket which moves as many IOs as possible without exceeding its target's
    capacity
    """

    @overrideable
    def capacity(self):
        # This expresses the capacity of the target bucket into which IOs are
        # moved by this bucket.
        raise NotImplementedError()

    def wanted_move_size(self):
        return max(self.capacity() - len(self.target), 0)

    def next_action(self):
        if not self.source:
            return math.inf

        if len(self.target) < self.capacity():
            return self.tick + 1

        return math.inf
