import collections.abc
from dataclasses import dataclass
import itertools
import pandas as pd


class IO: pass


class Pipeline:
    def __init__(self, *args):
        self.buckets = list(args)

        for i in range(len(self.buckets) - 1):
            self.buckets[i].target = self.buckets[i + 1]

    @property
    def data(self):
        """Return the tick joined data of each bucket in the pipeline."""
        data = self.buckets[0].data.add_prefix(self.buckets[0].name)
        for bucket in self.buckets[1:]:
            data = data.join(bucket.data.add_prefix(f"{bucket.name}_"))
        return data

    def run(self, volume, duration=None):
        for i in range(volume):
            self.buckets[0].add(IO())

        # Create an inifite series if duration is None
        timeline = range(duration) if duration else itertools.count()

        for tick in timeline:
            if len(self.buckets[-1]) == volume:
                break

            for bucket in self.buckets:
                bucket.tick = tick

            for bucket in self.buckets:
                bucket.run()

        return self.data


class Bucket(collections.abc.MutableSet):
    def __init__(self, name):
        self.name = name

        self.source = set()
        self.target = self

        self.counter = 0

        self._tick = None

        self._data = []
        self.tick_data = None

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

    def run(self):
        self.tick_data['num_ios'] = len(self)

        to_move = self.to_move()
        self.tick_data['to_move'] = len(to_move)

        for io in to_move:
            self.remove(io)
            self.target.add(io)


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
        return frozenset(itertools.islice(self.source, size))


class FlipFlopGateBucket(GateBucket):
    """A bucket will either move a specified number of IOs or none of them."""

    def is_on(self):
        raise NotImplementedError()

    def wanted_move_size(self):
        return self.wanted_move_size() if self.is_on() else 0


class DialBucket(Bucket):
    """A bucket that will retain each IO for a specified amount of time."""

    LATENCY = None

    def add(self, io):
        # In case self.tick is None
        io.move_at = (self.tick or 0) + self.latency()
        super().add(io)

    def discard(self, io):
        del io.move_at
        super().discard(io)

    def latency(self):
        """The amount of time that an IO should be retained in this bucket."""

        if self.LATENCY is not None:
            return self.LATENCY
        raise NotImplementedError()

    def to_move(self):
        return frozenset(io for io in self.source if io.move_at <= self.tick)


class IntakeBucket(Bucket):
    """A bucket that will move each IO on each tick."""

    def to_move(self):
        return frozenset(self.source)


class StopBucket(Bucket):
    """An immobile bucket."""

    def to_move(self):
        return frozenset()
