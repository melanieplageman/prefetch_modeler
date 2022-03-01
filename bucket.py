import collections.abc
from dataclasses import dataclass
import itertools
import pandas as pd
import math

LOG_BUCKETS = False

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

    def run(self, workload):
        for i in range(workload.volume):
            self.buckets[0].add(IO())

        next_tick = 0
        last_tick = 0
        while True:
            for bucket in self.buckets:
                bucket.tick = next_tick

            for bucket in self.buckets:
                bucket.run()

            if len(self.buckets[-1]) == workload.volume:
                break

            next_tick = min([bucket.next_action() for bucket in self.buckets])

            if next_tick <= last_tick:
                raise ValueError(f'Next action tick request {next_tick} is older than last action tick {last_tick}.')
            last_tick = next_tick

            if workload.duration and next_tick > workload.duration:
                break

        return self.data

class Bucket(collections.abc.MutableSet):
    def __init__(self, name, pipeline):
        self.name = name
        self.pipeline = pipeline

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

    def next_action(self):
        return math.inf

    def adjust_before(self):
        # TODO: replace string 'adjust_before' with function name
        # introspection?
        adjust_func = self.pipeline.registry.get(self.__class__.__name__ + '.adjust_before')
        if adjust_func:
            return adjust_func(self)

    def adjust_after(self):
        # TODO: replace string 'adjust_after' with function name
        # introspection?
        adjust_func = self.pipeline.registry.get(self.__class__.__name__ + '.adjust_after')
        if adjust_func:
            return adjust_func(self)

    def run(self):
        self.adjust_before()
        self.tick_data['num_ios'] = len(self)

        to_move = self.to_move()
        self.tick_data['to_move'] = len(to_move)

        if len(to_move) and LOG_BUCKETS:
            print(f'{self.tick}: moving {len(to_move)} IOs from {self} to {self.target}')
        for io in to_move:
            self.remove(io)
            self.target.add(io)

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

    def next_action(self):
        if not self.source:
            return self.tick + 1
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
