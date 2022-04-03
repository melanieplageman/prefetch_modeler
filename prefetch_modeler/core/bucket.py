import collections.abc
import pandas as pd
import math
import warnings


LOG_BUCKETS = False
DEBUG = False


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

    def run(self, ios, duration=None):
        for io in ios:
            self.buckets[0].add(io)

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

            if len(self.buckets[-1]) == len(ios):
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

            if duration is not None and next_tick > duration.total:
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
        io.on_add(self)
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

    def run(self):
        to_move = self.to_move()
        self.tick_data['to_move'] = len(to_move)

        if len(to_move) and LOG_BUCKETS:
            print(f'{self.tick}: moving {len(to_move)} IOs from {self} to {self.target}')
        for io in to_move:
            self.remove(io)
            self.target.add(io)

        self.tick_data['num_ios'] = len(self)
