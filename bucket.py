import collections.abc
from dataclasses import dataclass
import logging

@dataclass
class IO:
    expiry: int = 0

    def is_expired(self, current_tick):
        return self.expiry <= current_tick

class Measurer(collections.abc.MutableMapping):
    def __init__(self):
        self._tick_data = None
        self._data = []

    def __getitem__(self, metric):
        return self._tick_data[metric]

    def __setitem__(self, metric, number):
        self._tick_data[metric] = number

    def __delitem__(self, metric):
        del self._tick_data[metric]

    def __iter__(self):
        return iter(self._tick_data)

    def __len__(self):
        return len(self._tick_data)

    @property
    def data(self):
        return self._data + [self._tick_data]

    def next_tick(self, tick):
        if self._tick_data is not None:
            self._data.append(self._tick_data)
        self._tick_data = {'tick': tick}


class Bucket:
    def __init__(self):
        self.measurer = Measurer()
        self.ios = []
        self.target_bucket = None

    def __repr__(self):
        return f"{type(self).__name__}()"

    @property
    def num_ios(self):
        return len(self.ios)

    def latency(self):
        return 0

    def desired_move_size(self, tick):
        return self.num_ios

    def add(self, io, tick):
        io.expiry = tick + self.latency()
        self.ios.append(io)

    def move_many(self, ios_to_move, tick):
        for io in ios_to_move:
            self.add(io, tick)

    def split_to_move(self, want_to_move, tick):
        return []

    def run(self, tick):
        want_to_move = self.desired_move_size(tick)
        ios_to_move = self.split_to_move(want_to_move, tick)

        if self.target_bucket:
            self.target_bucket.move_many(ios_to_move, tick)

        self.measurer.next_tick(tick)
        self.measurer['to_move'] = len(ios_to_move)
        self.measurer['want_to_move'] = want_to_move
        self.measurer['num_ios'] = self.num_ios

        logging.info(f'ran {type(self).__name__}. to_move: {len(ios_to_move)}. num ios: {self.num_ios}. want to move: {want_to_move}.')

    @property
    def data(self):
        return self.measurer.data

class GateBucket(Bucket):
    def split_to_move(self, want_to_move, tick):
        to_move = min(self.num_ios, want_to_move)
        ios_to_move = self.ios[0:to_move]
        del self.ios[0:to_move]
        return ios_to_move

class DialBucket(Bucket):
    def split_to_move(self, want_to_move, tick):
        ios_to_move = []

        for io in self.ios:
            if not io.is_expired(tick):
                continue
            self.ios.remove(io)
            ios_to_move.append(io)

        return ios_to_move
