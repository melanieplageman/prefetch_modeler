import pandas as pd


class IO:
    def on_add(self, bucket):
        """Called when the IO is added to the ``bucket``."""
        pass

    def on_discard(self, bucket):
        """Called when the IO is discarded from the ``bucket``."""
        pass


class Tracer(IO):
    """An `IO` that will record the tick of each transition to a bucket."""

    def __init__(self, id):
        self.id = id
        self._data = []

    @property
    def data(self):
        """Return the data stored in this tracer as a `DataFrame`."""
        data = pd.DataFrame(self._data)
        data['interval'] = -data['tick'].diff(-1).astype('Int64')
        return data

    def on_add(self, bucket):
        self._data.append({
            'io': self.id, 'bucket': bucket.name, 'tick': bucket.tick or 0,
        })
