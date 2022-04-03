from prefetch_modeler.core.bucket import IO
import pandas as pd

class Tracer(IO):
    def __init__(self, id):
        self.id = id
        self.trace_data = []
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
