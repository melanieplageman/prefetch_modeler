from bucket import IO

class Tracer(IO):
    def __init__(self, id):
        self.id = id
        self.trace_data = []

    def on_add(self, bucket):
        self.trace_data.append({'io': self.id, 'bucket': bucket.name, 'tick':
                                bucket.tick, 'op': 'add'})

    def on_discard(self, bucket):
        self.trace_data.append({'io': self.id, 'bucket': bucket.name, 'tick':
                                bucket.tick, 'op': 'discard'})

