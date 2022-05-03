class Module:
    template = []

    def __init__(self, *args):
        self.buckets = [bucket_type(name, self) for name, bucket_type in self.template]
        self.buckets.extend(args)

        for i in range(len(self.buckets) - 1):
            self.buckets[i].target = self.buckets[i + 1]
        self.buckets[-1].target = self

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

    def run(self):

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
