import threading, time, math, logging

# variables
COMPLETION_TARGET_DISTANCE = 512
MAX_IN_FLIGHT = 128
MIN_DISPATCH = 2

# How long it takes to submit 1 IO
BASE_SUBMISSION_LATENCY = 0.001

# How long it takes 1 IO to complete if only 1 is in flight
BASE_COMPLETION_LATENCY = 0.1

# Time between calls to next()
BASE_CONSUMPTION_LATENCY = 0.1

logger = logging.getLogger()
logger.setLevel('CRITICAL')

class AtomicCounter:
    def __init__(self, initial=0):
        self.value = initial
        self._lock = threading.Lock()

    def increment(self, num=1):
        with self._lock:
            self.value += num
            return self.value

    def increment_batch_get_batch_size(self, max_val, target_batch_size):
        with self._lock:
            batch_size = min(max_val - self.value, target_batch_size)
            logger.warning(f'Submitted: {self.value}. nblocks {max_val}, min_dispatch {target_batch_size}, batch size {batch_size}')
            self.value += batch_size
            logger.warning(f'incrementing submitted by {batch_size}')
            return batch_size

    def __repr__(self):
        return str(self.value)

class System:
    def __init__(self):
        self.base_submission_latency = BASE_SUBMISSION_LATENCY
        self.base_completion_latency = BASE_COMPLETION_LATENCY
        self.base_consumption_latency = BASE_CONSUMPTION_LATENCY
        self.min_dispatch = MIN_DISPATCH

        self.global_completed = AtomicCounter()
        self.global_consumed = AtomicCounter()
        self.global_submitted = AtomicCounter()

    # How long it takes 1 IO to complete when nrequests are in flight
    def completion_latency(self, nrequests):
        # TODO: fix this
        # distance in nrequests before a jump in latency
        # e.g. after 256 kB, request latency jumps because it must be spread
        # across multiple network requests in Azure
        # in nrequests
        d = 4
        # how much does latency jump by when it increases
        # in ms
        j = 0.02
        # slope of curve
        m = 0.1
        latency = (m * nrequests) + self.base_completion_latency + math.floor(nrequests/d) * j
        return self.base_completion_latency

    # How long it takes to submit batch_size # of requests
    def submission_latency(self, window):
        # TODO: fix this
        # variable_submission_cost = 0.0001
        # latency = self.base_submission_latency + (window *
        #                                           variable_submission_cost)
        return self.base_submission_latency

    # Fixed cost for time to wait between calls to next() on the scan
    def consumption_latency(self):
        return self.base_consumption_latency

    def window_size(self, nblocks, completed, in_flight):
        if completed >= COMPLETION_TARGET_DISTANCE - self.min_dispatch:
            logger.warning('No need to prefetch due to suffient completed IOs')
            return 0
        if in_flight >= MAX_IN_FLIGHT:
            logger.warning('Cannot prefetch because hit MAX_IN_FLIGHT')
            return 0

        window = self.global_submitted.increment_batch_get_batch_size(nblocks, self.min_dispatch)
        return window

    def run(self):
        ioq = IOQueue()
        nblocks = 10

        completion_worker = CompletionWorker(ioq, nblocks, self)

        completion_worker.start()
        ioq.submit_io()
        start = time.time()
        for i in Scan(ioq, nblocks, self):
            logger.warning(f'Acquired: {i}')
        end = time.time()
        logger.critical(f'Total Time: {end - start}')

class IOQueue:
    def __init__(self):
        self._completed = 0
        self._in_flight = 0

        self.cv = threading.Condition()

    @property
    def in_flight(self):
        with self.cv:
            return self._in_flight

    @property
    def completed(self):
        with self.cv:
            return self._completed

    def submit_io(self):
        with self.cv:
            self._in_flight += 1

            logger.warning(f'Submitted IO. In-flight {self._in_flight}. Available Completed: {self._completed}.')
            self.cv.notify_all()

    def complete_io(self):
        with self.cv:
            while self._in_flight < 1:
                logger.warning('No IO in flight to complete')
                self.cv.wait()

            self._in_flight -= 1
            self._completed += 1

            logger.warning(f'Completed IO. In-flight {self._in_flight}. Available Completed: {self._completed}')
            self.cv.notify_all()

    def get_io(self):
        with self.cv:
            while self._completed < 1:
                logger.warning('No IO completed yet to get')
                self.cv.wait()

            self._completed -= 1

            logger.warning(f'Acquired IO. In-flight {self._in_flight}. Available Completed: {self._completed}.')

class Scan:
    def __init__(self, ioq, nblocks, system):
        self.nblocks = nblocks
        self.ioq = ioq
        self.system = system
        self.local_acquired = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.system.global_consumed.increment() <= self.nblocks:
            # time.sleep(self.system.consumption_latency())
            self.ioq.get_io()
            self.local_acquired += 1
            self.prefetch()
            return self.local_acquired
        raise StopIteration

    def prefetch(self):
        window = self.system.window_size(self.nblocks, self.ioq.completed,
                                         self.ioq.in_flight)

        # time.sleep(self.system.submission_latency(window))

        while window > 0:
            self.ioq.submit_io()
            window -= 1

class CompletionWorker(threading.Thread):
    def __init__(self, ioq, nblocks, system):
        self.nblocks = nblocks
        self.ioq = ioq
        self.system = system
        # local completed counter will not equal nblocks if multiple threads
        # doing completions
        self.local_completed = 0
        super().__init__(name='completion worker')

    def run(self):
        while self.system.global_completed.increment() <= self.nblocks:
            time.sleep(self.system.completion_latency(self.ioq.in_flight))
            self.ioq.complete_io()
            self.local_completed += 1
            logger.warning(f'Completed: {self.local_completed}')

system = System()
system.run()
