def prefetcher_type(prefetch_num_ios_func, adjust_func, min_dispatch,
                    initial_completion_target_distance, cap_in_progress):
    if initial_completion_target_distance > cap_in_progress:
        raise ValueError(f'Value {initial_completion_target_distance} for ' f'completion_target_distance exceeds cap_in_progress ' f'value of {cap_in_progress}.')

    class remaining(GateBucket):
        """
        A bucket that will move the number of IOs specified by an algorithm, with
        the option of modifying the algorithm on each run.
        """

        def __init__(self, *args, **kwargs):
            self.cap_in_progress = cap_in_progress
            self.completion_target_distance = initial_completion_target_distance

        def min_dispatch(self):
            return min_dispatch

        def adjust(self):
            if adjust_func is not None:
                adjust_func(self)

        def wanted_move_size(self):
            return prefetch_num_ios_func(self)

        def run(self):
            if not self.tick == 0:
                self.adjust()
            super().run()
