
class Window(Sequence):
    def __init__(self, change_id, tick, distance):
        self.change_id = change_id
        self.open_tick = tick
        self.distance = distance
        self.ios = []
        self.cached_ios_submitted = 0
        self.cached_ios_consumed = 0
        self.closing_tput = 0

    def __getitem__(self, i):
        return self.ios[i]

    def __len__(self):
        return len(self.ios)

    def append(self, io):
        self.ios.append(io)

    @property
    def is_done(self):
        """Whether this window is 'done'"""
        return len(self) >= self.distance

    @property
    def mean_distance(self):
        return sum(io.prefetch_distance for io in self) / len(self)

    @property
    def throughput(self):
        """The throughput of the window (as calculated from last_io)"""
        if not self.ios:
            return None
        return len(self) / (self[-1].completion_time - self.open_tick)

    @property
    def throughput_w_cached(self):
        if not self.ios:
            return None
        duration = self[-1].completion_time - self.open_tick
        return (len(self) + self.cached_ios_consumed) / duration
        # return len(self) + self.cached_ios_consumed / duration
        # return len(self) + (len(self) + self.cached_ios_consumed) / duration


class WindowedSimpleFetcher(SimpleFetcher):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.waited_at = None
        self.headroom = 2

        self.consume_log = []
        self.log = [Window(0, 0, self.prefetch_distance)]
        self.maximum = None

    @property
    def window(self):
        return self.log[-1]

    @property
    def throughput(self):
        for window in reversed(self.log):
            if window.is_done:
                return window.throughput

    @property
    def throughput_w_cached(self):
        for window in reversed(self.log):
            if window.is_done:
                return window.throughput_w_cached


    def remove(self, io):
        if not getattr(io, "cached", False):
            io.change_id = self.window.change_id
            io.prefetch_distance = self.prefetch_distance
            io.submission_time = self.tick
        else:
            self.window.cached_ios_submitted += 1
        return super().remove(io)

    def alt_through(pipeline):
        prefetch_distance = self.prefetch_distance
        if not self.consume_log:
            return None
        ios = list(reversed(self.consume_log))[:int(prefetch_distance)]
        return mean([io.prefetch_distance / (io.completion_time - io.submission_time) for io in ios])

    # @property
    # def change(self):
    #     if self.maximum is None:
    #         return 1
    #     return 1 / self.maximum

    def on_consume(self, io):
        super().on_consume(io)

        # if self.maximum is not None and self.prefetch_distance > self.maximum:
        #     self.prefetch_distance = self.maximum

        if len(self.consume_log) > 1:
            ios = list(reversed(self.consume_log))[:int(self.prefetch_distance)]
            avg_tput = mean([cur.prefetch_distance / (cur.completion_time - cur.submission_time) for cur in ios])
            io_tput = io.prefetch_distance / (io.completion_time - io.submission_time)
            avg_pfd = mean([cur.prefetch_distance for cur in ios])
            tput_ratio = io_tput / avg_tput
            pfd_ratio = io.prefetch_distance / avg_tput
            if pfd_ratio > 1 and tput_ratio < 1:
               self.prefetch_distance -= 1

            io_pfd_str = f"{format(io.prefetch_distance, '.2f')}"
            io_tput_str = f"{format(io_tput, '.2f')}"
            avg_tput_str = f"{format(avg_tput, '.2f')}"
            avg_pfd_str = f"{format(avg_pfd, '.2f')}"
            tput_ratio_str = f"{format(tput_ratio, '.2f')}"
            print(f"io pfd: {io_pfd_str}. io tput: {io_tput_str}. avg pfd: {avg_pfd_str}. avg tput: {avg_tput_str}. tput ratio: {tput_ratio_str}")

        if getattr(io, "cached", False):
            self.window.cached_ios_consumed += 1
            return

        if io.change_id != self.window.change_id:
            return

        self.window.append(io)

        if self.window.is_done:
            self.maximum = None

            if len(self.log) > 1:
                previous_window, this_window = self.log[-2], self.log[-1]

                previous_mean_distance = previous_window.mean_distance
                this_mean_distance = this_window.mean_distance

                previous_throughput = previous_window.throughput
                this_throughput = this_window.throughput

                previous_tput_w_cached = previous_window.throughput_w_cached
                this_tput_w_cached = this_window.throughput_w_cached

                # if this_mean_distance > previous_mean_distance:
                distance_ratio = this_mean_distance / previous_mean_distance
                throughput_ratio = this_throughput / previous_throughput
                tput_w_cached_ratio = this_tput_w_cached / previous_tput_w_cached
                # ratios = f"distance ratio: {format(distance_ratio, '.2f')}. throughput_ratio: {format(throughput_ratio, '.2f')}. "
                ratios = f"distance ratio: {format(distance_ratio, '.2f')}. w_cache_throughput_ratio: {format(tput_w_cached_ratio, '.2f')}. "

                this_max_distance = self.prefetch_distance

                # if this_max_distance > previous_window.distance and throughput_ratio < 1:
                # if tput_w_cached_ratio < distance_ratio:
                # if tput_w_cached_ratio < distance_ratio:
                # if throughput_ratio < distance_ratio * 0.9:
                # if distance_ratio > 1 and throughput_ratio < 1:
                # if distance_ratio > 1 and throughput_ratio < distance_ratio:
                # if distance_ratio > 1 and this_throughput <= previous_throughput:
                    # self.maximum = this_mean_distance

                    # self.maximum = (previous_mean_distance + this_mean_distance) / 2

                    # self.maximum = (throughput_ratio * previous_window.distance)
                    # self.maximum = (throughput_ratio * this_mean_distance)
                    # self.maximum = previous_mean_distance
                    # self.prefetch_distance = previous_window.distance / 2
                    # self.prefetch_distance = this_mean_distance
                    # self.prefetch_distance = previous_window.distance
                    # self.prefetch_distance = (throughput_ratio * previous_window.distance)
                # else:
                #     self.maximum = None

                distances = f"this mean distance: {format(this_mean_distance, '.2f')}. previous mean distance: {format(previous_mean_distance, '.2f')}. this max distance: {format(this_max_distance, '.2f')}. new_starting_distance: {format(self.prefetch_distance, '.2f')}."
                # print(ratios, distances)

            # if self.maximum is None:
            #     self.maximum = self.prefetch_distance * 2
            new_window = Window(self.window.change_id + 1, self.tick, self.prefetch_distance)
            new_window.closing_throughput = self.alt_through
            self.log.append(new_window)

    def reaction(self):
        if self.waited_at is None:
            if self.completed < self.headroom:
                self.waited_at = self.tick
            wait_time = 0
        else:
            wait_time = self.tick - self.waited_at
        # elif self.waited_at is not None and self.completed >= self.headroom:
        #     self.waited_at = None

        for io in itertools.chain(self.pipeline['completed'], self.pipeline['completed']):
            # Ensure that we don't account an IO more than once
            if getattr(io, "accounted", None) is not None:
                continue
            io.accounted = True

            self.on_complete(io)

#         if len(self.consume_log) > 1:
#             ios = list(reversed(self.consume_log))[:int(self.prefetch_distance)]
#             max_through_for_pfd = 0
#             best_io = None
#             for io in ios:
#                 ratio = io.prefetch_distance / (io.completion_time -
#                         io.submission_time)
#                 if ratio > max_throughput_for_pfd:
#                     max_throughput_for_pfd = ratio
#                     best_io = io

            # print(f"best pfd: {best_io.prefetch_distance}")

            # mean([io.prefetch_distance / (io.completion_time - io.submission_time) for io in ios])


        if len(self) == 0:
            if len(self.log) > 0:
                for window in self.log:
                    if len(window) > 0:
                        duration = window[-1].completion_time - window.open_tick
                        total_w_cached = len(window) + window.cached_ios_consumed
                        tput_w_cached = total_w_cached / duration
                        # tput_w_cached = len(window) + (len(window) + window.cached_ios_consumed) / duration
                        avg_lat = mean([io.completion_time - io.submission_time for io in window])
                        mystr = f"starting pfd: {format(window.distance, '.2f')}. duration: {duration}. nios: {len(window)}. cached_ios: {window.cached_ios_consumed}. avg_lat: {format(avg_lat, '.2f')}. tput: {format(len(window) / duration * 10000, '.2f')}. tput_w_cached: {format(tput_w_cached, '.2f')}"
                        # print(mystr)
                        # for io in window:
                        #     print(f"io submitted: {io.submission_time}. io completed: {io.completion_time}")

        super().reaction()


        self.info['wait_time'] = wait_time
        self.info['idle_time'] = 0
        self.info['throughput'] = self.throughput
        self.info['throughput_w_cached'] = self.throughput_w_cached
