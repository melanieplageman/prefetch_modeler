class AdjustmentAlgorithm:
    def __init__(self, config):
        self.func = config['function']
        self.min_dispatch = config['inputs']['min_dispatch']
        self.max_in_flight = config['inputs']['max_in_flight']
        self.completion_target_distance = config['inputs']['completion_target_distance']

    def hit_limit(self, *args):
        hit_limit = self.func(self.completion_target_distance, self.min_dispatch,
                         self.max_in_flight, *args)
        if hit_limit:
            self.completion_target_distance += 1

        return hit_limit

class Storage:
    def __init__(self, config):
        self.latency_func = config['completion_latency_function']
        self.max_iops = config['max_iops']
        self.base_completion_latency = config['base_completion_latency']
        self.submission_overhead = config['submission_overhead']

    def latency(self, num_ios):
        return self.latency_func(self.base_completion_latency, num_ios)

def adjustment_func(completion_target_distance, min_dispatch, max_in_flight, completed, inflight):
    if completed >= completion_target_distance - min_dispatch:
        return True

    if inflight >= max_in_flight:
        return True

    return False

def latency_func(base_completion_latency, num_ios):
    # TODO: make this change with the number of requests inflight
    completion_latency = (0.01 * num_ios) + base_completion_latency
    return completion_latency

config = {
  'adjustment_algorithm': {
    'function': adjustment_func,
    'inputs': {
      'min_dispatch': 2,
      'max_in_flight': 10,
      'completion_target_distance': 512,
    }
  },
  'storage': {
    'max_iops': 10000,
    'completion_latency_function': latency_func,
    'base_completion_latency': 1.2,
    'submission_overhead': 0.1,
  },
  'run': {
    'consumption_rate': 1,
    'nblocks': 100,
    'nticks': 100,
  },
}
