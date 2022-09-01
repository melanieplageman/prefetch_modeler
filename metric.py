from prefetch_modeler.core import Metric
from numpy import mean



def metric(function):
    metric_type = type(Metric)(function.__name__, (Metric,), {"function": staticmethod(function)})
    return metric_type


@metric
def wait_time(pipeline):
    return pipeline['cd_fetcher'].info.get('wait_time', 0)

@metric
def idle_time(pipeline):
    return -pipeline['cd_fetcher'].info.get('idle_time', 0)


@metric
def do_cd_fetch(pipeline):
    return pipeline['cd_fetcher'].info['to_move']

@metric
def prefetch_rate_limit(pipeline):
    return float(pipeline['ratelimiter'].rate())

@metric
def capacity(pipeline):
    return float(pipeline['ratelimiter'].target_group_capacity())

@metric
def raw_demand_rate(pipeline):
    return float(pipeline['remaining'].raw_demand_rate)

@metric
def storage_capacity(pipeline):
    return pipeline['submitted'].target_capacity()

@metric
def storage_rate(pipeline):
    return float(pipeline['ratelimiter'].raw_storage_rate)

@metric
def io_latency(pipeline):
    latency = pipeline['ratelimiter'].latency
    if latency == 0:
        return None
    return latency

@metric
def base_latency_estimate(pipeline):
    return pipeline['ratelimiter'].regression

# @metric
# def prop(pipeline):
#     return float(pipeline['remaining'].proportional_term)

# @metric
# def cnc_integral(pipeline):
#     return float(pipeline['remaining'].cnc_integral_term)

@metric
def lat_derivative(pipeline):
    latdt = float(pipeline['ratelimiter'].latency_derivative)
    if latdt == 0:
        return None
    return latdt

@metric
def gain(pipeline):
    return pipeline['ratelimiter'].recent_gain

@metric
def adjustment(pipeline):
    return float(pipeline['ratelimiter'].adjustment)

@metric
def lat_integral(pipeline):
    return float(pipeline['ratelimiter'].integral_term)

@metric
def storage_rate_change(pipeline):
    return float(pipeline['ratelimiter'].storage_rate_change)

@metric
def latency_rate_of_change(pipeline):
    return float(pipeline['ratelimiter'].latency_rate)

@metric
def submitted(pipeline):
    return len(pipeline['submitted'])

@metric
def in_storage(pipeline):
    return len(pipeline['minimum_latency']) + \
           len(pipeline['inflight']) + \
           len(pipeline['deadline'])
    return len(pipeline['submitted']) + len(pipeline['inflight'])

@metric
def io_ratio(pipeline):
    latency = io_latency.function(pipeline)
    if latency is None or latency == 0:
        return None
    return in_storage.function(pipeline) / latency

@metric
def storage_latency_ratio(pipeline):
    lchange = latency_change.function(pipeline)
    if lchange is None or lchange == 0:
        return 0
    srchange = storage_rate_change.function(pipeline)
    if srchange == 0:
        return 0
    return srchange / lchange

@metric
def storage_latency_ratio2(pipeline):
    latency = io_latency.function(pipeline)
    if latency is None or latency == 0:
        return None
    completed_rate = pipeline['ratelimiter'].raw_storage_rate
    return float(completed_rate / latency)

@metric
def completed_not_consumed(pipeline):
    return len(pipeline['completed'])

@metric
def cd_remaining(pipeline):
    return len(pipeline['cd_fetcher'])

@metric
def remaining(pipeline):
    return len(pipeline['remaining'])

# @metric
# def remaining(pipeline):
#     return len(pipeline['remaining']) + len(pipeline['ratelimiter'])

@metric
def done(pipeline):
    return len(pipeline['consumed'])

@metric
def prefetch_distance(pipeline):
    return pipeline['cd_fetcher'].prefetch_distance

@metric
def constant_cnc_headroom(pipeline):
    return pipeline['cd_fetcher'].cnc_headroom

@metric
def cnc_headroom(pipeline):
    return pipeline['remaining'].cnc_headroom

@metric
def raw_completion_rate(pipeline):
    raw = pipeline['ratelimiter'].raw_completion_rate
    return float(raw)

@metric
def completion_inflight_ratio(pipeline):
    completion = pipeline['ratelimiter'].raw_completion_rate
    in_storage = pipeline['ratelimiter'].in_storage_rate
    if in_storage <= 0:
        return None
    return completion / in_storage

@metric
def in_storage_rate(pipeline):
    in_storage_rate = pipeline['ratelimiter'].in_storage_rate
    return in_storage_rate

@metric
def awaiting_dispatch(pipeline):
    in_progress = pipeline['remaining'].counter - len(pipeline['consumed'])
    inflight = len(pipeline['inflight'])
    completed = len(pipeline['completed'])
    return in_progress - inflight - completed - len(pipeline['remaining'])

@metric
def do_prefetch(pipeline):
    return pipeline['remaining'].info['to_move']

@metric
def do_ratelimit(pipeline):
    return pipeline['ratelimiter'].info['to_move']

@metric
def do_dispatch(pipeline):
    return pipeline['submitted'].info['to_move']

@metric
def submitted(pipeline):
    return len(pipeline['submitted'])

@metric
def inflight(pipeline):
    return len(pipeline['inflight'])

@metric
def do_complete(pipeline):
    return pipeline['inflight'].info['to_move']

@metric
def do_consume(pipeline):
    return pipeline['completed'].info['to_move']

@metric
def num_ios_w_buffer(pipeline):
    return len(pipeline['w_claimed_buffer'])

@metric
def do_invoke_kernel(pipeline):
    return pipeline['w_claimed_buffer'].info['to_move']

@metric
def do_submit(pipeline):
    return pipeline['kernel_batch'].info['to_move']

@metric
def wait_consume(pipeline):
    completed = pipeline['completed']
    return int(completed.info['want_to_move'] > completed.info['to_move'])

@metric
def demand_rate(pipeline):
    return float(pipeline['remaining'].demand_rate)

@metric
def consumption_rate(pipeline):
    return float(pipeline['completed'].rate())

@metric
def prefetch_rate(pipeline):
    return float(pipeline['remaining'].rate())

@metric
def max_iops(pipeline):
    return float(pipeline['inflight'].rate())
    return float(pipeline['submitted'].storage_speed)


@metric
def proportional_term(pipeline):
    return float(pipeline['remaining'].proportional_term)

@metric
def proportional_term_w_coefficient(pipeline):
    term = pipeline['remaining'].proportional_term
    gain = pipeline['remaining'].kp
    return float(term * gain)

@metric
def cnc_integral_term(pipeline):
    return float(pipeline['remaining'].cnc_integral_term)

@metric
def cnc_integral_term_w_coefficient(pipeline):
    term = pipeline['remaining'].cnc_integral_term
    gain = pipeline['remaining'].ki_cnc
    return float(term * gain)

@metric
def awd_integral_term(pipeline):
    return float(pipeline['remaining'].awd_integral_term)

@metric
def awd_integral_term_w_coefficient(pipeline):
    term = pipeline['remaining'].awd_integral_term
    gain = pipeline['remaining'].ki_awd
    return float(term * gain)

@metric
def avg_total_latency_completed_ios(pipeline):
    real_ios = [io for io in pipeline['completed'] if getattr(io, 'cached', None) is None]
    if not real_ios:
        return None
    completion_latencies = [io.completion_time - io.submission_time for io in real_ios]
    return mean(completion_latencies)

@metric
def latency_dt(pipeline):
    return pipeline['cd_fetcher'].info.get('latency_dt', None)

@metric
def in_storage_dt(pipeline):
    return pipeline['cd_fetcher'].info.get('in_storage_dt', None)

@metric
def wait_dt(pipeline):
    return pipeline['cd_fetcher'].info.get('wait_dt', None)

@metric
def latency_cost(pipeline):
    # return in_storage.function(pipeline) / avg_total_latency_completed_ios.function(pipeline)
    return pipeline['cd_fetcher'].info.get('latency_cost', None)

@metric
def wait_benefit(pipeline):
    return pipeline['cd_fetcher'].info.get('wait_benefit', None)

@metric
def wait_benefit_dt(pipeline):
    return pipeline['cd_fetcher'].info.get('wait_benefit_dt', None)

@metric
def latency_cost_dt(pipeline):
    return pipeline['cd_fetcher'].info.get('latency_cost_dt', None)

@metric
def processing_vs_pfd(pipeline):
    avg_p_t = pipeline['consumed'].info.get('avg_processing_time', 0)
    return avg_p_t

