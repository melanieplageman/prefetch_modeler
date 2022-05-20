from prefetch_modeler.core import Metric


def metric(function):
    metric_type = type(Metric)(function.__name__, (Metric,), {"function": staticmethod(function)})
    return metric_type

@metric
def storage_rate(pipeline):
    return float(pipeline['newfetcher'].raw_storage_rate)

@metric
def io_latency(pipeline):
    latency = pipeline['newfetcher'].period.latency
    if latency == 0:
        return None
    return latency

@metric
def storage_rate_change(pipeline):
    return float(pipeline['newfetcher'].storage_rate_change)

@metric
def latency_change(pipeline):
    return float(pipeline['newfetcher'].latency_change)

@metric
def in_storage(pipeline):
    return inflight.function(pipeline) + submitted.function(pipeline)

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
        return None
    return storage_rate_change.function(pipeline) / lchange

@metric
def storage_latency_ratio2(pipeline):
    latency = io_latency.function(pipeline)
    if latency is None or latency == 0:
        return None
    completed_rate = pipeline['newfetcher'].raw_storage_rate
    return float(completed_rate / latency)

@metric
def completed_not_consumed(pipeline):
    return len(pipeline['completed'])

@metric
def remaining(pipeline):
    return len(pipeline['remaining'])

@metric
def done(pipeline):
    return len(pipeline['consumed'])

@metric
def cnc_headroom(pipeline):
    return pipeline['remaining'].cnc_headroom

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
def do_consume(pipeline):
    return pipeline['awaiting_buffer'].info['to_move']

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
