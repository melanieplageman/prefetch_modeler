def default_metrics(simulation):
    @simulation.metric('proportional_term')
    def metric(self):
        return float(getattr(self['remaining'], 'proportional_term', 0))

    @simulation.metric('proportional_term_w_coefficient')
    def metric(self):
        term = float(getattr(self['remaining'], 'proportional_term', 0))
        gain = float(getattr(self['remaining'], 'kp', 0))
        return term * gain

    @simulation.metric('cnc_integral_term')
    def metric(self):
        return float(getattr(self['remaining'], 'cnc_integral_term', 0))

    @simulation.metric('cnc_integral_term_w_coefficient')
    def metric(self):
        term = float(getattr(self['remaining'], 'cnc_integral_term', 0))
        gain = float(getattr(self['remaining'], 'ki_cnc', 0))
        return term * gain

    @simulation.metric('awd_integral_term')
    def metric(self):
        return float(getattr(self['remaining'], 'awd_integral_term', 0))

    @simulation.metric('awd_integral_term_w_coefficient')
    def metric(self):
        term = float(getattr(self['remaining'], 'awd_integral_term', 0))
        gain = float(getattr(self['remaining'], 'ki_awd', 0))
        return term * gain

    @simulation.metric('demand_rate')
    def metric(self):
        return float(getattr(self['remaining'], 'demand_rate', 0))

    @simulation.metric('awaiting_dispatch')
    def metric(self):
        in_progress = self['remaining'].counter - len(self['consumed'])
        inflight = len(self['inflight'])
        completed = len(self['completed'])
        return in_progress - inflight - completed - len(self['remaining'])

    @simulation.metric('cnc_headroom')
    def metric(self):
        return getattr(self['remaining'], 'cnc_headroom', 0)

    @simulation.metric('wait_consume')
    def metric(self):
        completed = self['completed']
        return completed.info['want_to_move'] > completed.info['to_move']

    @simulation.metric('remaining_ios')
    def metric(self):
        return len(self['remaining'])

    @simulation.metric('max_iops')
    def metric(self):
        return float(self['submitted'].storage_speed)

    @simulation.metric('consumption_rate')
    def metric(self):
        return float(self['completed'].rate())

    @simulation.metric('prefetch_rate')
    def metric(self):
        return float(self['remaining'].rate())

    @simulation.metric('completed_not_consumed')
    def metric(self):
        return len(self['completed'])

    @simulation.metric('remaining')
    def metric(self):
        return len(self['remaining'])

    @simulation.metric('done')
    def metric(self):
        return len(self['consumed'])

    @simulation.metric('do_prefetch')
    def metric(self):
        return self['remaining'].info['to_move']


    @simulation.metric('do_dispatch')
    def metric(self):
        return self['submitted'].info['to_move']

    @simulation.metric('submitted')
    def metric(self):
        return len(self['submitted'])

    @simulation.metric('inflight')
    def metric(self):
        return len(self['inflight'])

    @simulation.metric('do_complete')
    def metric(self):
        return self['inflight'].info['to_move']

    @simulation.metric('do_consume')
    def metric(self):
        return self['completed'].info['to_move']

def storage_metrics(simulation):
    @simulation.metric('do_claim')
    def metric(self):
        return self['awaiting_buffer'].info['to_move']

    @simulation.metric('num_ios_w_buffer')
    def metric(self):
        return len(self['w_claimed_buffer'])

    @simulation.metric('do_invoke_kernel')
    def metric(self):
        return self['w_claimed_buffer'].info['to_move']

    @simulation.metric('do_submit')
    def metric(self):
        return self['kernel_batch'].info['to_move']
