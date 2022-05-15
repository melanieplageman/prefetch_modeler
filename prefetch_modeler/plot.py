import matplotlib.pyplot as plt
import pandas as pd

def io_data(data):
    view = pd.DataFrame(index=data.index)
    rename = {
        # 'remaining': 'remaining_num_ios',
        # 'done': 'consumed_num_ios',
        # 'do_sync_fetch': 'baseline_sync_to_move',
        # 'do_fetch_all': 'baseline_all_to_move',
        # 'do_prefetch': 'remaining_to_move',
        # 'do_claim': 'awaiting_buffer_to_move',
        # 'num_ios_w_buffer': 'w_claimed_buffer_num_ios',
        # 'do_invoke_kernel': 'w_claimed_buffer_to_move',
        # 'do_submit': 'kernel_batch_to_move',
        # 'do_dispatch': 'submitted_to_move',
        # 'submitted': 'submitted_num_ios',
        # 'inflight': 'inflight_num_ios',
        # 'do_complete': 'inflight_to_move',
        'completed_not_consumed': 'completed_num_ios',
        'awaiting_dispatch': 'remaining_awaiting_dispatch',
        'cnc_headroom': 'remaining_cnc_headroom',
        # 'aw_headroom': 'remaining_aw_headroom',
        # 'do_consume': 'completed_to_move',
        # 'completion_target_distance': 'remaining_completion_target_distance',
        # 'min_dispatch': 'remaining_min_dispatch',
    }
    rename = {k: data[v] for k, v in rename.items() if v in data}
    view = view.assign(**rename)
    return view

def rate_data(data):
    rate_view = pd.DataFrame(index=data.index)
    cr_rename = {
        'consumption_rate': 'completed_rate',
        'prefetch_rate': 'remaining_rate',
        'demand_rate': 'remaining_demand_rate',
        'max_iops': 'remaining_max_iops',
        # 'storage_completed_rate': 'remaining_storage_completed_rate',
        # 'cnc_rate': 'remaining_cnc_rate',
        # 'awd_rate': 'remaining_awd_rate',
    }
    cr_rename = {k: data[v] for k, v in cr_rename.items() if v in data}
    rate_view = rate_view.assign(**cr_rename)

    for attr in cr_rename.keys():
        if attr in rate_view:
            rate_view[attr] = [value * 1000 * 1000 for value in rate_view[attr]]

    dropped_columns = [column for column in rate_view if column not in cr_rename.keys()]
    rate_view = rate_view.drop(columns=dropped_columns)
    # pd.set_option('display.max_rows', None)
    print(rate_view)
    return rate_view

def pid_data(data):
    pid_view = pd.DataFrame(index=data.index)
    cr_rename = {
        # 'derivative_term': 'remaining_derivative_term',
        'proportional_term': 'remaining_proportional_term',
        'integral_term': 'remaining_integral_term',
        'integral_term_w_coefficient': 'remaining_integral_term_w_coefficient',
        'cnc_integral_term': 'remaining_cnc_integral_term',
        'cnc_integral_term_w_coefficient': 'remaining_cnc_integral_term_w_coefficient',
        'awd_integral_term': 'remaining_awd_integral_term',
        'awd_integral_term_w_coefficient': 'remaining_awd_integral_term_w_coefficient',
        'proportional_term_w_coefficient': 'remaining_proportional_term_w_coefficient',
        'derivative_term_w_coefficient': 'remaining_derivative_term_w_coefficient',
    }

    cr_rename = {k: data[v] for k, v in cr_rename.items() if v in data}
    pid_view = pid_view.assign(**cr_rename)
    dropped_columns = [column for column in pid_view if column not in cr_rename.keys()]
    pid_view = pid_view.drop(columns=dropped_columns)
    print(pid_view)
    return pid_view

def wait_data(data):
    wait_view = pd.DataFrame(index=data.index)
    for column in data:
        if not column.endswith('_want_to_move'):
            continue
        bucket_name = column.removesuffix('_want_to_move')
        wait_view[f'{bucket_name}_wait'] = data.apply(
            lambda record: record[f'{bucket_name}_want_to_move'] > record[f'{bucket_name}_to_move'],
            axis='columns')

    wait_rename = {
        # 'wait_dispatch': 'submitted_wait',
        'wait_consume': 'completed_wait',
    }

    wait_rename = {k: wait_view[v] for k, v in wait_rename.items() if v in wait_view}
    wait_view = wait_view.assign(**wait_rename)
    dropped_columns = [column for column in wait_view if column not in wait_rename.keys()]
    wait_view = wait_view.drop(columns=dropped_columns)
    return wait_view
