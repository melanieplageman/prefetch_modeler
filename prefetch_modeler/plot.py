import matplotlib.pyplot as plt
from mpl_toolkits.axisartist.parasite_axes import HostAxes, ParasiteAxes
import pandas as pd

def calc_wait(to_plot):
    # TODO: what is a good way to skip ticks that aren't there
    last_wait = 0
    for idx, waited in enumerate(to_plot['wait']):
        try:
            last_wait = last_wait + 1 if waited else 0
            to_plot['wait'][idx] = last_wait
        except: KeyError


def transform(data):
    to_plot = pd.DataFrame(index=data.index)
    to_plot['wait'] = data.apply(lambda record:
        record['completed_want_to_move'] > record['completed_to_move'], axis='columns')

    # Consider printing the discrepancy between how many you wanted to consume
    # and how many you did consume - this would be the same as consumed_num_ios
    # if we added an extra bucket at the end (I think?)
    # for idx, completed_want_to_move in enumerate(data['completed_want_to_move']):
    #     completed_moved = data['completed_to_move'][idx]
    #     if completed_want_to_move - completed_moved

    # calc_wait(to_plot)

    to_plot['prefetched'] = data['prefetched_num_ios']
    to_plot['submitted'] = data['submitted_num_ios']
    to_plot['completed'] = data['completed_num_ios']
    to_plot['inflight'] = data['inflight_latency_num_ios']
    to_plot['total_inflight'] = data['inflight_num_ios']
    to_plot['consumed'] = data['consumed_num_ios']
    to_plot['completion_target_distance'] = data['prefetched_completion_target_distance']
    to_plot['target_inflight'] = data['prefetched_target_inflight']
    return to_plot

def single_run_plot(df):
    fig = plt.figure(figsize=(15,11))
    ax_main = fig.add_subplot()
    ax_wait = ax_main.twinx()
    # ax_main.plot(df['prefetched'], label='prefetched')
    # ax_main.plot(df['submitted'], label='submitted')
    ax_main.plot(df['completed'], label='completed')
    ax_main.plot(df['inflight'], label='inflight')
    ax_main.plot(df['total_inflight'], label='total inflight')
    # ax_main.plot(df['completion_target_distance'], label='CTD')
    # TODO: make sure the color is different
    ax_wait.plot(df['wait'], ':', label='wait', color='black')
    ax_main.legend(loc='upper left')
    ax_wait.legend(loc='upper right')
    ax_wait.set_ylabel('Wait Time')
    ax_main.set_ylabel('IOs')
    ax_main.set_xlabel('Time')

    # ax_consumed = ax_main.twinx()
    # ax_consumed.plot(df['consumed'])

    plt.show()

def to_row(data):
    row = {}
    transformed_data = transform(data)
    max_wait = 0
    total_wait = 0
    for idx, wait_time in enumerate(transformed_data['wait']):
        try:
            if wait_time > max_wait:
                max_wait = wait_time
            if wait_time > 0:
                total_wait += 1
        except: KeyError

    row['max_wait_sec'] = max_wait / 1000 / 1000
    duration = len(transformed_data.index)
    row['% wait'] = int(total_wait / duration * 100)

    consumed = max(transformed_data['consumed'])
    duration_sec = duration / 1000 / 1000
    rate = int(consumed/duration_sec)

    print(f'duration: {duration_sec} seconds. consumed: {consumed} IOs. rate: {rate} IOs/sec')
    return row


def do_plot(data):
    to_plot = transform(data)

    # Reindex the dataframe on `tick` to be continuous so that matplotlib
    # doesn't interpolate
    to_plot = to_plot.reindex(range(to_plot.index.min(), to_plot.index.max() + 1),
                    method='ffill')
    single_run_plot(to_plot)
    return to_row(data)

def io_title(pipeline_config):
    storage = pipeline_config.storage
    workload = pipeline_config.workload
    prefetcher = pipeline_config.prefetcher

    storage_s = f'storage: {storage.name}'
    workload_s = f'workload: {str(workload.id)}'

    prelim = storage_s + ', ' + workload_s
    prefetcher_s = f'prefetcher: {str(prefetcher)}'

    title = '\n'.join([prelim, prefetcher_s])
    return title

def plot_io_data(data):
    view = pd.DataFrame(index=data.index)
    rename = {
        # 'do_sync_fetch': 'baseline_sync_to_move',
        # 'do_fetch_all': 'baseline_all_to_move',
        # 'do_prefetch': 'remaining_to_move',
        # 'do_claim': 'awaiting_buffer_to_move',
        # 'num_ios_w_buffer': 'w_claimed_buffer_num_ios',
        # 'do_invoke_kernel': 'w_claimed_buffer_to_move',
        # 'do_submit': 'kernel_batch_to_move',
        # 'do_dispatch': 'submitted_to_move',
        # 'inflight': 'inflight_num_ios',
        # 'do_complete': 'inflight_to_move',
        'completed_not_consumed': 'completed_num_ios',
        'do_consume': 'completed_to_move',
        'completion_target_distance': 'remaining_completion_target_distance',
        'min_dispatch': 'remaining_min_dispatch',
    }
    rename = {k: data[v] for k, v in rename.items() if v in data}
    view = view.assign(**rename)
    return view

def plot_wait_data(data):
    wait_view = pd.DataFrame(index=data.index)
    for column in data:
        if not column.endswith('_want_to_move'):
            continue
        bucket_name = column.removesuffix('_want_to_move')
        wait_view[f'{bucket_name}_wait'] = data.apply(
            lambda record: record[f'{bucket_name}_want_to_move'] > record[f'{bucket_name}_to_move'],
            axis='columns')

    wait_rename = {
        'wait_dispatch': 'submitted_wait',
        'wait_consume': 'completed_wait',
    }

    wait_rename = {k: wait_view[v] for k, v in wait_rename.items() if v in wait_view}
    wait_view = wait_view.assign(**wait_rename)
    dropped_columns = [column for column in wait_view if column not in wait_rename.keys()]
    wait_view = wait_view.drop(columns=dropped_columns)
    return wait_view
