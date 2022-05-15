import matplotlib.pyplot as plt
import pandas as pd
from chart_image import ChartImage

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

def calc_upper_ylim(df, columns, current_ylim):
    max_y = 0
    for column in columns:
        if column not in df.columns:
            continue
        column_max_y = max(df[column])
        max_y = column_max_y if column_max_y > max_y else max_y

    return max_y if max_y > current_ylim else current_ylim

def calc_lower_ylim(df, columns, current_ylim):
    min_y = 0
    for column in columns:
        if column not in df.columns:
            continue
        column_min_y = min(df[column])
        min_y = column_min_y if column_min_y < min_y else min_y

    return min_y if min_y < current_ylim else current_ylim

class Limits:
    xlim = 0
    yiolim = 0
    yratelim = 0
    ypid_integral_lowerlim = 0
    ypid_integral_upperlim = 0
    ypid_proportional_lowerlim = 0
    ypid_proportional_upperlim = 0

def set_limits(simulations):
    bounds = Limits()
    for member in simulations:
        max_x = max(max(member.metric_data.index),
                    max(member.io_view.index))
        bounds.xlim = max_x if max_x > bounds.xlim else bounds.xlim

        bounds.yiolim = calc_upper_ylim(member.io_view, member.io_view.columns,
                                        bounds.yiolim)

        # columns = ['prefetch_rate', 'consumption_rate']
        # bounds.yratelim = calc_upper_ylim(member.metric_data, columns,
        # bounds.yratelim)

        columns = ['awd_integral_term', 'cnc_integral_term',
                    'awd_integral_term_w_coefficient',
                    'cnc_integral_term_w_coefficient']
        bounds.ypid_integral_upperlim = calc_upper_ylim(member.metric_data, columns,
                                                 bounds.ypid_integral_upperlim)
        bounds.ypid_integral_lowerlim = calc_lower_ylim(member.metric_data, columns,
                                                 bounds.ypid_integral_lowerlim)

        columns = ['proportional_term', 'proportional_term_w_coefficient']
        bounds.ypid_proportional_upperlim = calc_upper_ylim(member.metric_data,
                                                     columns, bounds.ypid_proportional_upperlim)
        bounds.ypid_proportional_lowerlim = calc_lower_ylim(member.metric_data,
                                                     columns, bounds.ypid_proportional_lowerlim)

    bounds.ypid_integral_upperlim *= 1.1
    bounds.ypid_derivative_upperlim *= 1.1
    bounds.ypid_proportional_upperlim *= 1.1
    bounds.yratelim *= 1.05
    bounds.yiolim *= 1.05
    return bounds



def dump_plots(member, storage_name, workload_name):
    directory = f'images/{storage_name}/{workload_name}/'

    title_str = ", ".join(hint for i, hint in
        sorted(bucket_type.hint() for bucket_type in member.schema if
                bucket_type.hint() is not None)
    )

    figure, axes = plt.subplots(5)
    figure.set_size_inches(15, 25)

    member.io_view.plot(ax=axes[0], title=title_str)

    axes[1].get_yaxis().set_visible(False)

    columns = ['wait_consume']
    member.metric_data.astype(int).plot.area(y=columns, ax=axes[1], stacked=False)

    # columns = ['prefetch_rate', 'consumption_rate']
    # member.metric_data.plot(y=columns, ax=axes[2])

    prefetcher_name = '_'.join([bucket.__name__ for bucket in member.prefetcher])
    filename = ChartImage(storage_name, workload_name).parented_path(prefetcher_name)

    if prefetcher_name in ['BaselineFetchAll', 'BaselineSync']:
        plt.savefig(filename)
        return

    prop_ax = 3
    integral_ax = 4

    columns = ['proportional_term', 'proportional_term_w_coefficient']
    member.metric_data.plot(y=columns, ax=axes[prop_ax])

    columns = ['cnc_integral_term', 'cnc_integral_term_w_coefficient']
    member.metric_data.plot(y=columns, ax=axes[integral_ax])

    columns = ['awd_integral_term', 'awd_integral_term_w_coefficient']
    member.metric_data.plot(y=columns, ax=axes[integral_ax])


    plt.savefig(filename)

    # plt.show()

    # if not member.tracer_view.empty:
    #     member.tracer_view.plot(kind='barh', stacked=True)
    #     plt.savefig(f'{directory}/{prefetcher_name}_tracer.png')

