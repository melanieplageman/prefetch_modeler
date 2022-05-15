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

def dump_plots(cohort, storage_name, workload_name):
    xlim = 0
    yiolim, yratelim = 0, 0
    ypid_integral_lowerlim, ypid_integral_upperlim = 0, 0
    ypid_proportional_lowerlim, ypid_proportional_upperlim = 0, 0
    ypid_derivative_lowerlim, ypid_derivative_upperlim = 0, 0
    for member in cohort.members:
        max_x = max(max(member.rate_view.index),
                    max(member.pid_view.index),
                    max(member.io_view.index))
        xlim = max_x if max_x > xlim else xlim

        yiolim = calc_upper_ylim(member.io_view, member.io_view.columns, yiolim)

        yratelim = calc_upper_ylim(member.rate_view,
                                    member.rate_view.columns, yratelim)

        columns = ['integral_term', 'integral_term_w_coefficient',
                    'awd_integral_term', 'cnc_integral_term',
                    'awd_integral_term_w_coefficient',
                    'cnc_integral_term_w_coefficient']
        ypid_integral_upperlim = calc_upper_ylim(member.pid_view, columns, ypid_integral_upperlim)
        ypid_integral_lowerlim = calc_lower_ylim(member.pid_view, columns, ypid_integral_lowerlim)

        columns = ['derivative_term', 'derivative_term_w_coefficient']
        ypid_derivative_upperlim = calc_upper_ylim(member.pid_view, columns, ypid_derivative_upperlim)
        ypid_derivative_lowerlim = calc_lower_ylim(member.pid_view, columns, ypid_derivative_lowerlim)

        columns = ['proportional_term', 'proportional_term_w_coefficient']
        ypid_proportional_upperlim = calc_upper_ylim(member.pid_view, columns, ypid_proportional_upperlim)
        ypid_proportional_lowerlim = calc_lower_ylim(member.pid_view, columns, ypid_proportional_lowerlim)

    directory = f'images/{storage_name}/{workload_name}/'

    ypid_integral_upperlim *= 1.1
    ypid_derivative_upperlim *= 1.1
    ypid_proportional_upperlim *= 1.1
    yratelim *= 1.05
    yiolim *= 1.05
    for member in cohort.members:
        title_str = ", ".join(hint for i, hint in
            sorted(bucket_type.hint() for bucket_type in member.schema if
                    bucket_type.hint() is not None)
        )

        figure, axes = plt.subplots(5)
        figure.set_size_inches(15, 25)

        axes[0].set_xlim([0, xlim])
        axes[0].set_ylim([0, yiolim])
        member.io_view.plot(ax=axes[0], title=title_str)

        axes[1].get_yaxis().set_visible(False)
        axes[1].set_xlim([0, xlim])
        member.wait_view.astype(int).plot.area(ax=axes[1], stacked=False)

        axes[2].set_xlim([0, xlim])
        # axes[2].set_ylim([0, yratelim])
        member.rate_view.plot(ax=axes[2])

        prefetcher_name = '_'.join([bucket.__name__ for bucket in member.prefetcher])
        filename = ChartImage(storage_name, workload_name).parented_path(prefetcher_name)

        if prefetcher_name in ['BaselineFetchAll', 'BaselineSync']:
            plt.savefig(filename)
            continue

        prop_ax = 3
        integral_ax = 4
        der_ax = 5

        axes[prop_ax].set_xlim([0, xlim])
        axes[prop_ax].set_ylim([ypid_proportional_lowerlim, ypid_proportional_upperlim])

        if 'proportional_term' in member.pid_view.columns:
            member.pid_view.plot(y=['proportional_term'], ax=axes[prop_ax])
        if 'proportional_term_w_coefficient' in member.pid_view.columns:
            member.pid_view.plot(y=['proportional_term_w_coefficient'], ax=axes[prop_ax])

        axes[integral_ax].set_xlim([0, xlim])
        axes[integral_ax].set_ylim([ypid_integral_lowerlim, ypid_integral_upperlim])

        if 'integral_term' in member.pid_view.columns:
            member.pid_view.plot(y=['integral_term'], ax=axes[integral_ax])
        if 'integral_term_w_coefficient' in member.pid_view.columns:
            member.pid_view.plot(y=['integral_term_w_coefficient'], ax=axes[integral_ax])

        if 'cnc_integral_term' in member.pid_view.columns:
            member.pid_view.plot(y=['cnc_integral_term'], ax=axes[integral_ax])
        if 'cnc_integral_term_w_coefficient' in member.pid_view.columns:
            member.pid_view.plot(y=['cnc_integral_term_w_coefficient'], ax=axes[integral_ax])

        if 'awd_integral_term' in member.pid_view.columns:
            member.pid_view.plot(y=['awd_integral_term'], ax=axes[integral_ax])
        if 'awd_integral_term_w_coefficient' in member.pid_view.columns:
            member.pid_view.plot(y=['awd_integral_term_w_coefficient'], ax=axes[integral_ax])

        # axes[der_ax].set_xlim([0, xlim])
        # axes[der_ax].set_ylim([ypid_derivative_lowerlim, ypid_derivative_upperlim])

        # if 'derivative_term' in member.pid_view.columns:
        #     member.pid_view.plot(y=['derivative_term'], ax=axes[der_ax])
        # if 'derivative_term_w_coefficient' in member.pid_view.columns:
        #     member.pid_view.plot(y=['derivative_term_w_coefficient'], ax=axes[der_ax])


        plt.savefig(filename)

        # plt.show()

        # if not member.tracer_view.empty:
        #     member.tracer_view.plot(kind='barh', stacked=True)
        #     plt.savefig(f'{directory}/{prefetcher_name}_tracer.png')

