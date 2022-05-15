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

def dump_plots(cohort, storage_name, workload_name):
    xlim = 0
    yiolim, yratelim = 0, 0
    ypid_integral_lowerlim, ypid_integral_upperlim = 0, 0
    ypid_proportional_lowerlim, ypid_proportional_upperlim = 0, 0
    ypid_derivative_lowerlim, ypid_derivative_upperlim = 0, 0
    for member in cohort.members:
        max_x = max(max(member.metric_data.index),
                    max(member.io_view.index))
        xlim = max_x if max_x > xlim else xlim

        yiolim = calc_upper_ylim(member.io_view, member.io_view.columns, yiolim)

        # columns = ['prefetch_rate', 'consumption_rate']
        # yratelim = calc_upper_ylim(member.metric_data, columns, yratelim)

        columns = ['awd_integral_term', 'cnc_integral_term',
                    'awd_integral_term_w_coefficient',
                    'cnc_integral_term_w_coefficient']
        ypid_integral_upperlim = calc_upper_ylim(member.metric_data, columns, ypid_integral_upperlim)
        ypid_integral_lowerlim = calc_lower_ylim(member.metric_data, columns, ypid_integral_lowerlim)

        columns = ['proportional_term', 'proportional_term_w_coefficient']
        ypid_proportional_upperlim = calc_upper_ylim(member.metric_data, columns, ypid_proportional_upperlim)
        ypid_proportional_lowerlim = calc_lower_ylim(member.metric_data, columns, ypid_proportional_lowerlim)

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

        columns = ['wait_consume']
        member.metric_data.astype(int).plot.area(y=columns, ax=axes[1], stacked=False)

        axes[2].set_xlim([0, xlim])
        # axes[2].set_ylim([0, yratelim])

        # columns = ['prefetch_rate', 'consumption_rate']
        # member.metric_data.plot(y=columns, ax=axes[2])

        prefetcher_name = '_'.join([bucket.__name__ for bucket in member.prefetcher])
        filename = ChartImage(storage_name, workload_name).parented_path(prefetcher_name)

        if prefetcher_name in ['BaselineFetchAll', 'BaselineSync']:
            plt.savefig(filename)
            continue

        prop_ax = 3
        integral_ax = 4

        axes[prop_ax].set_xlim([0, xlim])
        axes[prop_ax].set_ylim([ypid_proportional_lowerlim, ypid_proportional_upperlim])

        columns = ['proportional_term', 'proportional_term_w_coefficient']
        member.metric_data.plot(y=columns, ax=axes[prop_ax])

        # axes[integral_ax].set_xlim([0, xlim])
        # axes[integral_ax].set_ylim([ypid_integral_lowerlim, ypid_integral_upperlim])

        columns = ['cnc_integral_term', 'cnc_integral_term_w_coefficient']
        member.metric_data.plot(y=columns, ax=axes[integral_ax])

        columns = ['awd_integral_term', 'awd_integral_term_w_coefficient']
        member.metric_data.plot(y=columns, ax=axes[integral_ax])


        plt.savefig(filename)

        # plt.show()

        # if not member.tracer_view.empty:
        #     member.tracer_view.plot(kind='barh', stacked=True)
        #     plt.savefig(f'{directory}/{prefetcher_name}_tracer.png')

