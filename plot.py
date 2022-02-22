import matplotlib.pyplot as plt
from mpl_toolkits.axisartist.parasite_axes import HostAxes, ParasiteAxes

def single_plot(df):
    fig = plt.figure(figsize=(15,11))

    host = fig.add_subplot(axes_class=HostAxes)
    consumed_ax = ParasiteAxes(host, sharex=host)
    wait_ax = ParasiteAxes(host, sharex=host)

    host.parasites.append(consumed_ax)
    host.parasites.append(wait_ax)

    host.axis['right'].set_visible(False)

    consumed_ax.axis['right'].set_visible(True)
    consumed_ax.axis['right'].major_ticklabels.set_visible(True)
    consumed_ax.axis['right'].label.set_visible(True)

    wait_ax.axis['right2'] = wait_ax.new_fixed_axis(loc='right', offset=(60, 0))

    host.plot(df['inflight'], label='Inflight')
    host.plot(df['completed'], label='Completed Not Consumed')
    host.plot(df['completion_target_distance'], label='Completion Target Distance')
    host.plot(df['target_inflight'], label='Target Inflight')

    p3, = consumed_ax.plot(df['consumed'], label='Consumed')

    p4, = wait_ax.plot(df['wait'], label='Wait')

    host.set_xlabel('Time')
    host.set_ylabel('IOs')
    consumed_ax.set_ylabel('IOs Consumed')
    wait_ax.set_ylabel('Wait')

    host.legend()

    consumed_ax.axis['right'].label.set_color(p3.get_color())
    wait_ax.axis['right2'].label.set_color(p4.get_color())

    plt.show()
