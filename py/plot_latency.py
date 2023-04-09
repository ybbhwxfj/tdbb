import colorsys
import matplotlib
import matplotlib.backends.backend_pdf as pdf
import matplotlib.pyplot as plt
import numpy as np
from utils import util
import numpy
import matplotlib.pylab as pylab
import math

params = {'legend.fontsize': 'x-large',
          'axes.labelsize': 'x-large',
          'axes.titlesize':'x-large',
          'xtick.labelsize':'x-large',
          'ytick.labelsize':'x-large'}
#todo

annotate_size = 20

json_tb = util.json_values(file='share/g7.4x_terminal_tb.json', filter=lambda row: row['terminals'] == 100)
json_lb_wan = util.json_values(file='share/g7.4x_terminal_lb_wan.json', filter=lambda row: row['terminals'] == 100)
json_lb_lan = util.json_values(file='share/g7.4x_terminal_lb_lan.json', filter=lambda row: row['terminals'] == 100)

name_2_json = {
    'DB-S-TB': json_tb[0],
    'DB-S-LB(LAN)': json_lb_lan[0],
    'DB-S-LB(WAN)': json_lb_wan[0],
}

names = [
    'DB-S-TB', 'DB-S-LB(LAN)', 'DB-S-LB(WAN)'
]


def scale_lightness(rgb, scale_l):
    # convert rgb to hls
    h, l, s = colorsys.rgb_to_hls(*rgb)
    # manipulate h, l, s values and return as rgb
    return colorsys.hls_to_rgb(h, min(1, l * scale_l), s=s)


def plot_bar_chart_latency(name2json, total_latency):
    plt.clf()
    pylab.rcParams.update(params)
    height = 0.6
    fig, axis = plt.subplots(figsize=(14,8))
    label2values = {}
    labels = []
    for key in name2json.keys():
        r = name2json[key]
        read = r['lt_read']
        read_dsb = r['lt_read_dsb']
        append = r['lt_app']
        append_rlb = r['lt_app_rlb']
        lock = r['lt_lock']
        latency = r['lt_part']
        label = key
        labels.append(label)
        array = []
        array.append((read - read_dsb) / latency)
        array.append((append - append_rlb) / latency)
        array.append(read_dsb / latency)
        array.append(append_rlb / latency)
        array.append(lock / latency)
        array.append((latency - read - append - lock) / latency)
        label2values[label] = array

    hatch_list = ['-', '.', '\\', '*', '/', 'x']
    category_names = ['R RPC', 'A RPC', 'R', 'A', 'L', 'O']

    for t in names:
        # print(label2values[t])
        labels = [t]
        _data = [label2values[t]]
        _data = np.array(_data)
        category_colors = plt.colormaps['RdYlGn'](
            np.linspace(0.10, 0.85, _data.shape[1]))
        if total_latency:
            data = [label2values[t]]
            data = np.array(data)
            data_cum = data.cumsum(axis=1)
            ax0 = axis

            ax0.invert_yaxis()
            ax0.xaxis.set_visible(False)
            ax0.set_xlim(0, np.sum(data, axis=1).max())

            ax0.set_title("total latency", size=annotate_size)

            for i, (cat_name, color, hatch) in enumerate(zip(category_names, category_colors, hatch_list)):
                widths = data[:, i]
                starts = data_cum[:, i] - widths
                # print(starts, widths)

                edgecolor = scale_lightness(matplotlib.colors.ColorConverter.to_rgb(color), 0.2)
                rects = ax0.barh(labels, widths, left=starts, height=height,
                                 label=cat_name, color=color, edgecolor=edgecolor, hatch=hatch, alpha=0.8, zorder=0)

                # rects = ax.barh(labels, widths, left=starts, height=0.5, color='none', edgecolor='k', alpha=0.9, zorder=1)
                r, g, b, _ = color
                text_color = 'black'
                # TODO label text overlapped

                for j, rect in enumerate(rects):
                    d = data[j][i]
                    text = '{:.2f}%'.format(d * 100.0)
                    small_value = 0.06
                    h = rect.get_height()
                    w = rect.get_width()
                    if d < small_value :
                        continue
                    else:
                        rx, ry = rect.get_xy()
                        cx = rx + w / 2.0
                        cy = ry + h / 2.0
                        ax0.annotate(text, xy=(cx, cy), color=text_color,
                                     ha='center', va='center', size=annotate_size)

                # ax.legend(ncol=len(category_names), bbox_to_anchor=(0, 1),
                #          loc='lower left', fontsize='small', prop={'size': 20})
        else:
            ax1 = axis
            ax1.invert_yaxis()
            ax1.xaxis.set_visible(False)
            data = label2values[t][:2]
            data1 = [data]
            data1 = np.array(data1)
            data = [[math.log(x * 1000, 10)/3 for x in data]]
            data = np.array(data)
            data_cum = data.cumsum(axis=1)

            # ax.set_xlim(0, np.sum(data, axis=1).max())
            for i, (cat_name, color, hatch) in enumerate(zip(category_names[:2], category_colors[:2], hatch_list[:2])):
                widths = data[:, i]
                starts = data_cum[:, i] - widths

                edgecolor = scale_lightness(matplotlib.colors.ColorConverter.to_rgb(color), 0.2)
                rects = ax1.barh(labels, widths, left=starts, height=height,
                                 label=cat_name, color=color, edgecolor=edgecolor, hatch=hatch, alpha=0.8, zorder=0)

                # rects = ax.barh(labels, widths, left=starts, height=0.5, color='none', edgecolor='k', alpha=0.9, zorder=1)
                r, g, b, _ = color
                text_color = 'black'
                # TODO label text overlapped

                for j, rect in enumerate(rects):
                    # print(j, i)
                    d = data1[j][i]
                    text = '{:.2f}%'.format(d * 100.0)
                    h = rect.get_height()
                    w = rect.get_width();
                    rx, ry = rect.get_xy()
                    cx = rx + w / 2.0
                    cy = ry + h / 2.0

                    ax1.annotate(text, xy=(cx, cy), color=text_color,
                                 ha='center', va='center', size=annotate_size)
            ax1.set_title("RPC latency", size=annotate_size)
    if total_latency:
        lines, labels = axis.get_legend_handles_labels()
        fig.legend(lines[:len(category_names)], labels[:len(category_names)], loc='lower center', ncol= len(category_names), prop = { "size": 20 })
    else:
        lines, labels = axis.get_legend_handles_labels()
        fig.legend(lines[:2], labels[:2], loc='lower center',  ncol= 2, prop = { "size": 20 })

    #plt.tight_layout()
    #plt.subplots_adjust()
    if total_latency:
        pdf_file = "fig_total_latency.pdf"
    else:
        pdf_file = "fig_rpc_latency.pdf"
    pp = pdf.PdfPages(pdf_file)
    pp.savefig(fig, bbox_inches='tight', pad_inches=0)
    pp.close()


plot_bar_chart_latency(name_2_json, True)
plot_bar_chart_latency(name_2_json, False)