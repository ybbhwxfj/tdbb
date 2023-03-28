#!/usr/bin/env python3

import argparse
import colorsys

import jsonpickle
import matplotlib
import matplotlib.backends.backend_pdf as pdf
import matplotlib.pyplot as plt
import numpy as np

DB_TYPE_DISTRIBUTED = ["db-sn", "db-d", "db-gro"]
LOOSE_BIND = 'loose_bind'
TIGHT_BIND = 'tight_bind'
LABEL_MYSQL = 'mysql'
S_3X = 'lb-s3'
LABEL_BINDING = 'binding'
DIST_NOT_CONTENTED = 'dist-uc'
DIST_CONTENDED = 'dist-c'


def scale_lightness(rgb, scale_l):
    # convert rgb to hls
    h, l, s = colorsys.rgb_to_hls(*rgb)
    # manipulate h, l, s values and return as rgb
    return colorsys.hls_to_rgb(h, min(1, l * scale_l), s=s)


def draw_pdf(fig, name):
    pp = pdf.PdfPages("plot_" + name + ".pdf")
    pp.savefig(fig, bbox_inches='tight')
    pp.close()


class LineChart:
    def __init__(self):
        self.line_width = 2.5
        self.maker_size = 14
        self.legend_font = 12
        plt.clf()
        self.fig = plt.figure()
        self.ax = self.fig.subplots()

    def draw(self, name):
        draw_pdf(self.fig, name)


def from_json(json_text):
    return jsonpickle.decode(json_text)


def get_result_of_test(file, label=None):
    results = []
    f = open(file)
    for line in f.readlines():
        if line.isspace():
            continue
        r = from_json(line)
        if label is None or r['label'] == label:
            results.append(r)
    return results


def get_result_filter(file, row_filter):
    results = []
    f = open(file)
    for line in f.readlines():
        if line.isspace():
            continue
        r = from_json(line)
        if row_filter(r):
            results.append(r)
    return results


def filter_row(row, bind_type):
    return row['label'] == bind_type


def plot_bar_chart_latency(file, bind_type):
    plt.clf()

    fig, axis = plt.subplots(1, 2, figsize=(9.2, 5))

    result = get_result_filter(file, row_filter=lambda row: filter_row(row, bind_type))
    name = {LOOSE_BIND: 'lb', TIGHT_BIND: 'tb'}

    label2values = {}
    terminals = []
    for r in result:
        read = r['lt_read']
        read_dsb = r['lt_read_dsb']
        append = r['lt_app']
        append_rlb = r['lt_app_rlb']
        lock = r['lt_lock']
        latency = r['lt_part']
        terminal = r['percent_cached_tuple']
        terminals.append(terminal)
        array = []
        array.append((read - read_dsb) / latency)
        array.append((append - append_rlb) / latency)
        array.append(read_dsb / latency)
        array.append(append_rlb / latency)
        array.append(lock / latency)
        array.append((latency - read - append - lock) / latency)
        label2values[terminal] = array

    hatch_list = ['-', '.', '\\', '*', '/', 'x']
    category_names = ['R RPC', 'A RPC', 'R', 'A', 'L', 'O']

    labels = sorted(label2values.keys())
    data = []
    for t in label2values:
        data.append(label2values[t])

    labels = list(map(str, labels))
    data = np.array(data)
    category_colors = plt.colormaps['RdYlGn'](
        np.linspace(0.10, 0.85, data.shape[1]))
    data_cum = data.cumsum(axis=1)

    ax0 = axis[0]

    ax0.invert_yaxis()
    ax0.xaxis.set_visible(False)
    ax0.set_xlim(0, np.sum(data, axis=1).max())

    for i, (cat_name, color, hatch) in enumerate(zip(category_names, category_colors, hatch_list)):
        widths = data[:, i]
        starts = data_cum[:, i] - widths

        edgecolor = scale_lightness(matplotlib.colors.ColorConverter.to_rgb(color), 0.2)
        rects = ax0.barh(labels, widths, left=starts, height=0.5,
                         label=cat_name, color=color, edgecolor=edgecolor, hatch=hatch, alpha=0.8, zorder=0)

        # rects = ax.barh(labels, widths, left=starts, height=0.5, color='none', edgecolor='k', alpha=0.9, zorder=1)
        r, g, b, _ = color
        text_color = 'black'
        # TODO label text overlapped

        for j, rect in enumerate(rects):
            # print(j)
            d = data[j][i]
            text = '{:.2f}%'.format(d * 100.0)
            small_value = 0.04
            h = rect.get_height()
            w = rect.get_width();
            if d < small_value or (i == 1 or i == 0):
                continue
            else:
                rx, ry = rect.get_xy()
                cx = rx + w / 2.0
                cy = ry + h / 2.0
                ax0.annotate(text, xy=(cx, cy), color=text_color,
                             ha='center', va='center')
            ax0.set_ylabel("percentage of cached rows")
        # ax.legend(ncol=len(category_names), bbox_to_anchor=(0, 1),
        #          loc='lower left', fontsize='small', prop={'size': 20})
    plt.figlegend(ncol=len(category_names), bbox_to_anchor=(0.9, 1),
                  bbox_transform=fig.transFigure, fontsize='small', prop={'size': 15})

    ax1 = axis[1]
    ax1.invert_yaxis()
    ax1.yaxis.set_visible(False)
    ax1.xaxis.set_visible(False)
    # ax.set_xlim(0, np.sum(data, axis=1).max())
    for i, (cat_name, color, hatch) in enumerate(zip(category_names[:2], category_colors[:2], hatch_list[:2])):
        widths = data[:, i]
        starts = data_cum[:, i] - widths

        edgecolor = scale_lightness(matplotlib.colors.ColorConverter.to_rgb(color), 0.2)
        rects = ax1.barh(labels, widths, left=starts, height=0.5,
                         label=cat_name, color=color, edgecolor=edgecolor, hatch=hatch, alpha=0.8, zorder=0)

        # rects = ax.barh(labels, widths, left=starts, height=0.5, color='none', edgecolor='k', alpha=0.9, zorder=1)
        r, g, b, _ = color
        text_color = 'black'
        # TODO label text overlapped

        for j, rect in enumerate(rects):
            # print(j, i)
            d = data[j][i]
            text = '{:.2f}%'.format(d * 100.0)
            h = rect.get_height()
            w = rect.get_width();
            rx, ry = rect.get_xy()
            cx = rx + w / 2.0
            cy = ry + h / 2.0

            ax1.annotate(text, xy=(cx, cy), color=text_color,
                         ha='center', va='center')
    plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.05, hspace=None)
    # plt.tight_layout()
    draw_pdf(fig, 'bar_' + name[bind_type])


def plot_ccb_cache_performance(file, x_attr, y_attr):
    shapes = {LOOSE_BIND: '--or', TIGHT_BIND: '-.vg', LABEL_MYSQL: '--sb'}
    legend_name = {LOOSE_BIND: 'S-DB-LB', TIGHT_BIND: 'S-DB-TB', LABEL_MYSQL: 'MySQL'}
    result1 = get_result_of_test(file, LOOSE_BIND)
    result2 = get_result_of_test(file, TIGHT_BIND)
    result3 = get_result_of_test(file, LABEL_MYSQL)
    results = sorted(result1 + result2 + result3, key=lambda res: (res['label'], res[x_attr]))

    # print(results)

    dbt2result = {}
    for r in results:
        dbt = r['label']
        if dbt in dbt2result:
            dbt2result[dbt][0].append(r[x_attr])
            dbt2result[dbt][1].append(r[y_attr])
        else:
            dbt2result[dbt] = ([r[x_attr], ], [r[y_attr], ])

    lc = LineChart()

    x_ticks = []
    legend = []
    for t in [TIGHT_BIND, LOOSE_BIND, S_3X]:
        if t in dbt2result:
            x = dbt2result[t][0]
            y = dbt2result[t][1]
            # print(x, y)
            lc.ax.plot(x, y, shapes[t], label=t, linewidth=lc.line_width, markersize=lc.maker_size)
            legend.append(legend_name[t])
            if len(x_ticks) == 0:
                x_ticks = x

    plt.xlabel("CCB cache percentage of rows")
    plt.ylabel("TPM")
    lc.ax.legend(legend, prop={"size": lc.legend_font})
    lc.ax.set_xticks(x_ticks)
    lc.draw("plot_ccb_cache")


def plot_binding_performance(file, x_attr, y_attr, name):
    shapes = {LOOSE_BIND: '--or', TIGHT_BIND: '-.vg', LABEL_MYSQL: '--sb'}
    legend_name = {LOOSE_BIND: 'S-DB-LB', TIGHT_BIND: 'S-DB-TB', LABEL_MYSQL: 'MySQL'}
    x_labels = {'terminals': 'number of terminals'}
    result1 = get_result_of_test(file, LOOSE_BIND)
    result2 = get_result_of_test(file, TIGHT_BIND)
    result3 = get_result_of_test(file, LABEL_MYSQL)
    results = sorted(result1 + result2 + result3, key=lambda res: (res['label'], res[x_attr]))

    dbt2result = {}
    for r in results:
        dbt = r['label']
        if dbt in dbt2result:
            dbt2result[dbt][0].append(r[x_attr])
            dbt2result[dbt][1].append(r[y_attr])
        else:
            dbt2result[dbt] = ([r[x_attr], ], [r[y_attr], ])

    lc = LineChart()

    x_ticks = []
    legend = []
    for t in [TIGHT_BIND, LOOSE_BIND, LABEL_MYSQL]:
        if t in dbt2result:
            x = dbt2result[t][0]
            y = dbt2result[t][1]
            lc.ax.plot(x, y, shapes[t], label=t, linewidth=lc.line_width, markersize=lc.maker_size)
            legend.append(legend_name[t])
            if len(x_ticks) == 0:
                x_ticks = x

    plt.xlabel(x_labels[x_attr])
    plt.ylabel("TPM")
    lc.ax.legend(legend, prop={"size": lc.legend_font})
    lc.ax.set_xticks(x_ticks)
    lc.draw(name + '_' + y_attr)


def plot_distributed_performance(file, y_attr, x_attr, label):
    lc = LineChart()
    shapes = {'db-sn': '-.or', 'db-d': '-vg'}
    legend_name = {'db-sn': 'DB-SN', 'db-d': 'DB-D'}
    x_labels = {'terminals': 'number of terminals', 'percent_remote': 'percent of distributed transactions', }
    results = get_result_of_test(file)
    results = sorted(results, key=lambda res: (res['db_type'], res[x_attr]))

    dbt2result = {}
    for r in results:
        dbt = r['db_type']
        if dbt in dbt2result:
            dbt2result[dbt][0].append(r[x_attr])
            dbt2result[dbt][1].append(r[y_attr])
        else:
            dbt2result[dbt] = ([r[x_attr], ], [r[y_attr], ])

    x_ticks = []
    legend = []
    for t in DB_TYPE_DISTRIBUTED:
        if t in dbt2result:
            x = dbt2result[t][0]
            y = dbt2result[t][1]
            lc.ax.plot(x, y, shapes[t], label=t, linewidth=lc.line_width, markersize=lc.maker_size)
            legend.append(legend_name[t])
            if len(x_ticks) == 0:
                x_ticks = x

    plt.xlabel(x_labels[x_attr])
    plt.ylabel("TPM")
    lc.ax.legend(legend, prop={"size": lc.legend_font})
    lc.ax.set_xticks(x_ticks)
    lc.draw('dist_' + label + '_' + y_attr)


def plot_performance_price():
    perf_price = [1, 2, 3, 4, 5, 6, 7, 8]
    conf_types = ['g7.1x', 'g7.2x', 'g7.4x', 'g7.8x', 'g7.16x',
                  'g7.2x X 2', 'g7.2x X 4', 'g7.2x X 8']
    bar_labels = ['red', 'blue', '_red', 'orange'
                                         'red', 'blue', '_red', 'orange']
    bar_colors = ['tab:red', 'tab:blue',
                  'tab:red', 'tab:orange', 'tab:red',
                  'tab:blue', 'tab:red', 'tab:orange']
    plt.bar(conf_types, perf_price, label=bar_labels, color=bar_colors)
    plt.ylabel("Price(RMB YUAN)/TPM")
    plt.xlabel("ECS Configuration")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='benchmark.')
    parser.add_argument('-j', '--json', type=str, help='json file path')
    parser.add_argument('-t', '--type', type=str, help='plot type cache/tb/lb/db-s/db-sn/db-sn-dist')
    args = parser.parse_args()

    json_path = getattr(args, 'json')
    type = getattr(args, 'type')
    if type == "tb":
        plot_bar_chart_latency(file=json_path, bind_type=TIGHT_BIND)
    elif type == "lb":
        plot_bar_chart_latency(file=json_path, bind_type=LOOSE_BIND)
    elif type == "db-sn":
        plot_distributed_performance(file=json_path, label="db_sn", x_attr='terminals', y_attr='tpm')
    elif type == "db-sn-dist":
        plot_distributed_performance(file=json_path, label="db_sn_dist_tx", x_attr='percent_remote', y_attr='tpm')
    elif type == "cache":
        plot_ccb_cache_performance(file=json_path, x_attr='percent_cached_tuple', y_attr='tpm')
    elif type == "db-s":
        plot_binding_performance(file=json_path, name="binding", x_attr='terminals', y_attr='tpm')
    elif type == "perf-price":
        plot_performance_price()
