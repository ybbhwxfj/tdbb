#!/usr/bin/env python3

import argparse
import colorsys
import jsonpickle
import matplotlib
import matplotlib.backends.backend_pdf as pdf
import matplotlib.pyplot as plt
import numpy as np

DB_TYPE_DISTRIBUTED = ["db-sn", "db-d", "db-gro"]
LOOSE_BIND = 'unbind'
TIGHT_BIND = 'bind'
S_3X = 'unbind-s3'
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


def get_result_of_test(file, label):
    results = []
    f = open(file)
    for line in f.readlines():
        if line.isspace():
            continue
        r = from_json(line)
        if r['label'] == label:
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
    result = get_result_filter(file, row_filter=lambda row: filter_row(row, bind_type))
    name = {LOOSE_BIND: 'ub1x', TIGHT_BIND: 'b', S_3X: 'ub3x'}

    label2values = {}
    terminals = []
    for r in result:
        read = r['lt_read']
        read_dsb = r['lt_read_dsb']
        append = r['lt_app']
        append_rlb = r['lt_app_rlb']
        lock = r['lt_wait']
        latency = r['lt_part']
        terminal = r['terminals']
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
    category_names = ['R1', 'A1', 'R', 'A', 'L', 'O']

    labels = sorted(label2values.keys())
    data = []
    for t in label2values:
        data.append(label2values[t])

    labels = list(map(str, labels))
    data = np.array(data)
    category_colors = plt.colormaps['RdYlGn'](
        np.linspace(0.10, 0.85, data.shape[1]))
    data_cum = data.cumsum(axis=1)
    fig, ax = plt.subplots(figsize=(9.2, 5))
    ax.invert_yaxis()
    ax.xaxis.set_visible(False)
    ax.set_xlim(0, np.sum(data, axis=1).max())

    for i, (cat_name, color, hatch) in enumerate(zip(category_names, category_colors, hatch_list)):
        widths = data[:, i]
        starts = data_cum[:, i] - widths

        edgecolor = scale_lightness(matplotlib.colors.ColorConverter.to_rgb(color), 0.2)
        rects = ax.barh(labels, widths, left=starts, height=0.5,
                        label=cat_name, color=color, edgecolor=edgecolor, hatch=hatch, alpha=0.8, zorder=0)
        # rects = ax.barh(labels, widths, left=starts, height=0.5, color='none', edgecolor='k', alpha=0.9, zorder=1)
        r, g, b, _ = color
        text_color = 'black'
        # TODO label text overlapped

        for j, rect in enumerate(rects):
            d = data[j][i]
            text = '{:.2f}%'.format(d * 100.0)
            small_value = 0.03
            h = rect.get_height()
            w = rect.get_width();
            if d < small_value:
                xy = (0.5, 0.5)
                xytext = None
                if i % 2 == 1:
                    if i >= 1 and data[j][i - 1] < small_value:
                        xytext = (1.0, 1.3)
                    else:
                        xytext = (0.5, 1.3)
                else:
                    if len(data[j]) > i + 1 and data[j][i + 1] < small_value:
                        xytext = (-1.5, -0.3)
                    else:
                        xytext = (0.5, -0.3)
                arrow_style = matplotlib.patches.ArrowStyle("->", head_length=0.4, head_width=0.2, widthA=1.0,
                                                            widthB=1.0,
                                                            lengthA=0.2, lengthB=0.2, angleA=0, angleB=0)
                ax.annotate(text, xy=xy, xycoords=rect, color=text_color,
                            xytext=xytext, textcoords=rect,
                            # backgroundcolor='white',
                            arrowprops=dict(facecolor='black', arrowstyle=arrow_style),
                            horizontalalignment='center', verticalalignment='top',
                            )
            else:
                rx, ry = rect.get_xy()
                cx = rx + w / 2.0
                cy = ry + h / 2.0
                ax.annotate(text, xy=(cx, cy), color=text_color,
                            ha='center', va='center')

        ax.legend(ncol=len(category_names), bbox_to_anchor=(0, 1),
                  loc='lower left', fontsize='small', prop={'size': 20})
    plt.ylabel("terminals")
    plt.tight_layout()
    draw_pdf(fig, 'bar_' + name[bind_type])


def plot_binding_performance(file, x_attr, y_attr, label):
    MYSQL = 'mysql'
    shapes = {LOOSE_BIND: '--or', TIGHT_BIND: '-.vg', S_3X: '--sb'}
    legend_name = {LOOSE_BIND: 'DB-UB1x', TIGHT_BIND: 'DB-B', S_3X: 'DB-UB3x'}
    result1 = get_result_of_test(file, LOOSE_BIND)
    result2 = get_result_of_test(file, TIGHT_BIND)
    result3 = get_result_of_test(file, S_3X)
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
    for t in [TIGHT_BIND, LOOSE_BIND, S_3X]:
        if t in dbt2result:
            x = dbt2result[t][0]
            y = dbt2result[t][1]
            lc.ax.plot(x, y, shapes[t], label=t, linewidth=lc.line_width, markersize=lc.maker_size)
            legend.append(legend_name[t])
            if len(x_ticks) == 0:
                x_ticks = x

    plt.xlabel(x_attr)
    plt.ylabel("TPM")
    lc.ax.legend(legend, prop={"size": lc.legend_font})
    lc.ax.set_xticks(x_ticks)
    lc.draw(label + '_' + y_attr)


def plot_distributed_performance(file, y_attr, x_attr, label):
    lc = LineChart()
    shapes = {'db-sn': '-.or', 'db-d': '-vg', 'db-gro': '--sb'}
    legend_name = {'db-sn': 'DB-SN', 'db-d': 'DB-D', 'db-gro': 'DB-SP'}
    results = get_result_of_test(file, label)
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
    lc.ax.legend(legend, prop={"size": lc.legend_font})
    lc.ax.set_xticks(x_ticks)
    lc.draw('dist_' + y_attr)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='benchmark.')
    parser.add_argument('-j', '--json', type=str)
    plot_distributed_performance(file='result_sn.txt', label=DIST_NOT_CONTENTED, x_attr='terminals', y_attr='tpm')
    plot_binding_performance(file='result_s.txt', label=LABEL_BINDING, x_attr='terminals', y_attr='tpm')
    plot_bar_chart_latency(file='result_s.txt', bind_type=TIGHT_BIND)
    plot_bar_chart_latency(file='result_s.txt', bind_type=LOOSE_BIND)
    plot_bar_chart_latency(file='result_s.txt', bind_type=S_3X)
