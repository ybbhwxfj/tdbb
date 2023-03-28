import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf

from utils import util

linewidth = 2.2
markersize = 12

t80_s_tro = util.json_attr_values(file='readonly/g7.2x4_t80_s_readonly.txt', attr_names=['tpm', 'percent_read_only'])
t80_scr_tro = util.json_attr_values(file='readonly/g7.2x4_t80_scr_readonly.txt', attr_names=['tpm', 'percent_read_only'])

t160_s_tro = util.json_attr_values(file='readonly/g7.2x4_t160_s_readonly.txt', attr_names=['tpm', 'percent_read_only'])
t160_scr_tro = util.json_attr_values(file='readonly/g7.2x4_t160_scr_readonly.txt', attr_names=['tpm', 'percent_read_only'])

result = {
    "t80": {
        "s": t80_s_tro,
        "scr": t80_scr_tro,
        "terminal": 80,
    },
    "t160": {
        "s": t160_s_tro,
        "scr": t160_scr_tro,
        "terminal": 160,
    }
}


for key in result.keys():
    tr = result[key]
    scr_tr = tr["scr"]
    sn_tr = tr["s"]
    num_terminal = tr["terminal"]
    fig = plt.figure()
    ax = fig.subplots()

    x_ticks = []
    x_values = []
    for p in sn_tr['percent_read_only']:
        num = int(float(p) * float(num_terminal))
        x_values.append(num)
        x_ticks.append("+{}".format(num))
    legend = ['DB-SCR', 'DB-S']
    ax.plot(x_ticks, scr_tr['tpm'], '--o', color='tab:blue', label='db-scr', linewidth=linewidth, markersize=markersize)
    ax.plot(x_ticks, sn_tr['tpm'], '-.v', color='tab:orange', label='db-s', linewidth=linewidth, markersize=markersize)
    plt.xlabel('additional terminals issue readonly transactions')
    plt.ylabel('TPM')
    ax.legend(legend)
    ax.set_xticks(x_ticks)
    pp = pdf.PdfPages('fig_readonly_{}.pdf'.format(key))
    pp.savefig(fig, bbox_inches='tight')
    pp.close()

    plt.clf()
