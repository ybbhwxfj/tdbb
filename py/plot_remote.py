import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf

from utils import util

linewidth = 2.2
markersize = 12

t80_scr_tr = util.json_attr_values(file='remote/g7.2x4_t80_scr_remote.txt', attr_names=['tpm', 'percent_remote'])
t80_sn_tr = util.json_attr_values(file='remote/g7.2x4_t80_sn_remote.txt', attr_names=['tpm', 'percent_remote'])

t160_scr_tr = util.json_attr_values(file='remote/g7.2x4_t160_scr_remote.txt', attr_names=['tpm', 'percent_remote'])
t160_sn_tr = util.json_attr_values(file='remote/g7.2x4_t160_sn_remote.txt', attr_names=['tpm', 'percent_remote'])

result = {
    "t80": {
        "scr": t80_scr_tr,
        "sn": t80_sn_tr
    },
    "t160": {
        "scr": t160_scr_tr,
        "sn": t160_sn_tr
    }
}


for key in result.keys():
    tr = result[key]
    scr_tr = tr["scr"]
    sn_tr = tr["sn"]
    fig = plt.figure()
    ax = fig.subplots()

    x_ticks = sn_tr['percent_remote']
    legend = ['DB-SCR', 'DB-SN']
    ax.plot(scr_tr['percent_remote'], scr_tr['tpm'], '--o', color='tab:blue', label='db-scr', linewidth=linewidth, markersize=markersize)
    ax.plot(sn_tr['percent_remote'], sn_tr['tpm'], '-.v', color='tab:orange', label='db-sn', linewidth=linewidth, markersize=markersize)
    plt.xlabel('percentage of transactions access remote shards')
    plt.ylabel('TPM')
    ax.legend(legend)
    ax.set_xticks(x_ticks)
    pp = pdf.PdfPages('fig_access_remote_{}.pdf'.format(key))
    pp.savefig(fig, bbox_inches='tight')
    pp.close()

    plt.clf()
