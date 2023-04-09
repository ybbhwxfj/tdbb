import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf
from utils import util
import matplotlib.pylab as pylab

fontsize = 20
params = {'legend.fontsize': 'x-large',
          'axes.labelsize': 'x-large',
          'axes.titlesize':'x-large',
          'xtick.labelsize':'x-large',
          'ytick.labelsize':'x-large'}
pylab.rcParams.update(params)

result1 = util.json_attr_values(file='scale/scale_remote_low.json', attr_names=['tpm'])
tpm_r_l = result1['tpm']
result2 = util.json_attr_values(file='scale/scale_remote_high.json', attr_names=['tpm'])
tpm_r_h = result2['tpm']

price = [1044.64*3, 2089.28*3, 4178.56*3, 1044.64*2*3, 1044.64*4*3, 1044.64*8*3]

tpm2 = [
            # low level remote access,
            tpm_r_l,
            # high level remote access
            tpm_r_h
        ]
name = ['2x', '4x', '8x', '2x*2', '2x*4', '2x*8']

low_or_high = ['remote_low', 'remote_high']

bar_labels = ['base', 'scale up', '_scale up', 'scale out',  '_scale out', '_scale out']
bar_colors = ['lightcyan', 'cornflowerblue', 'cornflowerblue', 'blue', 'blue', 'blue']
bar_colors2 = ['mistyrose', 'salmon', 'salmon', 'tomato', 'tomato', 'tomato']
hatch = ['\\',  '//', '//', '+', '+', '+']

for j in range(len(tpm2)):
    fig, ax = plt.subplots()
    dist = low_or_high[j]

    tpm = tpm2[j]
    ax.bar(name, tpm,
           label=bar_labels,
           color=bar_colors,
           hatch=hatch,
           # use alpha=.99 to render hatches when exporting in PDF to avoid bugs
           alpha=.99)
    ax.set_ylabel('TPM', size=fontsize)
    ax.set_xlabel('scale up/out setting', size=fontsize)
    ax.legend()

    pp = pdf.PdfPages("fig_scale_tpm_{}.pdf".format(dist))
    pp.savefig(fig, bbox_inches='tight', pad_inches=0)
    pp.close()
    ax.clear()
    plt.clf()

    fig, ax = plt.subplots()
    price_per_tpm = []
    for i in range(len(tpm)):
        price_per_tpm.append(price[i]/tpm[i])

    ax.bar(name, price_per_tpm,
           label=bar_labels,
           color=bar_colors2,
           hatch=hatch,
           alpha=.99
           )
    ax.set_ylabel('price/TPM', size=fontsize)
    ax.set_xlabel('scale up/out setting', size=fontsize)
    ax.legend()
    pp = pdf.PdfPages("fig_scale_price_per_tpm_{}.pdf".format(dist))
    pp.savefig(fig, bbox_inches='tight', pad_inches=0)
    pp.close()
    plt.clf()
