import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf
from utils import util

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
name = ['g7.2x', 'g7.4x', 'g7.8x', 'g7.2x*2', 'g7.2x*4', 'g7.2x*8']

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
    ax.set_ylabel('TPM')
    ax.set_title('setting')
    ax.legend(title='scale up or scale out')

    pp = pdf.PdfPages("fig_scale_tpm_{}.pdf".format(dist))
    pp.savefig(fig, bbox_inches='tight')
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
    ax.set_ylabel('price/TPM')
    ax.set_title('setting')
    ax.legend(title='scale up or scale out')
    pp = pdf.PdfPages("fig_scale_price_per_tpm_{}.pdf".format(dist))
    pp.savefig(fig, bbox_inches='tight')
    pp.close()
    plt.clf()
