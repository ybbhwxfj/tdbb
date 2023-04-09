import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf
from utils import util
import matplotlib.pylab as pylab


params = {'legend.fontsize': 'x-large',
          'axes.labelsize': 'x-large',
          'axes.titlesize':'x-large',
          'xtick.labelsize':'x-large',
          'ytick.labelsize':'x-large'}
pylab.rcParams.update(params)
linewidth = 4
markersize = 14
fontsize = 20

terminal_db_sn = util.json_attr_values(file='share-nothing/g7.2x_6_terminal.json', attr_names=['tpm', 'terminals'], filter=lambda row : row['db_type'] == 'db-sn')
terminal_db_d = util.json_attr_values(file='share-nothing/g7.2x_6_terminal.json', attr_names=['tpm', 'terminals'], filter=lambda row : row['db_type'] == 'db-d')

tpm_terminal_d = terminal_db_sn['tpm']
tpm_terminal_nd = terminal_db_d['tpm']
terminals = terminal_db_sn['terminals']


remote_db_sn = util.json_attr_values(file='share-nothing/g7.2x_6_remote.json', attr_names=['tpm', 'percent_remote'], filter=lambda row : row['db_type'] == 'db-sn')
remote_db_d = util.json_attr_values(file='share-nothing/g7.2x_6_remote.json', attr_names=['tpm', 'percent_remote'], filter=lambda row : row['db_type'] == 'db-d')
tpm_remote_wh_d = remote_db_sn['tpm']
tpm_remote_wh_nd = remote_db_d['tpm']
remote_wh = remote_db_sn['percent_remote']

fig = plt.figure()
ax = fig.subplots()
shapes = {'db-sn': '-.or', 'db-d': '-vg'}
x_ticks = terminals
legend = ['DB-SN-L', 'DB-SN-D']
ax.plot(terminals, tpm_terminal_d, '--o', color='tab:blue', label='db-sn-l', linewidth=linewidth, markersize=markersize)
ax.plot(terminals, tpm_terminal_nd, '-.v', color='tab:orange', label='db-sn-l', linewidth=linewidth, markersize=markersize)
plt.xlabel('number of terminals', size=fontsize)
plt.ylabel('TPM', size=fontsize)
ax.legend(legend)
ax.set_xticks(x_ticks)
pp = pdf.PdfPages('fig_sn_tpm_terminal.pdf')
pp.savefig(fig, bbox_inches='tight', pad_inches=0)
pp.close()

ax.clear()

x_ticks = ["{}%".format(x*100) for x in remote_wh]
ax.plot(x_ticks, tpm_remote_wh_d, '--o', color='tab:blue', label='db-sn-l', linewidth=linewidth, markersize=markersize)
ax.plot(x_ticks, tpm_remote_wh_nd, '-.v', color='tab:orange', label='db-sn-l', linewidth=linewidth, markersize=markersize)
plt.xlabel('distributed transactions', size=fontsize)
plt.ylabel('TPM', size=fontsize)
ax.legend(legend)
ax.set_xticks(x_ticks)
pp = pdf.PdfPages('fig_sn_tpm_remote.pdf')
pp.savefig(fig, bbox_inches='tight', pad_inches=0)
pp.close()
