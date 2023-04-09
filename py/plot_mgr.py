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
fontsize=20

terminal_db_mgr = util.json_attr_values(file='mgr/mgr_l40_b100.txt', attr_names=['tpm', 'terminals'])
terminal_db_sdb = util.json_attr_values(file='mgr/sdb_tb_l40_b100.txt', attr_names=['tpm', 'terminals'])

tpm_terminal_mgr = terminal_db_mgr['tpm']
tpm_terminal_sdb = terminal_db_sdb['tpm']
terminals = terminal_db_mgr['terminals']

fig = plt.figure()
ax = fig.subplots()
x_ticks = terminals
legend = ['MGR', 'DB-S']
ax.plot(terminals, tpm_terminal_mgr, '--o', color='tab:blue', label='db-mgr', linewidth=linewidth, markersize=markersize)
ax.plot(terminals, tpm_terminal_sdb, '-.v', color='tab:orange', label='db-sdb', linewidth=linewidth, markersize=markersize)
plt.xlabel('number of terminals', size=fontsize)
plt.ylabel('TPM', size=fontsize)
ax.legend(legend)
ax.set_xticks(x_ticks)
pp = pdf.PdfPages('fig_mgr_tpm_terminal.pdf')
pp.savefig(fig, bbox_inches='tight', pad_inches=0)
pp.close()

