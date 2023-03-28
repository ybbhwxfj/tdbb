import matplotlib.pyplot as plt
import matplotlib.backends.backend_pdf as pdf
from utils import util

linewidth=2.2
markersize=12

terminal_db_sn = util.json_attr_values(file='share-nothing/g7.4x_6_terminal.json', attr_names=['tpm', 'terminals'], filter=lambda row : row['db_type'] == 'db-sn')
terminal_db_d = util.json_attr_values(file='share-nothing/g7.4x_6_terminal.json', attr_names=['tpm', 'terminals'], filter=lambda row : row['db_type'] == 'db-d')

tpm_terminal_d = terminal_db_sn['tpm']
tpm_terminal_nd = terminal_db_d['tpm']
terminals = terminal_db_sn['terminals']


remote_db_sn = util.json_attr_values(file='share-nothing/g7.4x_6_remote.json', attr_names=['tpm', 'percent_remote'], filter=lambda row : row['db_type'] == 'db-sn')
remote_db_d = util.json_attr_values(file='share-nothing/g7.4x_6_remote.json', attr_names=['tpm', 'percent_remote'], filter=lambda row : row['db_type'] == 'db-d')
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
plt.xlabel('number of terminals')
plt.ylabel('TPM')
ax.legend(legend)
ax.set_xticks(x_ticks)
pp = pdf.PdfPages('fig_sn_tpm_terminal.pdf')
pp.savefig(fig, bbox_inches='tight')
pp.close()

ax.clear()

ax.plot(remote_wh, tpm_remote_wh_d, '--o', color='tab:blue', label='db-sn-l', linewidth=linewidth, markersize=markersize)
ax.plot(remote_wh, tpm_remote_wh_nd, '-.v', color='tab:orange', label='db-sn-l', linewidth=linewidth, markersize=markersize)
plt.xlabel('percent ratio of accessing remote warehouse')
plt.ylabel('TPM')
ax.legend(legend)
ax.set_xticks(remote_wh)
pp = pdf.PdfPages('fig_sn_tpm_remote.pdf')
pp.savefig(fig, bbox_inches='tight')
pp.close()
