#!/usr/bin/env python3
import argparse
import json
import os
import paramiko
import pathlib
import re
import shutil
import stat
import subprocess
import time

# NUM_ITEM = 100
# CUST_PER_DIST = 10
# DIST_PER_WARE = 1
# ORD_PER_DIST = 10


NUM_ITEM = 100000
CUST_PER_DIST = 1000
DIST_PER_WARE = 10
ORD_PER_DIST = 1000
MAX_NUM_ITEMS = 15
HOT_ITEM_NUM = 10
NUM_WAREHOUSE = 40
NUM_TRANSACTIONS = 100000
APPEND_LOG_ENTRIES_BATCH_MIN = 10

THREADS_ASYNC_CONTEXT = 10
THREADS_IO = 10
CONNECTIONS_PER_PEER = 10
MYSQL_TPCC_RUN_SECONDS = 100
MYSQL_MAX_CONNECTIONS = 2500

SSH_SERVER_PORT = 22
RETRY_AFTER_EXCEPTION = 500
WAN_LATENCY_DELAY_WAIT_MS = 0
CACHED_TUPLE_PERCENTAGE = 0.8
AZ_RTT_LATENCY_MS = 10
CALVIN_EPOCH_MS = 50
NUM_OUTPUT_RESULT = 100
DEFAULT_NUM_TERMINAL = 200

NODE_MYSQL_CONF = 'node.mysql.conf.json'
NODE_SHARE_TIGHT_CONF = "node.conf.sdb.b.json"
NODE_SHARE_LOOSE_CONF = "node.conf.sdb.ub.json"
NODE_SHARE_NOTHING_CONF = "node.conf.sndb.json"
USER_CONF = "user.conf.json"

BLOCK_CONF = "block.json"
TPCC_SCHEMA = 'tpcc_schema.json'
BINARY_BLOCK_DB = 'block-db'
BINARY_BLOCK_CLIENT = 'block-client'
TMPDIR = "/tmp/block"
STARTUP_SCRIPT = "block.sh"
RUN_BLOCK = "run_block"

BLOCK_PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY_PATH = os.path.join(BLOCK_PROJECT_PATH, 'py')
BINARY_PATH = os.path.join(BLOCK_PROJECT_PATH, 'bin')
CONF_PATH = os.path.join(BLOCK_PROJECT_PATH, 'conf')

TEST_CACHED_PERCENTAGE = [0.2, 0.8]
MYSQL_TERMINALS = [50, 100, 150, 200, 250, 300]
BIND_TERMINALS = [50, 100, 150, 200, 250, 300]
TERMINALS = [100, 200, 300, 400, 500, 600]

WAREHOUSES = [20, 40, 60, 80, 100]
CONTENTED_TERMINALS = [50, 100, 150, 200, 250, 300]

TEST_NODE_CONF_BINDING = {NODE_SHARE_LOOSE_CONF: 'l', NODE_SHARE_TIGHT_CONF: 't'}
TEST_NODE_CONF_DISTRIBUTED = {NODE_SHARE_NOTHING_CONF: 'dist'}
DB_S = 'db-s'
DB_TYPE_DISTRIBUTED = ['db-sn', 'db-d']

MYSQL_PORT = 3306
MYSQL_X_PORT = 33060  # mysqldump needed
MYSQL_MGR_PORT = 33061


def config_file_name(distributed=True, tight_binding=True):
    if distributed:
        return NODE_SHARE_NOTHING_CONF
    elif tight_binding:
        return NODE_SHARE_TIGHT_CONF
    else:
        return NODE_SHARE_LOOSE_CONF


class SFTPClientWrapper(paramiko.SFTPClient):
    def put_dir(self, source, target):
        """ Uploads the contents of the source directory to the target path. The
            target directory needs to exists. All subdirectories in source are
            created under target.
        """
        for item in os.listdir(source):
            if os.path.isfile(os.path.join(source, item)):
                self.put(os.path.join(source, item), '%s/%s' % (target, item))
            else:
                self.mkdir('%s/%s' % (target, item), ignore_existing=True)
                self.put_dir(os.path.join(source, item), '%s/%s' % (target, item))

    def mkdir(self, path, mode=511, ignore_existing=False):
        """ Augments mkdir by adding an option to not fail if the folder exists  """
        try:
            super(SFTPClientWrapper, self).mkdir(path, mode)
        except IOError:
            if ignore_existing:
                pass
            else:
                raise


def get_user_password():
    user_conf = os.path.join(CONF_PATH, USER_CONF)
    user_json = load_json_file(user_conf)
    user = user_json['user']
    password = user_json['password']
    return user, password


def run_shell_command(shell_command):
    print("local shell command run")
    print(shell_command)
    p = subprocess.run(shell_command, shell=True,
                       capture_output=True, text=True)
    print(p.stdout)
    print(p.stderr)
    return p.returncode, p.stdout, p.stderr


def run_command_remote(username, password, host, shell_command):
    print("remote shell command run", username, password, host)
    print(shell_command)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    if username == 'root':
        key_file = '/root/.ssh/id_rsa'
    else:
        key_file = '/home/{}/.ssh/id_rsa'.format(username)
    if password is None or password == '':
        client.connect(host, username=username, password=password, key_filename=key_file)
    else:
        client.connect(host, username=username, password=password)

    stdin, stdout, stderr = client.exec_command(shell_command)
    exit_code = stdout.channel.recv_exit_status()
    outlines = stdout.readlines()
    outerrlines = stderr.readlines()
    # print(outlines)
    # print(outerrlines)
    client.close()
    print('command exit code:' + str(exit_code))
    for line in outlines:
        print(line)
    for line in outerrlines:
        print(line)

    return exit_code, outlines, outerrlines


def rsync_to_remote(source_path, username, host, target_path):
    option = ' -e \"ssh -o StrictHostKeyChecking=no\" '
    if os.path.isdir(source_path):
        option = option + ' -r '
    rsync_cmd = 'rsync {} {} {}@{}:{}'.format(option, source_path, username, host, target_path)
    stream = os.popen(rsync_cmd)
    output = stream.read()
    print(output)


def load_json_file(path):
    with open(path, 'r') as file:
        text = file.read()
        return json.loads(text)


def gen_priority(node_server):
    az_set = {}
    az_vec = []
    for node in node_server:
        az = node['zone_name']
        if node['zone_name'] not in az_set:
            az_set[az] = 1
            az_vec.append(az)
        else:
            az_set[az] = az_set[az] + 1

    sorted(az_vec, key=lambda az_name: az_set[az_name])
    # az name to priority
    az_priority = {}
    for i in range(len(az_vec)):
        az_priority[az_vec[i]] = i + 1
    return az_priority


def run_bench(num_terminal=DEFAULT_NUM_TERMINAL,
              percent_distributed=0.0,
              percent_hot_item=0.0,
              num_warehouse=NUM_WAREHOUSE,
              num_item=NUM_ITEM,
              dist_per_ware=DIST_PER_WARE,
              db_type='db-s', label='xxx',
              conf_file=NODE_SHARE_TIGHT_CONF,
              tight_binding=True,
              cached_tuple_percentage=CACHED_TUPLE_PERCENTAGE,
              ):
    path_node_configure_file = os.path.join(CONF_PATH, conf_file)
    conf_map = load_json_file(path_node_configure_file)
    name2path = {}
    node_server = conf_map['node_server_list'].copy()
    node_client = conf_map['node_client'].copy()
    node_panel = conf_map['node_panel'].copy()

    node_list = node_server.copy()
    node_list.append(node_client)
    node_list.append(node_panel)

    az_priority = gen_priority(node_server)
    for node in node_server:
        az = node['zone_name']
        if node['zone_name'] in az_priority:
            node['priority'] = az_priority[az]

    tpcc_config = {
        'num_warehouse': num_warehouse,
        'num_item': num_item,
        'num_order_initialize_per_district': ORD_PER_DIST,
        'num_district_per_warehouse': dist_per_ware,
        'num_max_order_line': MAX_NUM_ITEMS,
        'num_terminal': num_terminal,
        'num_customer_per_district': CUST_PER_DIST,
        'num_new_order': NUM_TRANSACTIONS,
        'percent_non_exist_item': 0,
        'percent_hot_item': percent_hot_item,
        'hot_item_num': HOT_ITEM_NUM,
        'percent_distributed': percent_distributed,
        "raft_follow_tick_num": 20,
        "raft_leader_tick_ms": AZ_RTT_LATENCY_MS,
        "calvin_epoch_ms": CALVIN_EPOCH_MS,
        "num_output_result": NUM_OUTPUT_RESULT,
        "az_rtt_ms": AZ_RTT_LATENCY_MS,
        "flow_control_rtt_count": 4,
    }

    test_conf = {
        'wan_latency_delay_wait_ms': WAN_LATENCY_DELAY_WAIT_MS,
        'cached_tuple_percentage': cached_tuple_percentage,
        'label': label,
        'parameter': ''
    }

    for node in node_list:
        is_client = len(node['block_type']) == 1 and node['block_type'][0] == 'CLIENT'
        (name, path) = configure_node(
            server_node_conf_list=node_server,
            client_node_conf=node_client,
            panel_node_conf=node_panel,
            tpcc_config=tpcc_config,
            test_conf=test_conf,
            address=node['address'],
            node_name=node['node_name'],
            binding_node_name=node['node_name'],
            db_type=db_type,
            is_backend=not is_client,
            label=label,
            az_priority=az_priority
        )
        name2path[name] = path

    output_result = {
        'label': label,
        'db_type': db_type,
        'terminals': num_terminal,
        'binding': tight_binding,
        'warehouses': num_warehouse,
        'num_item': num_item,
        'percent_distributed': percent_distributed,
        'percent_hot_item': percent_hot_item,
        'cached_tuple_percentage': cached_tuple_percentage,
    }

    servers = node_server.copy()
    servers.append(node_panel)
    for node in servers:
        node_name = node['node_name']
        node_path = name2path[node_name]

        run_block(node['address'], node_path, True, output_result)

    client_path = name2path[node_client['node_name']]
    run_block(node_client['address'], client_path, False, output_result)
    json_file = os.path.join(client_path, 'output.txt')
    with open(json_file, 'w') as file:
        file.write(json.dumps(output_result) + '\n')
    file.close()


def run_block(address, run_dir, backend, output_result):
    if backend:
        run_cmd = 'cd {}\n' \
                  'ulimit -c unlimited\n' \
                  ' nohup {}/{} > be.out 2>&1 </dev/null & \n '.format(run_dir,
                                                                       run_dir, STARTUP_SCRIPT)
    else:
        run_cmd = 'cd {}\n' \
                  'ulimit -c unlimited\n' \
                  ' nohup {}/{} > fe.out 2>&1 </dev/null & \n'.format(run_dir,
                                                                      run_dir, STARTUP_SCRIPT)
    if backend:
        user, password = get_user_password()
        # print(run_cmd)
        run_command_remote(username=user, password=password, host=address, shell_command=run_cmd)
    else:
        run_shell_command(shell_command=run_cmd)
        result_file = os.path.join(run_dir, 'result.json')
        result_json_path = pathlib.Path(result_file)
        while not result_json_path.is_file():
            time.sleep(2)

        result = load_json_file(result_file)

        output_result['abort'] = result['abort']
        output_result['tpm'] = result['tpm']
        output_result['lt'] = result['lt']
        output_result['lt_read'] = result['lt_read']
        output_result['lt_read_dsb'] = result['lt_read_dsb']
        output_result['lt_app'] = result['lt_app']
        output_result['lt_app_rlb'] = result['lt_app_rlb']
        output_result['lt_lock'] = result['lt_lock']
        output_result['lt_part'] = result['lt_part']


def configure_node(
        server_node_conf_list,
        client_node_conf,
        panel_node_conf,
        tpcc_config,
        test_conf,
        address,
        node_name,
        binding_node_name,
        db_type,
        is_backend,
        label,
        az_priority
):
    ts = time.strftime('%a_%d_%b_%Y_%H_%M_%S', time.gmtime())
    name = '{}_{}_{}__{}'.format(label, db_type, node_name, ts)

    run_path = os.path.join(PY_PATH, RUN_BLOCK)
    install_path = os.path.join(run_path, name)

    local_tmp = os.path.join(TMPDIR, name)

    user, password = get_user_password()
    if not os.path.exists(local_tmp):
        os.makedirs(local_tmp)

    db_path = install_path
    block_config = {
        'node_name': node_name,
        'binding_node_name': binding_node_name,
        'db_path': db_path,
        'schema_path': os.path.join(install_path, TPCC_SCHEMA),
        'label': label,
        'az_priority': az_priority,
        'threads_async_context': THREADS_ASYNC_CONTEXT,
        'threads_io': THREADS_IO,
        'connections_per_peer': CONNECTIONS_PER_PEER,
        'append_log_entries_batch_min': APPEND_LOG_ENTRIES_BATCH_MIN,
    }

    configure = {
        'block': block_config,
        'param': tpcc_config,
        'test': test_conf,
        'node_server_list': server_node_conf_list,
        'node_client': client_node_conf,
        'node_panel': panel_node_conf,
    }

    block_conf_file = os.path.join(local_tmp, BLOCK_CONF)
    with open(block_conf_file, 'w') as file:
        file.write(json.dumps(configure, indent=4))
    file.close()

    schema_path = os.path.join(CONF_PATH, TPCC_SCHEMA)
    shutil.copy(schema_path, local_tmp)

    script_path = os.path.join(local_tmp, STARTUP_SCRIPT)
    # create run path
    run_cmd = 'export PATH={}/bin:$PATH \n'.format(run_path)
    argument = '--dbtype ' + db_type
    if is_backend:
        run_cmd = run_cmd + '{} {} --conf {}/{} \n'.format(
            BINARY_BLOCK_DB, argument, install_path, BLOCK_CONF)
    else:
        run_cmd = run_cmd + '{} {} --conf {}/{} \n'.format(
            BINARY_BLOCK_CLIENT, argument, install_path, BLOCK_CONF)
    with open(script_path, 'w', 777) as file:
        file.write(run_cmd)
    os.chmod(script_path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

    cmd = 'mkdir -p {}'.format(run_path)
    run_command_remote(username=user, password=password, host=address, shell_command=cmd)
    rsync_to_remote(source_path=BINARY_PATH, username=user, host=address, target_path=run_path)
    rsync_to_remote(source_path=local_tmp, username=user, host=address, target_path=run_path)

    # start up shell
    chmod_cmd = 'cd {} \n ' \
                'chmod 755 {}'.format(install_path,
                                      STARTUP_SCRIPT
                                      )

    run_command_remote(username=user, password=password, host=address, shell_command=chmod_cmd)
    return node_name, install_path


def clean_all(conf_file, clean_run_dir=False):
    path_node_configure_file = os.path.join(CONF_PATH, conf_file)
    conf_map = load_json_file(path_node_configure_file)
    node_server = conf_map['node_server_list'].copy()
    node_server.append(conf_map['node_panel'].copy())
    user, password = get_user_password()

    for node in node_server:
        clean_cmd = 'pkill -f {} || true \n'.format(BINARY_BLOCK_DB)
        run_command_remote(user, password, node['address'], clean_cmd)
        clean_cmd = 'pkill -f {} || true \n'.format(BINARY_BLOCK_CLIENT)
        run_command_remote(user, password, node['address'], clean_cmd)
        clean_cmd = 'rm -rf /tmp/block \n'
        if clean_run_dir:
            run_path = os.path.join(PY_PATH, RUN_BLOCK)
            clean_cmd = clean_cmd + 'rm -rf {}\n'.format(run_path)
        run_command_remote(user, password, node['address'], clean_cmd)


def evaluation_contention(conf_path):
    percent_distributed = 0.1
    for db_type in DB_TYPE_DISTRIBUTED:
        for percent_hot in [0.0, 0.1, 0.2]:
            term = DEFAULT_NUM_TERMINAL
            clean_all(conf_path)
            label = 'hot_' + str(percent_hot)
            run_bench(num_terminal=term,
                      num_warehouse=NUM_WAREHOUSE,
                      num_item=NUM_ITEM,
                      percent_distributed=percent_distributed,
                      percent_hot_item=percent_hot,
                      db_type=db_type,
                      label=label,
                      conf_file=conf_path,
                      tight_binding=True)


def evaluation_warehouse(conf_path):
    percent_distributed = 0.1
    for db_type in DB_TYPE_DISTRIBUTED:
        for warehouse in WAREHOUSES:
            term = DEFAULT_NUM_TERMINAL
            clean_all(conf_path)
            label = 'wh_' + str(warehouse)
            run_bench(num_terminal=term,
                      num_warehouse=warehouse,
                      num_item=NUM_ITEM,
                      percent_distributed=percent_distributed,
                      db_type=db_type,
                      label=label,
                      conf_file=conf_path,
                      tight_binding=True)


def evaluation_distributed(conf_path):
    percent_distributed = 0.1
    for wh_item in [
        (NUM_WAREHOUSE, NUM_ITEM, '-uc', TERMINALS)
    ]:
        num_warehouse = wh_item[0]
        num_item = wh_item[1]
        for db_type in DB_TYPE_DISTRIBUTED:
            for term in wh_item[3]:
                clean_all(conf_path)
                label = 'dist' + wh_item[2]
                run_bench(num_terminal=term,
                          num_warehouse=num_warehouse,
                          num_item=num_item,
                          percent_distributed=percent_distributed,
                          db_type=db_type,
                          label=label,
                          conf_file=conf_path,
                          tight_binding=True)


def evaluation_block_binding(conf_path, tight_binding=True):
    if tight_binding:
        label = 'bind'
    else:
        label = 'unbind'
    db_type = DB_S
    num_warehouse = NUM_WAREHOUSE
    percent_distributed = 0.0
    for term in BIND_TERMINALS:
        for percentage in TEST_CACHED_PERCENTAGE:
            clean_all(conf_path)
            run_bench(num_terminal=term,
                      num_warehouse=num_warehouse,
                      percent_distributed=percent_distributed,
                      db_type=db_type,
                      label=label,
                      conf_file=conf_path,
                      tight_binding=tight_binding,
                      cached_tuple_percentage=percentage)


def tc_command(ip, delay):
    cmd = '''
interface=eth0
ip={}
delay={}ms

tc qdisc add dev $interface root handle 1: prio
tc filter add dev $interface parent 1:0 protocol ip prio 1 u32 match ip dst $ip flowid 2:1
tc qdisc add dev $interface parent 1:1 handle 2: netem delay $delay
    '''.format(ip, delay)
    return cmd


def tc_command_list(address_list, delay):
    cmd = ""
    for address in address_list:
        cmd = cmd + tc_command(address, delay)
    return cmd


def get_nodes(conf_file, all_node=True):
    path_node_configure_file = os.path.join(CONF_PATH, conf_file)
    conf_map = load_json_file(path_node_configure_file)
    node_server = conf_map['node_server_list'].copy()
    node = node_server
    if all_node:
        node_client = conf_map['node_client'].copy()
        node.append(node_client)
    return node


def config_latency(conf_file, latency, wan=True):
    node = get_nodes(conf_file, all_node=False)
    user, password = get_user_password()
    for n in node:
        access_address = []
        for n1 in node:
            if n['node_name'] != n1['node_name']:
                if wan:
                    if n['zone_name'] != n1['zone_name']:
                        access_address.append(n1['address'])
                else:
                    if n['zone_name'] == n1['zone_name']:
                        access_address.append(n1['address'])
        cmd = tc_command_list(access_address, latency)
        run_command_remote(username=user, password=password, host=n['address'], shell_command=cmd)


def execute_command(conf_file, shell_command):
    node = get_nodes(conf_file)
    user, password = get_user_password()
    for n in node:
        print('=====run command=====:, address ' + n['address'])
        run_command_remote(username=user, password=password, host=n['address'], shell_command=shell_command)


def mysql_load_data(conf_file, warehouse):
    mysql_init_bench_tpcc(conf_file, warehouse=warehouse)


def mysql_run_evaluation(conf_file, warehouse):
    for t in MYSQL_TERMINALS:
        mysql_run_bench_tpcc(conf_file, terminal=t, warehouse=warehouse)


# local_address s1:33061
# group_seeds s1:33061,s2:33061,s3:33061
def mysql_cnf(datadir, server_id, local_address, group_seeds):
    my_cnf = '''\
[mysql]
port            = {}
socket          = {}/mysqld.sock
[mysqldump]
port            = {}
socket          = {}/mysqld.sock
[mysqld]
bind_address    = "0.0.0.0"
port            = {}
pid_file        = {}/mysqld.pid
socket          = {}/mysqld.sock
mysqlx_socket   = {}/mysqlx.sock
log_error       = {}/error.log
datadir         = {}/data
mysqlx_bind_address = "0.0.0.0"
mysqlx_port = {}
connect_timeout = 20
net_read_timeout = 60
max_connections = {}
transaction_alloc_block_size=8192
transaction_isolation=SERIALIZABLE
innodb_buffer_pool_size=20GB
# maximum value of max_prepared_stmt_count
max_prepared_stmt_count=1048576
disabled_storage_engines="MyISAM,BLACKHOLE,FEDERATED,ARCHIVE,MEMORY"
server_id={}
gtid_mode=ON
enforce_gtid_consistency=ON
binlog_checksum=NONE
plugin_load_add='group_replication.so'
# MySQL default value is caching_sha2_password, this need a RSA key authentication
default_authentication_plugin='mysql_native_password'
#loose-* prefix to avoid unknown variable
loose-group_replication_group_name="d27ad9d5-b700-4e92-9341-b40fb231478c"
## off start_on_boot 
loose-group_replication_start_on_boot=off
loose-group_replication_local_address= "{}"
loose-group_replication_group_seeds= "{}"
loose-group_replication_bootstrap_group=off
'''.format(MYSQL_PORT, datadir,
           MYSQL_PORT, datadir,
           MYSQL_PORT, datadir,
           datadir, datadir, datadir, datadir,
           MYSQL_X_PORT,  # mysqlx_port
           MYSQL_MAX_CONNECTIONS, server_id, local_address, group_seeds)
    return my_cnf


def mysql_user_privileges(user, password, host):
    sql = '''\
CREATE USER '{}'@'{}' IDENTIFIED BY '{}';
GRANT ALL ON *.* TO '{}'@'{}';
FLUSH PRIVILEGES;
'''.format(user, host, password,
           user, host)
    return sql


def mgr_privileges():
    sql = '''
SET SQL_LOG_BIN=0;
CREATE USER rpl_user@'%' IDENTIFIED BY 'password';
GRANT REPLICATION SLAVE ON *.* TO rpl_user@'%';
GRANT BACKUP_ADMIN ON *.* TO rpl_user@'%';
FLUSH PRIVILEGES;
SET SQL_LOG_BIN=1;
CHANGE REPLICATION SOURCE TO SOURCE_USER='rpl_user', SOURCE_PASSWORD='password' FOR CHANNEL 'group_replication_recovery';
'''
    return sql


def mgr_bootstrapping(bootstrap_group, single_leader=False):
    sql = ''
    if single_leader:
        sql += 'SET GLOBAL group_replication_paxos_single_leader=ON;\n'
    if bootstrap_group:
        sql += 'SET GLOBAL group_replication_bootstrap_group=ON;\n'
        sql += "START GROUP_REPLICATION USER='rpl_user', PASSWORD='password';\n"
        sql += 'SET GLOBAL group_replication_bootstrap_group=OFF;\n'
    else:
        sql += "START GROUP_REPLICATION USER='rpl_user', PASSWORD='password';\n"
    sql += 'SELECT * FROM performance_schema.replication_group_members;\n'
    return sql


def tpcc_environment_variable():
    env = '''\
export MAXITEMS={}
export CUST_PER_DIST={}
export DIST_PER_WARE={}
export ORD_PER_DIST={}
export MAX_NUM_ITEMS={}
export MAX_ITEM_LEN=14
'''.format(NUM_ITEM, CUST_PER_DIST, DIST_PER_WARE, ORD_PER_DIST, MAX_NUM_ITEMS)
    return env


remote_mgr_sql = '/home/mysql/mysql_data/mgr_privilege.sql'
remote_grant_sql = '/home/mysql/mysql_data/user_privilege.sql'
remote_bootstrap_sql = '/home/mysql/mysql_data/bootstrap.sql'
remote_bootstrap_sql_single_leader_on = '/home/mysql/mysql_data/bootstrap_single_leader_on.sql'
# remote_bin = '/home/mysql/block-db/bin'
mysql_root_user = 'root'
mysql_root_password = 'root'
mysql_db_name = 'tpcc'
mysql_homedir = '/home/mysql/'
mysql_datadir = '/home/mysql/mysql_data'


def mysql_run_bench_tpcc(conf_file, terminal, warehouse):
    path_node_configure_file = os.path.join(CONF_PATH, conf_file)
    conf_map = load_json_file(path_node_configure_file)
    node_server = conf_map['node_server_list'].copy()
    print("-------- MYSQL TPCC run benchmark --------")
    master_server_address = node_server[0]['address']
    cmd_run_tpcc = '''\
{}
tpcc_mysql_bench -h{} -P{} -d{} -u{} -p{} -w{} -c{} -r10 -l{}
'''.format(tpcc_environment_variable(),
           master_server_address,  # -h
           MYSQL_PORT,  # -P
           mysql_db_name,  # -d
           mysql_root_user,  # -u
           mysql_root_password,  # -p
           warehouse,  # -w
           terminal,
           MYSQL_TPCC_RUN_SECONDS
           )
    print(cmd_run_tpcc)
    rc, stdout, _ = run_shell_command(shell_command=cmd_run_tpcc)
    if rc == 0:
        match = re.search(r"([0-9]+\.?[0-9]*)\s*TpmC", stdout)
        if match:
            tpm = float(match.group(1))
            output_result = {
                'label': 'mysql',
                'db_type': 'mysql',
                'terminals': terminal,
                'binding': True,
                'warehouses': warehouse,
                'num_item': NUM_ITEM,
                'percent_distributed': 0,
                'cached_tuple_percentage':0.0,
                'tpm': tpm,
                'abort': 0.0,
            }
            with open('output.txt', 'a') as file:
                file.write(json.dumps(output_result) + '\n')
            file.close()


def mysql_run_sql_local(username, password, host, sql):
    cmd = 'mysql -u root --skip-password -e "{}" \n'.format(sql)
    ec, stdout, stderr = run_command_remote(username=username, password=password, host=host, shell_command=cmd)
    return ec, stdout, stderr


def mysql_dump_data(conf_file, warehouse):
    path_node_configure_file = os.path.join(CONF_PATH, conf_file)
    conf_map = load_json_file(path_node_configure_file)
    user, password = get_user_password()
    node_server = conf_map['node_server_list'].copy()
    node_client = conf_map['node_client'].copy()
    address = node_client['address']
    mysql_clean(user, password, address)
    hosts, group_seeds = mysql_gen_all_host_seeds(node_server, node_client)
    mysql_init_data_dir(user, password, 0, address, hosts, group_seeds)
    mysqld_startup(user, password, address)
    while True:
        cmd = 'mysql -u root --skip-password -f < {} \n'.format(remote_grant_sql)
        print(cmd)
        ec, _, _ = run_command_remote(username=user, password=password, host=address, shell_command=cmd)
        if ec == 0:
            break
        time.sleep(1)

    mysql_create_schema(user, password, address)
    mysql_load_run(address, warehouse)

    cmd = 'mysqldump -u root -p {} --init-command="SET SQL_LOG_BIN = 0;"  > {}/mysql/{}.sql'.format(mysql_db_name,
                                                                                                    BLOCK_PROJECT_PATH,
                                                                                                    mysql_db_name)
    run_shell_command(cmd)


def mysql_clean(user, password, address):
    print("-------- MYSQL TPCC kill mysqld server {} --------".format(address))
    clean_cmd = '''\
    killall -9 mysqld'''
    run_command_remote(user, password, address, clean_cmd)

    print("-------- MYSQL TPCC rm mysql DB directory {} --------".format(address))
    clean_cmd = '''\
    rm -rf /home/mysql/mysql_data
    rm -rf /home/mysql/.my.cnf
    mkdir -p /home/mysql/mysql_data
    ls -s
    '''
    run_command_remote(user, password, address, clean_cmd)


def mysql_init_data_dir(user, password, server_id, address, hosts, group_seeds, copy_data=False):
    print("-------- MYSQL TPCC initialize data dir {} --------".format(address))
    cmd = 'mkdir -p /tmp/mysql_data\n'
    run_shell_command(cmd)

    local_mgr_address = '0.0.0.0:{}'.format(MYSQL_MGR_PORT)
    mgr_address_list = str(group_seeds)
    # mgr_address_list = mgr_address_list.replace(address, '0.0.0.0')
    my_cnf_str = mysql_cnf(mysql_datadir, server_id, local_mgr_address, mgr_address_list)

    my_cnf_tmp_path = os.path.join('/tmp/mysql_data', '.my.cnf')
    with open(my_cnf_tmp_path, 'w') as file:
        file.write(my_cnf_str)

    bootstrap_group = server_id == 1
    sql = ''
    sql += mgr_privileges()
    sql_tmp_path = os.path.join('/tmp/mysql_data/mgr_privilege.sql')
    with open(sql_tmp_path, 'w') as file:
        file.write(sql)

    with open('/tmp/mysql_data/bootstrap_single_leader_on.sql', 'w') as file:
        sql = mgr_bootstrapping(bootstrap_group, True)
        file.write(sql)
    with open('/tmp/mysql_data/bootstrap.sql', 'w') as file:
        sql = mgr_bootstrapping(bootstrap_group, False)
        file.write(sql)
    sql = ''
    for h in hosts:
        sql += mysql_user_privileges(mysql_root_user, mysql_root_password, h)
    with open('/tmp/mysql_data/user_privilege.sql', 'w') as file:
        file.write(sql)

    if copy_data:
        # cp = 'cp {}/mysql/{}.sql /tmp/mysql_data/'.format(BLOCK_PROJECT_PATH, mysql_db_name)
        cp = 'cp {}/mysql/{}.sql {}'.format(BLOCK_PROJECT_PATH, mysql_db_name, mysql_datadir)
        run_command_remote(user, password, address, cp)

    cp = 'cp {}/mysql/tpcc_schema.sql /tmp/mysql_data/'.format(BLOCK_PROJECT_PATH)
    run_shell_command(cp)

    rsync_to_remote(source_path=my_cnf_tmp_path, username=user, host=address, target_path='/home/mysql/.my.cnf')
    rsync_to_remote(source_path='/tmp/mysql_data', username=user, host=address, target_path=mysql_homedir)

    cmd = 'rm -rf /tmp/mysql_data\n'
    run_shell_command(cmd)


def mysqld_startup(user, password, address):
    # mysqld startup
    cmd = '''\
mysqld_safe --initialize-insecure --user=mysql
cat /home/mysql/mysql_data/error.log
'''.format(mysql_datadir)
    run_command_remote(username=user, password=password, host=address, shell_command=cmd)

    print("-------- MYSQL TPCC start mysqld {} --------".format(address))
    cmd = 'nohup mysqld_safe > mysqld.out 2>&1 </dev/null & \n'
    run_command_remote(username=user, password=password, host=address, shell_command=cmd)


def mysql_create_schema(user, password, address):
    print("-------- MYSQL TPCC create table --------")
    cmd_create_table = 'mysql -u root --skip-password {} < {}/tpcc_schema.sql \n'.format(
        '',
        mysql_datadir)

    run_command_remote(user, password, address, shell_command=cmd_create_table)


def mysql_load_run(address, warehouse):
    print("-------- MYSQL TPCC load data --------")
    cmd_load_script = '''\
{}
{}/mysql/load.sh {} {} {} {} {}
    '''.format(tpcc_environment_variable(), BLOCK_PROJECT_PATH, mysql_db_name, warehouse,
               mysql_root_user, mysql_root_password, address)
    print(cmd_load_script)
    run_shell_command(shell_command=cmd_load_script)


def mysql_gen_all_host_seeds(node_server, node_client):
    hosts = []
    group_seeds = ""
    for n in node_server:
        if group_seeds != '':
            group_seeds += ','
        group_seeds += '{}:{}'.format(n['address'], MYSQL_MGR_PORT)
        hosts.append(n['address'])
    hosts.append(node_client['address'])
    return hosts, group_seeds


def mysql_init_bench_tpcc(conf_file, warehouse):
    path_node_configure_file = os.path.join(CONF_PATH, conf_file)
    conf_map = load_json_file(path_node_configure_file)
    node_server = conf_map['node_server_list'].copy()
    node_client = conf_map['node_client'].copy()
    user, password = get_user_password()
    # user = 'mysql'
    # password = 'mysql'

    master_server_address = ''
    hosts, group_seeds = mysql_gen_all_host_seeds(node_server, node_client)

    server_id = 0
    for n in node_server:
        server_id = server_id + 1
        address = n['address']

        mysql_clean(user, password, address)
        bootstrap_group = server_id == 1
        if bootstrap_group:
            master_server_address = address

        mysql_init_data_dir(user, password, server_id, address, hosts, group_seeds, True)

        mysqld_startup(user, password, address)

    time.sleep(3)

    for n in node_server:
        address = n['address']

        mysql_create_schema(user, password, address)

        cmd = 'mysql -u root --skip-password {} < {}/{}.sql'.format(mysql_db_name, mysql_datadir, mysql_db_name)
        ec, _, _ = run_command_remote(username=user, password=password, host=address, shell_command=cmd)
        if ec != 0:
            time.sleep(1)
            exit(0)

    print("-------- MYSQL TPCC load data finish --------")

    for n in node_server:
        address = n['address']
        print("-------- MYSQL TPCC run initialize script on address {}--------".format(address))
        cmd = 'mysql -u root --skip-password -f < {} \n'.format(remote_mgr_sql)
        while True:
            ec, std, err = run_command_remote(username=user, password=password, host=address, shell_command=cmd)
            if ec == 0:
                break
            else:
                time.sleep(3)
                print("init mgr privilege error", ec)
                print(std)
                print(err)
    sql_select_group_member = 'SELECT * FROM performance_schema.replication_group_members;'

    mysql_run_sql_local(username=user, password=password, host=master_server_address, sql=sql_select_group_member)

    for n in node_server:
        address = n['address']
        print("-------- MYSQL TPCC mgr bootstrap on address {}--------".format(address))
        while True:
            cmd = 'mysql -u root --skip-password -f < {} \n'.format(remote_bootstrap_sql)
            print(cmd)
            ec, std, err = run_command_remote(username=user, password=password, host=address, shell_command=cmd)
            if ec == 0:
                break
            else:
                time.sleep(3)
                print("bootstrap error", ec)
                print(std)
                print(err)

    mysql_run_sql_local(username=user, password=password, host=master_server_address, sql=sql_select_group_member)

    print("-------- MYSQL TPCC create user privileges --------")
    while True:
        cmd = 'mysql -u root --skip-password -f < {} \n'.format(remote_grant_sql)
        print(cmd)
        ec, std, err = run_command_remote(username=user, password=password, host=master_server_address,
                                          shell_command=cmd)
        if ec == 0:
            break
        else:
            time.sleep(3)
            print("init user mgr privilege error", ec)
            print(std)
            print(err)

    mysql_run_sql_local(username=user, password=password, host=master_server_address, sql=sql_select_group_member)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='benchmark')
    parser.add_argument('-l', '--configure-latency-wan', type=int, help='configure wan latency(ms)')
    parser.add_argument('-ll', '--configure-latency-lan', type=int, help='configure wan latency(ms)')
    parser.add_argument('-mg', '--mysql-data-gen', action='store_true', help='test data generate')
    parser.add_argument('-ml', '--mysql-load', action='store_true', help='test mysql load')
    parser.add_argument('-mr', '--mysql-run', action='store_true', help='test mysql run')
    parser.add_argument('-t', '--test-type', type=str, help='test type:bind/unbind/dist/contention')
    parser.add_argument('-r', '--run-command', type=str, help='run command on all site')
    parser.add_argument('-c', '--clean', action='store_true', help='clean all')

    args = parser.parse_args()

    latency_ms = getattr(args, 'configure_latency_wan')
    latency_ms_lan = getattr(args, 'configure_latency_lan')
    test_type = getattr(args, 'test_type')
    command = getattr(args, 'run_command')
    binding = False
    dist = False
    if args.mysql_data_gen:
        mysql_dump_data(NODE_MYSQL_CONF, NUM_WAREHOUSE)
        exit(0)
    if args.mysql_load:
        mysql_load_data(NODE_MYSQL_CONF, NUM_WAREHOUSE)
        exit(0)
    if args.mysql_run:
        if latency_ms is not None:
            config_latency(NODE_MYSQL_CONF, latency_ms)
        mysql_run_evaluation(NODE_MYSQL_CONF, NUM_WAREHOUSE)
        exit(0)
    if test_type is not None:
        if test_type == 'bind':
            binding = True
            dist = False
        elif test_type == 'unbind':
            binding = False
            dist = False
        elif test_type == 'dist':
            binding = True
            dist = True
        elif test_type == 'contention':
            binding = True
            dist = True
    conf = config_file_name(dist, binding)
    if args.clean:
        clean_all(conf, clean_run_dir=True)
        exit(0)
    if command is not None:
        execute_command(conf, command)
        exit(0)

    if latency_ms is not None:
        config_latency(conf, latency_ms)
        exit(0)
    if latency_ms_lan is not None:
        config_latency(conf, latency_ms_lan, wan=False)
        exit(0)

    if test_type is not None:
        if test_type == 'bind':
            evaluation_block_binding(conf, True)
        elif test_type == 'unbind':
            evaluation_block_binding(conf, False)
        elif test_type == 'dist':
            evaluation_distributed(conf)
        elif test_type == 'contention':
            evaluation_contention(conf)
        elif test_type == 'warehouse':
            evaluation_warehouse(conf)
