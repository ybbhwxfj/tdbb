#!/usr/bin/env python3
import argparse
import json
import multiprocessing
import os
import pathlib
import re
import shutil
import stat
import subprocess
import time

import paramiko

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

NUM_READ_ONLY_ROWS = 500
ADDITIONAL_READ_ONLY = False

APPEND_LOG_ENTRIES_BATCH_MIN = 1
APPEND_LOG_ENTRIES_BATCH_MAX = 32

THREADS_ASYNC_CONTEXT = 4
THREADS_CC = 4
THREADS_REPLICATION = 1
THREADS_IO = 10
CONNECTIONS_PER_PEER = 10
MYSQL_TPCC_RUN_SECONDS = 100
MYSQL_MAX_CONNECTIONS = 2500

SSH_SERVER_PORT = 22
RETRY_AFTER_EXCEPTION = 500
WAN_LATENCY_DELAY_WAIT_MS = 0
CACHED_TUPLE_PERCENTAGE = 0.2
DIST_PERCENTAGE = False
RAFT_FOLLOWER_TICK_MAX_REQUEST_VOTE = 40
RAFT_LEADER_ELECTION_TICK_MS = 200

AZ_RTT_LATENCY_MS = 40
DEADLOCK_DETECTION = False
DEADLOCK_DETECTION_MS = 1000
LOCK_TIMEOUT_MS = 500
CALVIN_EPOCH_MS = 20
NUM_OUTPUT_RESULT = 80

NODE_MYSQL_CONF = 'node.mysql.conf.json'
NODE_SHARE_TIGHT_CONF = "node.conf.sdb.tb.json"
NODE_SHARE_LOOSE_CONF = "node.conf.sdb.lb.json"
NODE_SHARE_NOTHING_CONF = "node.conf.sndb.json"
NODE_SHARE_CCB_DSB_CONF = "node.conf.scrdb.json"
USER_CONF = "user.conf.json"
USER_ROOT_CONF = "user.root.conf.json"


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

DEFAULT_TEST_CACHED_PERCENTAGE = 0.2

MYSQL_TERMINALS = [50, 100, 150, 200]
DEFAULT_NUM_TERMINAL = 160
NUM_TERMINAL_ARRAY = [160]

DEFAULT_PERCENT_REMOTE_WH = 0.01
PERCENT_REMOTE_WH_ARRAY = [0.01, 0.25, 0.5]
CCB_CACHED_PERCENTAGE_ARRAY = [0.0, 0.25, 0.5, 0.75, 1.0]
PERCENT_READ_ONLY = [0.0, 0.25, 0.5, 0.75, 1.0]

NUM_WAREHOUSE = 160
WAREHOUSES = [160]

TEST_CACHE = 'cache'
TEST_DISTRIBUTE = 'distribute'
TEST_TERMINAL = 'terminal'
TEST_READ_ONLY = 'readonly'

DB_CONFIG_SLB = 'lb'
DB_CONFIG_STB = 'tb'
DB_CONFIG_SN = 'sn'
DB_CONFIG_SCR = 'scr'
DB_CONFIG_MYSQL = 'mysql'

TEST_NODE_CONF_BINDING = {NODE_SHARE_LOOSE_CONF: 'l', NODE_SHARE_TIGHT_CONF: 't'}
TEST_NODE_CONF_DISTRIBUTED = {NODE_SHARE_NOTHING_CONF: 'dist'}
DB_S = 'db-s'
DB_TYPE_DISTRIBUTED = ['db-sn', 'db-d']
DB_TYPE_SN_LOCKING = ['db-sn']
DB_TYPE_SCR = ['db-scr']
NUM_TRANSACTIONS = {'db-sn': 800000, 'db-d': 800000, 'db-s': 400000, 'db-scr': 800000}

MYSQL_PORT = 3306
MYSQL_X_PORT = 33060  # mysqldump needed
MYSQL_MGR_PORT = 33061


def config_file_name(db_config_type="sn"):
    if db_config_type == DB_CONFIG_SCR:
        return NODE_SHARE_CCB_DSB_CONF
    elif db_config_type == DB_CONFIG_SN:
        return NODE_SHARE_NOTHING_CONF
    elif db_config_type == DB_CONFIG_STB:
        return NODE_SHARE_TIGHT_CONF
    elif db_config_type == DB_CONFIG_SLB:
        return NODE_SHARE_LOOSE_CONF
    elif db_config_type == DB_CONFIG_MYSQL:
        return NODE_MYSQL_CONF


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


def get_user_password(user_name=None, user_conf=USER_CONF):
    user_conf = os.path.join(CONF_PATH, user_conf)
    user_json = load_json_file(user_conf)
    user = user_json['user']
    password = user_json['password']
    if user_name is not None and user != user_name:
        if user_name == 'root':
            user_conf = os.path.join(CONF_PATH, USER_ROOT_CONF)
            user_json = load_json_file(user_conf)
            user = user_json['user']
            password = user_json['password']
        else:
            print("unknown user name {}".format(user_name))
            exit(-1)
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


def user_home(user):
    if user == 'root':
        home = '/root'
    else:
        home = '/home/{}'.format(user)
    return home


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
              percent_remote=DEFAULT_PERCENT_REMOTE_WH,
              percent_hot_item=0.0,
              percent_read_only=0.0,
              num_warehouse=NUM_WAREHOUSE,
              num_item=NUM_ITEM,
              dist_per_ware=DIST_PER_WARE,
              db_type='db-s', label='xxx',
              conf_file=NODE_SHARE_TIGHT_CONF,
              tight_binding=True,
              percent_cached_tuple=CACHED_TUPLE_PERCENTAGE,
              control_percent_dist_tx=DIST_PERCENTAGE,
              ):
    path_node_configure_file = os.path.join(CONF_PATH, conf_file)
    conf_map = load_json_file(path_node_configure_file)
    name2path = {}
    node_server = conf_map['node_server_list'].copy()
    node_client = conf_map['node_client_list'].copy()

    node_list = node_server + node_client

    az_priority = gen_priority(node_server)
    for node in node_server:
        az = node['zone_name']
        if node['zone_name'] in az_priority:
            node['priority'] = az_priority[az]
    num_transactions = NUM_TRANSACTIONS[db_type]
    tpcc_config = {
        'num_warehouse': num_warehouse,
        'num_item': num_item,
        'num_order_initialize_per_district': ORD_PER_DIST,
        'num_district_per_warehouse': dist_per_ware,
        'num_max_order_line': MAX_NUM_ITEMS,
        'num_terminal': num_terminal,
        'num_customer_per_district': CUST_PER_DIST,
        'num_new_order': num_transactions,
        'percent_non_exist_item': 0,
        'percent_hot_item': percent_hot_item,
        'percent_read_only': percent_read_only,
        'num_read_only_rows': NUM_READ_ONLY_ROWS,
        'additional_read_only_terminal': ADDITIONAL_READ_ONLY,
        'hot_item_num': HOT_ITEM_NUM,
        # remove warehouse or shard
        'percent_remote': percent_remote,
        # when this true, percent_remote is the remote shard percentage,
        # otherwise, it is warehouse percentage
        "control_percent_dist_tx": control_percent_dist_tx,
        "raft_follow_tick_num": RAFT_FOLLOWER_TICK_MAX_REQUEST_VOTE,
        "raft_leader_election_tick_ms": RAFT_LEADER_ELECTION_TICK_MS,
        "calvin_epoch_ms": CALVIN_EPOCH_MS,
        "num_output_result": NUM_OUTPUT_RESULT,
        "az_rtt_ms": AZ_RTT_LATENCY_MS,
        "flow_control_rtt_count": 4,
    }

    test_conf = {
        'wan_latency_delay_wait_ms': WAN_LATENCY_DELAY_WAIT_MS,
        'percent_cached_tuple': percent_cached_tuple,
        'deadlock_detection_ms': DEADLOCK_DETECTION_MS,
        'deadlock_detection': DEADLOCK_DETECTION,
        'lock_timeout_ms': LOCK_TIMEOUT_MS,
        'label': label,
        'parameter': ''
    }

    processors = []
    receivers = []
    for node in node_list:
        is_client = len(node['block_type']) == 1 and node['block_type'][0] == 'CLIENT'
        receiver, sender = multiprocessing.Pipe(duplex=False)
        p = multiprocessing.Process(
            target=process_configure_node,
            args=(
                sender,
                node_server,
                node_client,
                tpcc_config,
                test_conf,
                node['address'],
                node['node_name'],
                node['node_name'],
                db_type,
                not is_client,
                label,
                az_priority,
            )
        )
        receivers.append(receiver)
        processors.append(p)
    for p in processors:
        p.start()
    for p in processors:
        p.join()
    for r in receivers:
        (name, path) = r.recv()
        print("receive from PIPE, name:{}, path:{}".format(name, path))
        name2path[name] = path

    output_result = {
        'label': label,
        'db_type': db_type,
        'terminals': num_terminal,
        'binding': tight_binding,
        'warehouses': num_warehouse,
        'num_item': num_item,
        'percent_remote': percent_remote,
        'percent_hot_item': percent_hot_item,
        'percent_cached_tuple': percent_cached_tuple,
        'percent_read_only': percent_read_only,
        'control_dist_tx':control_percent_dist_tx,
    }

    # process server
    servers = node_server.copy()
    clients = node_client.copy()
    processors = []
    for node in servers:
        node_name = node['node_name']
        node_path = name2path[node_name]
        address = node['address']
        p = multiprocessing.Process(
            target=process_run_block,
            args=(address, node_path, True, None)
        )
        processors.append(p)
    for p in processors:
        p.start()
    for p in processors:
        p.join()

    # process client ...
    processors = []
    for i in range(len(clients)):
        node = clients[i]
        node_name = node['node_name']
        node_path = name2path[node_name]
        address = node['address']
        result_json = None
        # if the last one, run at local node and output the json result
        if i + 1 == len(clients):
            result_json = output_result
        p = multiprocessing.Process(
            target=process_run_client,
            args=(address, node_path, False, result_json, node_name)
        )
        processors.append(p)
    for p in processors:
        p.start()
    for p in processors:
        p.join()


def process_run_client(address, run_dir, backend, output_result, name):
    process_run_block(address, run_dir, backend, output_result)
    output = 'output_{}.txt'.format(name)
    if output_result is not None:
        json_file = os.path.join(run_dir, output)
        with open(json_file, 'w') as file:
            file.write(json.dumps(output_result) + '\n')
        file.close()


def process_run_block(address, run_dir, backend, output_result):
    run_block(address, run_dir, backend, output_result)


def run_block(address, run_dir, backend, output_result=None):
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
    if output_result is None:
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


def process_configure_node(
        pipe,
        server_node_conf_list,
        client_node_conf,
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
    (name, path) = configure_node(
        server_node_conf_list,
        client_node_conf,
        tpcc_config,
        test_conf,
        address,
        node_name,
        binding_node_name,
        db_type,
        is_backend,
        label,
        az_priority
    )
    pipe.send((name, path))
    pipe.close()
    print("send to PIPE, name:{}, path:{}".format(name, path))


def configure_node(
        server_node_conf_list,
        client_node_conf,
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
        'threads_cc': THREADS_CC,
        'threads_replication': THREADS_REPLICATION,
        'connections_per_peer': CONNECTIONS_PER_PEER,
        'append_log_entries_batch_min': APPEND_LOG_ENTRIES_BATCH_MIN,
        'append_log_entries_batch_max': APPEND_LOG_ENTRIES_BATCH_MAX,
    }

    configure = {
        'block': block_config,
        'param': tpcc_config,
        'test': test_conf,
        'node_server_list': server_node_conf_list,
        'node_client_list': client_node_conf,
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
    node_client = conf_map['node_client_list'].copy()
    user, password = get_user_password()
    node_list = node_server + node_client
    for node in node_list:
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
    percent_remote = 0.1
    for db_type in DB_TYPE_DISTRIBUTED:
        for percent_hot in [0.0, 0.1, 0.2]:
            term = DEFAULT_NUM_TERMINAL
            clean_all(conf_path)
            label = 'hot_' + str(percent_hot)
            run_bench(num_terminal=term,
                      num_warehouse=NUM_WAREHOUSE,
                      num_item=NUM_ITEM,
                      percent_remote=percent_remote,
                      percent_hot_item=percent_hot,
                      db_type=db_type,
                      label=label,
                      conf_file=conf_path,
                      tight_binding=True)


def evaluation_warehouse(conf_path):
    percent_remote = 0.1
    for db_type in DB_TYPE_DISTRIBUTED:
        for warehouse in WAREHOUSES:
            term = DEFAULT_NUM_TERMINAL
            clean_all(conf_path)
            label = 'wh_' + str(warehouse)
            run_bench(num_terminal=term,
                      num_warehouse=warehouse,
                      num_item=NUM_ITEM,
                      percent_remote=percent_remote,
                      db_type=db_type,
                      label=label,
                      conf_file=conf_path,
                      tight_binding=True)


def evaluation_ratio_read_only_tx(
        conf_path,
        db_types,
        num_item=NUM_ITEM,
        array_num_warehouse=None,
        array_num_terminal=None,
        control_percent_dist_tx=DIST_PERCENTAGE,
):
    if array_num_warehouse is None:
        array_num_warehouse = WAREHOUSES
    if array_num_terminal is None:
        array_num_terminal = [DEFAULT_NUM_TERMINAL]
    for db_type in db_types:
        for read_only in PERCENT_READ_ONLY:
            for num_warehouse in array_num_warehouse:
                for term in array_num_terminal:
                    clean_all(conf_path)
                    label = 'dist'
                    run_bench(num_terminal=term,
                              num_warehouse=num_warehouse,
                              num_item=num_item,
                              percent_read_only=read_only,
                              db_type=db_type,
                              label=label,
                              conf_file=conf_path,
                              tight_binding=True,
                              control_percent_dist_tx=control_percent_dist_tx,
                              )


def evaluation_distributed(
        conf_path,
        db_types=DB_TYPE_DISTRIBUTED,
        num_item=NUM_ITEM,
        array_num_warehouse=None,
        array_percent_remote_wh=None,
        array_num_terminal=None,
        control_percent_dist_tx=DIST_PERCENTAGE,
):
    if array_num_warehouse is None:
        array_num_warehouse = WAREHOUSES
    if array_num_terminal is None:
        array_num_terminal = [DEFAULT_NUM_TERMINAL]
    if array_percent_remote_wh is None:
        array_percent_remote_wh = [DEFAULT_PERCENT_REMOTE_WH]
    for db_type in db_types:
        for percent_remote in array_percent_remote_wh:
            for num_warehouse in array_num_warehouse:
                for term in array_num_terminal:
                    clean_all(conf_path)
                    label = 'dist'
                    run_bench(num_terminal=term,
                              num_warehouse=num_warehouse,
                              num_item=num_item,
                              percent_remote=percent_remote,
                              db_type=db_type,
                              label=label,
                              conf_file=conf_path,
                              tight_binding=True,
                              control_percent_dist_tx=control_percent_dist_tx,
                              )


def evaluation_block_binding(
        conf_path,
        tight_binding=True,
        num_terminal_array=None,
        cached_percentage_array=None,
        percent_read_only=None,
):
    if cached_percentage_array is None:
        cached_percentage_array = [DEFAULT_TEST_CACHED_PERCENTAGE]
    if num_terminal_array is None:
        num_terminal_array = [DEFAULT_NUM_TERMINAL]
    if percent_read_only is None:
        percent_read_only = [0.0]
    if tight_binding:
        label = 'tight_bind'
    else:
        label = 'loose_bind'
    db_type = DB_S
    num_warehouse = NUM_WAREHOUSE
    percent_remote = DEFAULT_PERCENT_REMOTE_WH
    for term in num_terminal_array:
        for percentage in cached_percentage_array:
            for read_only in percent_read_only:
                clean_all(conf_path)
                run_bench(num_terminal=term,
                          num_warehouse=num_warehouse,
                          percent_read_only=read_only,
                          percent_remote=percent_remote,
                          db_type=db_type,
                          label=label,
                          conf_file=conf_path,
                          tight_binding=tight_binding,
                          percent_cached_tuple=percentage)


def tc_set_command(ip, delay=None, rate=None):
    if delay is None:
        delay_s = ''
    else:
        delay_s = ' --delay {}ms '.format(delay)
    if rate is None:
        rate_s = ''
    else:
        rate_s = ' --rate {}Mbps '.format(rate)
    cmd = '''
tcset eth0 {} {} --network {} 
'''.format(delay_s, rate_s, ip)
    return cmd


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


def tc_command_list(address_list, delay, rate):
    cmd = ""
    for address in address_list:
        cmd = cmd + tc_set_command(address, delay, rate)
    return cmd


def get_nodes(conf_file, all_node=True):
    path_node_configure_file = os.path.join(CONF_PATH, conf_file)
    conf_map = load_json_file(path_node_configure_file)
    node_server = conf_map['node_server_list'].copy()
    node = node_server
    if all_node:
        node_client = conf_map['node_client_list'].copy()
        node = node + node_client
    return node


def config_latency(conf_file, wan_latency, lan_latency, bandwidth_wan, use_private_address=False):
    node = get_nodes(conf_file, all_node=True)
    user, password = get_user_password(user_name='root')
    if use_private_address:
        address_s = "private_address"
    else:
        address_s = "address"
    for n in node:
        wan_access_address = []
        lan_access_address = []
        for n1 in node:
            if n['node_name'] != n1['node_name']:
                if n['zone_name'] != n1['zone_name']:
                    print("not in one AZ:", n['zone_name'], n1['zone_name'])
                    wan_access_address.append(n1[address_s])
                else:
                    print("in one AZ:", n['zone_name'], n1['zone_name'])
                    lan_access_address.append(n1[address_s])
        cmd1 = tc_command_list(wan_access_address, wan_latency, bandwidth_wan)
        cmd2 = tc_command_list(lan_access_address, lan_latency, None)
        cmd = cmd1 + cmd2
        run_command_remote(username=user, password=password, host=n[address_s], shell_command=cmd)


def execute_command(conf_file, shell_command):
    node = get_nodes(conf_file)
    user, password = get_user_password()
    for n in node:
        print('=====run command=====:, address ' + n['address'])
        run_command_remote(username=user, password=password, host=n['address'], shell_command=shell_command)


def mysql_load_data(conf_file, load=False):
    mysql_init_bench_tpcc(conf_file, load)


def mysql_run_evaluation(conf_file, warehouse):
    for t in MYSQL_TERMINALS:
        mysql_run_bench_tpcc(conf_file, terminal=t, warehouse=warehouse)


# local_address s1:33061
# group_seeds s1:33061,s2:33061,s3:33061
def mysql_cnf(datadir, server_id, mysql_x_bind_address, local_address, group_seeds):
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
mysqlx_bind_address = "{}"
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
           mysql_x_bind_address,
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


# remote_bin = '/home/mysql/block-db/bin'
mysql_root_user = 'root'
mysql_root_password = 'root'
mysql_db_name = 'tpcc'


def mysql_datadir(user):
    d = '{}/mysql_data'.format(user_home(user))
    return d


def remote_grant_sql(user):
    d = '{}/user_privilege.sql'.format(mysql_datadir(user))
    return d


def remote_bootstrap_sql(user):
    d = '{}/bootstrap_single_leader_on.sql'.format(mysql_datadir(user))
    return d


def mysql_run_bench_tpcc(conf_file, terminal, warehouse):
    path_node_configure_file = os.path.join(CONF_PATH, conf_file)
    conf_map = load_json_file(path_node_configure_file)
    node_server = conf_map['node_server_list'].copy()
    print("-------- MYSQL TPCC run benchmark --------")
    # use the last
    master_server_address = node_server[len(node_server) - 1]['private_address']
    cmd_run_tpcc = '''\
export LD_LIBRARY_PATH=/usr/local/mysql/lib/mysql/:/usr/local/lib/
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
                'percent_remote': 0,
                'percent_cached_tuple': 0.0,
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
    node_client = conf_map['node_client_list'].copy()[0]
    address = node_client['private_address']
    mysql_clean(user, password, address)
    hosts, group_seeds = mysql_gen_all_host_seeds(node_server, node_client)
    mysql_init_data_dir(user, password, 0, address, hosts, group_seeds)
    mysqld_startup(user, password, address)
    while True:
        cmd = 'mysql -u root --skip-password -f < {} \n'.format(remote_grant_sql(user))
        print(cmd)
        ec, _, _ = run_command_remote(username=user, password=password, host=address, shell_command=cmd)
        if ec == 0:
            break
        time.sleep(1)

    mysql_create_schema(user, password, address)
    mysql_load_run(user, address, warehouse)

    cmd = 'mysqldump -u root -p {}  > {}/mysql/{}.sql'.format(mysql_db_name,
                                                              BLOCK_PROJECT_PATH,
                                                              mysql_db_name)
    run_shell_command(cmd)


def mysql_clean(user, password, address):
    print("-------- MYSQL TPCC kill mysqld server {} --------".format(address))
    clean_cmd = '''\
    killall -9 mysqld'''
    run_command_remote(user, password, address, clean_cmd)

    print("-------- MYSQL TPCC rm mysql DB directory {} --------".format(address))
    home = user_home(user)
    clean_cmd = '''\
    rm -rf {}/mysql_data
    rm -rf {}/.my.cnf
    mkdir -p {}/mysql_data
    ls -s
    '''.format(home, home, home)
    run_command_remote(user, password, address, clean_cmd)


def write_bootstrap_sql(bootstrap_group, path):
    with open('{}/bootstrap_single_leader_on.sql'.format(path), 'w') as file:
        sql = mgr_bootstrapping(bootstrap_group, True)
        file.write(sql)
    with open('{}/bootstrap.sql'.format(path), 'w') as file:
        sql = mgr_bootstrapping(bootstrap_group, False)
        file.write(sql)


def mysql_init_data_dir_without_loading(user, address, bootstrap_group=False):
    print("-------- MYSQL TPCC initialize data dir {} without loading --------".format(address))
    cmd = 'rm -rf /tmp/mysql_data\n'
    cmd = cmd + 'mkdir -p /tmp/mysql_data\n'
    run_shell_command(cmd)
    write_bootstrap_sql(bootstrap_group, '/tmp/mysql_data/')

    mysql_homedir = user_home(user)
    rsync_to_remote(source_path='/tmp/mysql_data', username=user, host=address, target_path=mysql_homedir)

    cmd = 'rm -rf /tmp/mysql_data\n'
    run_shell_command(cmd)


def mysql_init_data_dir(user, password, server_id, address, hosts, group_seeds, bootstrap_group=False, copy_data=False):
    print("-------- MYSQL TPCC initialize data dir {} --------".format(address))
    cmd = 'mkdir -p /tmp/mysql_data\n'
    run_shell_command(cmd)

    local_mgr_address = '{}:{}'.format(address, MYSQL_MGR_PORT)
    mgr_address_list = str(group_seeds)

    my_cnf_str = mysql_cnf(mysql_datadir(user), server_id, address, local_mgr_address, mgr_address_list)

    my_cnf_tmp_path = os.path.join('/tmp/mysql_data', '.my.cnf')
    with open(my_cnf_tmp_path, 'w') as file:
        file.write(my_cnf_str)

    sql = ''
    sql += mgr_privileges()
    sql_tmp_path = os.path.join('/tmp/mysql_data/mgr_privilege.sql')
    with open(sql_tmp_path, 'w') as file:
        file.write(sql)

    write_bootstrap_sql(bootstrap_group, '/tmp/mysql_data/')

    sql = ''
    for h in hosts:
        sql += mysql_user_privileges(mysql_root_user, mysql_root_password, h)
    with open('/tmp/mysql_data/user_privilege.sql', 'w') as file:
        file.write(sql)

    if copy_data:
        cp = 'cp {}/mysql/{}.sql /tmp/mysql_data/'.format(BLOCK_PROJECT_PATH, mysql_db_name)
        # cp = 'cp {}/mysql/{}.sql {}'.format(BLOCK_PROJECT_PATH, mysql_db_name, mysql_datadir(user))
        run_shell_command(cp)

    cp = 'cp {}/mysql/tpcc_schema.sql /tmp/mysql_data/'.format(BLOCK_PROJECT_PATH)
    run_shell_command(cp)
    mysql_homedir = user_home(user)
    rsync_to_remote(source_path=my_cnf_tmp_path, username=user, host=address, target_path='{}/.my.cnf'.format(mysql_homedir))
    rsync_to_remote(source_path='/tmp/mysql_data', username=user, host=address, target_path=mysql_homedir)

    cmd = 'rm -rf /tmp/mysql_data\n'
    run_shell_command(cmd)


def mysqld_startup(user, password, address):
    # mysqld startup
    cmd = '''\
mysqld_safe --initialize-insecure --user={}
cat {}/mysql_data/error.log
'''.format(user, user_home(user), mysql_datadir(user))
    run_command_remote(username=user, password=password, host=address, shell_command=cmd)

    print("-------- MYSQL TPCC start mysqld {} --------".format(address))
    cmd = 'nohup mysqld_safe > mysqld.out 2>&1 </dev/null & \n'
    run_command_remote(username=user, password=password, host=address, shell_command=cmd)


def mysql_create_schema(user, password, address):
    print("-------- MYSQL TPCC create table --------")
    cmd_create_table = 'mysql -u root --skip-password {} < {}/tpcc_schema.sql \n'.format(
        '',
        mysql_datadir(user))

    run_command_remote(user, password, address, shell_command=cmd_create_table)


def mysql_load_run(user, address, warehouse):
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
        group_seeds += '{}:{}'.format(n['private_address'], MYSQL_MGR_PORT)
        hosts.append(n['private_address'])
    hosts.append(node_client['private_address'])
    return hosts, group_seeds


def mysql_init_bench_tpcc(conf_file, load=False):
    path_node_configure_file = os.path.join(CONF_PATH, conf_file)
    conf_map = load_json_file(path_node_configure_file)
    node_server = conf_map['node_server_list'].copy()
    node_client = conf_map['node_client_list'].copy()[0]
    user, password = get_user_password()
    # user = 'mysql'
    # password = 'mysql'

    master_server_address = ''
    hosts, group_seeds = mysql_gen_all_host_seeds(node_server, node_client)

    server_id = 0
    for i in reversed(range(len(node_server))):
        n = node_server[i]
        server_id = server_id + 1
        address = n['private_address']

        bootstrap_group = i + 1 == len(node_server)
        if bootstrap_group:
            master_server_address = address
        if load:
            mysql_clean(user, password, address)
            mysql_init_data_dir(user, password, server_id, address, hosts, group_seeds, bootstrap_group, True)
        else:
            mysql_init_data_dir_without_loading(user, address, bootstrap_group)
        mysqld_startup(user, password, address)

    time.sleep(3)

    for n in node_server:
        address = n['private_address']
        if load:
            mysql_create_schema(user, password, address)

            cmd = 'mysql -u root --skip-password {} < {}/{}.sql'.format(mysql_db_name, mysql_datadir(user), mysql_db_name)
            ec, _, _ = run_command_remote(username=user, password=password, host=address, shell_command=cmd)
            if ec != 0:
                time.sleep(1)
                exit(0)

    print("-------- MYSQL TPCC load data finish --------")

    for n in node_server:
        address = n['private_address']
        print("-------- MYSQL TPCC run initialize script on address {}--------".format(address))
        remote_mgr_sql = '{}/mgr_privilege.sql'.format(mysql_datadir(user))
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

    for n in reversed(node_server):
        address = n['private_address']
        print("-------- MYSQL TPCC mgr bootstrap on address {}--------".format(address))
        while True:
            cmd = 'mysql -u root --skip-password -f < {} \n'.format(remote_bootstrap_sql(user))
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
        cmd = 'mysql -u root --skip-password -f < {} \n'.format(remote_grant_sql(user))
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


def handle_debug(conf, url):
    path_node_configure_file = os.path.join(CONF_PATH, conf)
    conf_map = load_json_file(path_node_configure_file)
    node_server = conf_map['node_server_list'].copy()
    node_client = conf_map['node_client_list'].copy()

    for node in [node_client]:
        address = node['private_address']
        port = node['port'] + 1000
        wget = 'wget -q --output-document - {}:{}/{}'.format(address, port, url)
        run_shell_command(wget)


def main():
    parser = argparse.ArgumentParser(description='benchmark')
    parser.add_argument('-lw', '--configure-latency-wan', type=int, help='configure wan latency(ms)')
    parser.add_argument('-ll', '--configure-latency-lan', type=int, help='configure lan latency(ms)')
    parser.add_argument('-bw', '--configure-bandwidth-wan', type=int, help='configure wan bandwidth(Mbps)')
    parser.add_argument('-mg', '--mysql-data-gen', action='store_true', help='test data generate')
    parser.add_argument('-ml', '--mysql-load', action='store_true', help='test mysql load')
    parser.add_argument('-mi', '--mysql-init', action='store_true', help='test mysql load')
    parser.add_argument('-mr', '--mysql-run', action='store_true', help='test mysql run')
    parser.add_argument('-t', '--db-config-type', type=str, help='db config type:lb/tb/sn/scr')
    parser.add_argument('-r', '--run-command', type=str, help='run command on all site')
    parser.add_argument('-c', '--clean', action='store_true', help='clean all')
    parser.add_argument('-tp', '--test-parameter', type=str, help='test parameter:cache/readonly/terminal/distribute')
    parser.add_argument('-dt', '--distributed-tx', action='store_true', help='control remote distributed transaction')
    parser.add_argument('-dg', '--debug-url', type=str, help='debug url')

    args = parser.parse_args()

    if args.mysql_data_gen:
        mysql_dump_data(NODE_MYSQL_CONF, NUM_WAREHOUSE)
        return
    elif args.mysql_load:
        mysql_load_data(NODE_MYSQL_CONF, load=True)
        return
    elif args.mysql_init:
        mysql_load_data(NODE_MYSQL_CONF, load=False)
        return
    elif args.mysql_run:
        mysql_run_evaluation(NODE_MYSQL_CONF, NUM_WAREHOUSE)
        return

    control_dist_tx = args.distributed_tx
    latency_ms_wan = getattr(args, 'configure_latency_wan')
    latency_ms_lan = getattr(args, 'configure_latency_lan')
    bandwidth_wan = getattr(args, 'configure_bandwidth_wan')
    db_config_type = getattr(args, 'db_config_type')
    test_parameter = getattr(args, 'test_parameter')
    command = getattr(args, 'run_command')
    debug_url = getattr(args, 'debug_url')

    if latency_ms_lan is None:
        latency_ms_lan = 0
    if latency_ms_wan is None:
        latency_ms_wan = 0
    if test_parameter is None:
        test_parameter = TEST_TERMINAL

    conf = config_file_name(db_config_type)
    if args.clean:
        clean_all(conf, clean_run_dir=True)
        return
    elif command is not None:
        execute_command(conf, command)
        return
    elif latency_ms_wan > 0:
        config_latency(conf, latency_ms_wan, latency_ms_lan, bandwidth_wan, True)
        return

    if debug_url is not None:
        if db_config_type is None:
            return
        handle_debug(conf, debug_url)
        return

    if db_config_type == DB_CONFIG_STB:
        if test_parameter == TEST_CACHE:
            evaluation_block_binding(
                conf,
                tight_binding=True,
                cached_percentage_array=CCB_CACHED_PERCENTAGE_ARRAY)
        elif test_parameter == TEST_TERMINAL:
            evaluation_block_binding(
                conf,
                tight_binding=True,
                num_terminal_array=NUM_TERMINAL_ARRAY)
        elif test_parameter == TEST_READ_ONLY:
            evaluation_block_binding(
                conf,
                tight_binding=True,
                percent_read_only=PERCENT_READ_ONLY
            )
        elif test_parameter == TEST_DISTRIBUTE:
            evaluation_distributed(
                conf,
                db_types=[DB_S],
                array_percent_remote_wh=PERCENT_REMOTE_WH_ARRAY,
                control_percent_dist_tx=control_dist_tx)
    elif db_config_type == DB_CONFIG_SLB:
        if test_parameter == TEST_CACHE:
            evaluation_block_binding(
                conf,
                tight_binding=False,
                cached_percentage_array=CCB_CACHED_PERCENTAGE_ARRAY)
        elif test_parameter == TEST_TERMINAL:
            evaluation_block_binding(
                conf,
                tight_binding=False,
                num_terminal_array=NUM_TERMINAL_ARRAY)
    elif db_config_type == DB_CONFIG_SN:
        if test_parameter == TEST_DISTRIBUTE:
            evaluation_distributed(
                conf,
                db_types=DB_TYPE_DISTRIBUTED,
                array_percent_remote_wh=PERCENT_REMOTE_WH_ARRAY,
                control_percent_dist_tx=control_dist_tx)
        elif test_parameter == TEST_TERMINAL:
            evaluation_distributed(conf,
                                   array_num_terminal=NUM_TERMINAL_ARRAY)
        elif test_parameter == TEST_READ_ONLY:
            evaluation_ratio_read_only_tx(
                conf,
                db_types=DB_TYPE_SN_LOCKING,
                control_percent_dist_tx=control_dist_tx)
    elif db_config_type == DB_CONFIG_SCR:
        if test_parameter == TEST_DISTRIBUTE:
            evaluation_distributed(
                conf,
                db_types=DB_TYPE_SCR,
                array_percent_remote_wh=PERCENT_REMOTE_WH_ARRAY,
                control_percent_dist_tx=control_dist_tx)
        elif test_parameter == TEST_READ_ONLY:
            evaluation_ratio_read_only_tx(
                conf,
                db_types=DB_TYPE_SCR,
                control_percent_dist_tx=control_dist_tx)


if __name__ == '__main__':
    main()
