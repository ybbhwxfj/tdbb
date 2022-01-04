#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import stat
import time
import paramiko
import subprocess

NODE_SHARE_TIGHT_CONF = "node.s.t.conf.json"
NODE_SHARE_LOOSE_CONF = "node.s.l.conf.json"
NODE_SN_CONF = "node.sn.conf.json"
USER_CONF = "user.conf.json"
BLOCK_CONF = "block.json"
TPCC_SCHEMA = 'tpcc_schema.json'
BINARY_BLOCK_DB = 'block-db'
BINARY_BLOCK_CLIENT = 'block-client'
TMPDIR = "/tmp/block"
STARTUP_SCRIPT = "block.sh"
SSH_SERVER_PORT = 22
RETRY_AFTER_EXCEPTION = 500
BLOCK_PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PY_PATH = os.path.join(BLOCK_PROJECT_PATH, 'py')
BINARY_PATH = os.path.join(BLOCK_PROJECT_PATH, 'bin')
CONF_PATH = os.path.join(BLOCK_PROJECT_PATH, 'conf')
RUN_BLOCK = "run_block"

TERMINALS = [500, 1000, 1500, 2000, 2500]
CONTENTED_TERMINALS = [200, 400, 800, 1000, 1200]
NUM_WAREHOUSE = 80
NUM_ITEM = 10000
NODE_CONF_BINDING = {NODE_SHARE_LOOSE_CONF: 'l', NODE_SHARE_TIGHT_CONF: 't'}
NODE_CONF_DISTRIBUTED = {NODE_SN_CONF: 'dist'}
DB_S = 'db-s'
DB_TYPE_DISTRIBUTED = ['db-sn', 'db-gro', 'db-d']
NUM_OUTPUT_RESULT = 50


def config_file_name(distributed=True, tight_binding=True):
    d_type = 'share'
    b_type = 'unbind'
    if distributed:
        d_type = 'dist'
    if tight_binding:
        b_type = 'bind'
    return 'node.{}.{}.conf.json'.format(d_type, b_type)


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
    p = subprocess.run(shell_command, shell=True,
                       capture_output=True, text=True)
    print(p.stdout)
    print(p.stderr)


def run_command_remote(username, password, host, shell_command):
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

    for line in stdout:
        print(line.strip('\n'))

    client.close()


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


def run_bench(num_terminal=10,
              percent_distributed=0.0,
              num_warehouse=NUM_WAREHOUSE,
              num_item=NUM_ITEM,
              num_district=10,
              db_type='db-s', label='xxx',
              conf_file=NODE_SHARE_TIGHT_CONF,
              tight_binding=True):
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
        'num_order_item': 10,
        'num_order_initialize': 3000,
        'num_district': num_district,
        'num_max_order_line': 10,
        'num_terminal': num_terminal,
        'num_customer': 1000,
        'num_new_order': 1000,
        'percent_non_exist_item': 0,
        'percent_distributed': percent_distributed,
        "raft_follow_tick_num": 20,
        "raft_leader_tick_ms": 50,
        "calvin_epoch_ms": 50,
        "num_output_result": NUM_OUTPUT_RESULT,
        "az_rtt_ms": 100,
        "flow_control_rtt_count": 4,
    }

    test_conf = {
        'wan_latency_ms': 0,
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
                  ' nohup {}/{} > be.out 2>&1 </dev/null & \n '.format(run_dir,
                                                                       run_dir, STARTUP_SCRIPT)
    else:
        run_cmd = 'cd {}\n' \
                  ' {}/{}\n '.format(run_dir,
                                     run_dir, STARTUP_SCRIPT)
    if backend:
        user, password = get_user_password()
        # print(run_cmd)
        run_command_remote(username=user, password=password, host=address, shell_command=run_cmd)
    else:
        run_shell_command(shell_command=run_cmd)
        result_file = os.path.join(run_dir, 'result.json')
        result = load_json_file(result_file)

        output_result['abort'] = result['abort']
        output_result['tpm'] = result['tpm']
        output_result['latency'] = result['latency']
        output_result['latency_read'] = result['latency_read']
        output_result['latency_append'] = result['latency_append']
        output_result['latency_replicate'] = result['latency_replicate']
        output_result['latency_lock_wait'] = result['latency_lock_wait']
        output_result['latency_part'] = result['latency_part']


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


def clean_all(conf_file):
    path_node_configure_file = os.path.join(CONF_PATH, conf_file)
    conf_map = load_json_file(path_node_configure_file)
    node_server = conf_map['node_server_list'].copy()
    user, password = get_user_password()

    for node in node_server:
        clean_cmd = 'pkill -f {}\n'.format(BINARY_BLOCK_DB)
        clean_cmd = clean_cmd + 'pkill -f {}\n'.format(BINARY_BLOCK_CLIENT)
        clean_cmd = clean_cmd + 'rm -rf /tmp/block \n'
        run_command_remote(user, password, node['address'], clean_cmd)


def evaluation_distributed(conf_path):
    percent_distributed = 0.1
    for wh_item in [
        (100, 10000, '-uc', TERMINALS),
        (20, 10000, '-c', CONTENTED_TERMINALS)
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
    for term in TERMINALS:
        clean_all(conf_path)
        run_bench(num_terminal=term,
                  num_warehouse=num_warehouse,
                  percent_distributed=percent_distributed,
                  db_type=db_type,
                  label=label,
                  conf_file=conf_path,
                  tight_binding=tight_binding)


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


def get_nodes(conf_file):
    path_node_configure_file = os.path.join(CONF_PATH, conf_file)
    conf_map = load_json_file(path_node_configure_file)
    node_server = conf_map['node_server_list'].copy()
    node_client = conf_map['node_client'].copy()
    node = node_server
    node.append(node_client)
    return node


def config_latency(conf_file, latency):
    node = get_nodes(conf_file)
    user, password = get_user_password()
    for n in node:
        wan_access_address = []
        for n1 in node:
            if n['zone_name'] != n1['zone_name']:
                wan_access_address.append(n1['address'])
        cmd = tc_command_list(wan_access_address, latency)
        run_command_remote(username=user, password=password, host=n['address'], shell_command=cmd)


def execute_command(conf_file, shell_command):
    node = get_nodes(conf_file)
    user, password = get_user_password()
    for n in node:
        print('=====run command=====:, address ' + n['address'])
        run_command_remote(username=user, password=password, host=n['address'], shell_command=shell_command)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='benchmark')
    parser.add_argument('-l', '--configure-latency', type=int, help='configure wan latency(ms)')
    parser.add_argument('-t', '--test-type', type=str, help='test type:bind/unbind/dist')
    parser.add_argument('-r', '--run-command', type=str, help='run command on all site')
    args = parser.parse_args()

    latency_ms = getattr(args, 'configure_latency')
    test_type = getattr(args, 'test_type')
    command = getattr(args, 'run_command')
    binding = False
    dist = False
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

    conf = config_file_name(dist, binding)
    if command is not None:
        execute_command(conf, command)
        exit(0)

    if test_type is not None:
        if test_type == 'bind':
            if latency_ms is not None:
                config_latency(conf, latency_ms)
            evaluation_block_binding(conf, True)
        elif test_type == 'unbind':
            if latency_ms is not None:
                config_latency(conf, latency_ms)
            evaluation_block_binding(conf, False)
        elif test_type == 'dist':
            if latency_ms is not None:
                config_latency(conf, latency_ms)
            evaluation_distributed(conf)
