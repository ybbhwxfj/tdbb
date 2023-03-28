import argparse
import json
import os
import re

DSB = "DSB"
RLB = "RLB"
CCB = "CCB"
PNB = "PNB"
CLIENT = "CLIENT"
PORT = 700
REPLICATION_PORT = 800

# https://gist.github.com/dfee/6ed3a4b05cfe7a6faf40a2102408d5d8
IP_ADDRESS = r'.*?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s*\((Internet|Public|Private|VPC)\).*?'
IPAddressPattern = re.compile(IP_ADDRESS)
Internet = ['Internet', 'Public']
Private = ['Private', 'VPC']


def config():
    config_map = {
    }
    return config_map


def retrieve_ip_from_file(file_path):
    ips = []
    internet_ip = ""
    private_ip = ""
    with open(file_path, 'r', encoding='utf-8') as fd:
        lines = fd.readlines()
        for line in lines:
            ip, net = regex_match(line)
            if ip is not None and net is not None:
                if net in Private:
                    private_ip = ip
                elif net in Internet:
                    internet_ip = ip
                if private_ip != '' and internet_ip != '':
                    ips.append((internet_ip, private_ip))
                    internet_ip = ""
                    private_ip = ""
    return ips


def regex_match(string):
    match = IPAddressPattern.match(string)
    if match is not None:
        ip = match.group(1)  # IP address
        net = match.group(2)  # Internet / Private
        return ip, net
    return None, None


def block_type_to_name(block_type_list):
    name = ''
    for i in range(len(block_type_list)):
        if i != 0:
            name = name + "-" + block_type_list[i]
        else:
            name = block_type_list[i]
    return name


def shard_name_string(names):
    name = ""
    for i in range(len(names)):
        if i == 0:
            name = names[i]
        else:
            name = name + "_" + names[i]
    return name

#
#     if tight:
#         array = [[CCB, RLB, DSB]]
#     else:
#         array = [[CCB], [RLB], [DSB]]
def gen_configure_json(
        files,
        json_name,
        port,
        repl_port,
        block_type=None,
        block_num_node=None
):
    if block_num_node is None:
        block_num_node = [1, 1, 1]
    if block_type is None:
        block_type = [[CCB], [RLB], [DSB]]
    if len(block_num_node) != len(block_type) or len(block_type) > 3 or len(block_type) == 0:
        print("configure error")
        exit(-1)

    az2ips = {}
    num_shard = 0

    for num in block_num_node:
        if num_shard < num:
            num_shard = num

    block_node_name = []
    for i in range(len(block_type)):
        s_name_array = []
        num_node = block_num_node[i]
        n = 0
        while n < num_shard:
            s_name = []  # in one node
            if num_shard % num_node != 0:
                raise 0
            num_shard_per_node = int(num_shard / num_node)
            for j in range(num_shard_per_node):
                s_name.append("s_{}".format(n + 1))
                n += 1
            s_name_array.append(s_name)
            print(s_name)
        block_node_name.append(s_name_array)

    for f in files:
        ips = retrieve_ip_from_file(f)
        print(ips)
        file_base_name = os.path.basename(f)
        az_name = os.path.splitext(file_base_name)[0]
        az2ips[az_name] = ips

    server_nodes = []
    for az_name in az2ips.keys():
        ips = az2ips[az_name]
        for i in range(len(block_type)):
            bt = block_type[i]

            for j in range(len(block_node_name[i])):
                shard_names = block_node_name[i][j]
                ip = ips.pop(0)
                b_name = block_type_to_name(bt)
                node = {
                    "zone_name": az_name,
                    "shard_name": shard_names,
                    "node_name": "node_s{}_r{}_b{}".format(shard_name_string(shard_names), az_name, b_name),
                    # public address
                    "address": ip[0],
                    # private address
                    "private_address": ip[1],
                    "port": port,
                    "repl_port": repl_port,
                    "block_type": bt
                }
                server_nodes.append(node)
        az2ips[az_name] = ips

    configure = config()
    configure["node_server_list"] = server_nodes

    # generate client configure
    client_nodes = []
    # in az2ips, only client ip address left
    client_id = 0
    for az_name in az2ips.keys():
        ips = az2ips[az_name]
        for ip in ips:
            client_id = client_id + 1
            cli_node = {
                "zone_name": az_name,
                "shard_name": ["client_{}".format(client_id)],
                "node_name": "client_{}".format(client_id),
                "address": ip[0],
                "private_address": ip[1],
                "port": port,
                "repl_port": repl_port,
                "block_type": [CLIENT]
            }
            client_nodes.append(cli_node)

    configure["node_client_list"] = client_nodes

    with open(json_name, 'w') as file:
        file.write(json.dumps(configure, indent=4))
    file.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='configure')
    parser.add_argument('-p', '--path', type=str, help="az configure file path")
    parser.add_argument('-s', '--shards', type=int)
    parser.add_argument('-tb', '--tight-bind', action='store_true', help='tight bind')
    parser.add_argument('-scr', '--share-ccb-rlb', action='store_true', help='share CCB and RLB, and scale DSB block')
    file_name = ["az1.txt", "az2.txt", "az3.txt"]
    # gen_configure_json(file_name, "node.conf.sdb.b.json", PORT, True)
    # gen_configure_json(file_name, "node.conf.sdb.ub.json", PORT, False)

    args = parser.parse_args()
    path = getattr(args,  'path')
    shards = getattr(args, 'shards')
    tight_bind = getattr(args, 'tight_bind')
    scale_dsb = getattr(args, 'share_ccb_rlb')

    if path is not None:
        f = []
        for file in file_name:
            f.append('{}/{}'.format(path, file))
        file_name = f
        
    if shards is None:
        shards = 1

    if scale_dsb:
        gen_configure_json(file_name, "node.conf.scrdb.json", PORT, REPLICATION_PORT,
                           [[CCB, RLB ], [DSB]], [1, shards])
        exit(0)

    if shards > 1:
        gen_configure_json(file_name, "node.conf.sndb.json", PORT, REPLICATION_PORT,
                           [[CCB, RLB, DSB]], [shards])
    else:
        if tight_bind:
            gen_configure_json(file_name, "node.conf.sdb.tb.json", PORT, REPLICATION_PORT,
                               [[CCB, RLB, DSB]], [1]
                               )
        else:
            gen_configure_json(file_name, "node.conf.sdb.lb.json", PORT, REPLICATION_PORT,
                               [[CCB], [RLB], [DSB]], [1, 1, 1])
