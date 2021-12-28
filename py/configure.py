import argparse
import json
import os
import re

DSB = "DSB"
RLB = "RLB"
CCB = "CCB"
PNB = "PNB"
CLIENT = "CLIENT"
PORT = 8000

# https://gist.github.com/dfee/6ed3a4b05cfe7a6faf40a2102408d5d8
IP_ADDRESS = r'.*?(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s*\((Internet|Private)\).*?'
IPAddressPattern = re.compile(IP_ADDRESS)
Internet = 'Internet'
Private = 'Private'


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
                if net == Private:
                    private_ip = ip
                elif net == Internet:
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


def gen_configure_json(files, json_name, port, tight=True):
    az2ips = {}
    be_az2ips = {}
    configure = config()
    if tight:
        array = [[CCB, RLB, DSB]]
    else:
        array = [[CCB], [RLB], [DSB]]

    max_n = 0
    min_n = 1000
    for f in files:
        ips = retrieve_ip_from_file(f)
        file_base_name = os.path.basename(f)
        az_name = os.path.splitext(file_base_name)[0]
        az2ips[az_name] = ips
        if len(ips) > max_n:
            max_n = len(ips)
            cli_az_name = az_name
        if len(ips) < min_n:
            min_n = len(ips)

    panel_az2ips = {}
    client_az2ips = {}
    for az in az2ips.keys():
        ips = az2ips[az]
        if len(az2ips[az]) - min_n == 2:
            panel_az2ips[az] = ips[min_n]
            client_az2ips[az] = ips[min_n + 1]
        elif len(az2ips[az]) - min_n == 1:
            if len(client_az2ips) == 0:
                client_az2ips[az] = ips[min_n]
            elif len(panel_az2ips) == 0:
                panel_az2ips[az] = ips[min_n]
        be_az2ips[az] = ips[0::min_n]

    for block_types in array:
        nodes = []
        for az_name in be_az2ips.keys():
            ips = be_az2ips[az_name]
            sid = 1
            for ip in ips:
                node = {
                    "zone_name": az_name,
                    "shard_name": "s{}".format(sid),
                    "node_name": "node_s{}_r{}".format(sid, az_name),
                    "address": ip[0],
                    "private_address": ip[1],
                    "port": port,
                    "block_type": block_types
                }
                sid = sid + 1
                nodes.append(node)
        configure["node_server_list"] = nodes
        for az in panel_az2ips.keys():
            ip = panel_az2ips[az]
            cli_node = {
                "zone_name": az,
                "shard_name": "",
                "node_name": "client",
                "address": ip[0],
                "private_address": ip[1],
                "port": port,
                "block_type": [CLIENT]
            }
            configure['node_client'] = cli_node
        for az in client_az2ips.keys():
            ip = client_az2ips[az]
            panel_node = {
                "zone_name": az,
                "shard_name": "",
                "node_name": "panel",
                "address": ip[0],
                "private_address": ip[1],
                "port": port,
                "block_type": [PNB]
            }
            configure['node_panel'] = panel_node

    with open(json_name, 'w') as file:
        file.write(json.dumps(configure, indent=4))
    file.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='benchmark')
    file_name = ["aazheyuan.txt", "azhangzhou.txt", "azhohhot.txt"]
    gen_configure_json(file_name, "node.conf.ecs.json", PORT)
