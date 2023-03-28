import json

import yaml

NETWORK_NAME = 'bdb-network'

bt2arr = {
    'client': ['CLIENT'],
    'pnb': ['PNB'],
    'ccb': ["CCB"],
    'rlb': ["RLB"],
    'dsb': ["DSB"],
    'all': ['CCB', 'RLB', 'DSB']
}

unbinding_block_types = ['rlb', 'dsb', 'ccb']

bt2port = {
    'client': 8000,
    'pnb': 8008,
    'ccb': 8001,
    'rlb': 8002,
    'dsb': 8003,
    'all': 8000
}


def gen_service(az, shard, block_type='client'):
    if block_type == 'client':
        service_name = 'bdb-client-az{}'.format(az)
        ip_address = '192.168.{}.{}'.format(az, 100)
    elif block_type == 'pnb':
        service_name = 'bdb-pnb-az{}'.format(az)
        ip_address = '192.168.{}.{}'.format(az, 101)
    else:
        service_name = 'bdb-s{}-az{}-{}'.format(shard, az, block_type)
        ip_address = '192.168.{}.{}'.format(az, shard)
    node_config = {
        "zone_name": 'az{}'.format(az),
        "shard_name": 's{}'.format(shard),
        "node_name": service_name,
        "address": ip_address,
        "private_address": ip_address,
        "port": bt2port[block_type],
        "block_type": bt2arr[block_type]
    }

    service = {
        'build': '.',
        'tty': True,
        'cap_add': [
            'NET_ADMIN'
        ],
        'networks':
            {
                NETWORK_NAME: {
                    'ipv4_address': ip_address
                }
            },
        'image': 'block-db',
        'deploy': {
            'resources': {
                'limits': {
                    'cpus': '1',
                    'memory': '20G',
                },
                'reservations': {
                    'cpus': '0.25',
                    'memory': '1G'
                }
            }
        }
    }
    return service_name, service, node_config


def gen_network():
    network_name = NETWORK_NAME
    network = {
        'name': network_name,
        'driver': 'overlay',
        'attachable': True,
        'ipam': {
            'config':
                [
                    {'subnet': '192.168.0.0/16',
                     'gateway': '192.168.0.1'}
                ]

        }
    }

    return network_name, network


def generate_docker_compose(num_shard, num_az, binding=True):
    compose_yaml = {
        'version': '3.8',
        'networks': {},
        'services': {},
    }
    nodes_conf = {
        'node_server_list': [],
        'node_client': {}
    }
    if num_shard > 1:
        binding = True
    for i in range(1, num_shard + 1):
        for j in range(1, num_az + 1):
            if not binding:
                name, service, _ = gen_service(j, i, block_type='all')
                compose_yaml['services'][name] = service
                for bt in unbinding_block_types:
                    name, service, node_conf = gen_service(j, i, block_type=bt)
                    nodes_conf['node_server_list'].append(node_conf)
            else:
                name, service, node_conf = gen_service(j, i, block_type='all')
                compose_yaml['services'][name] = service
                nodes_conf['node_server_list'].append(node_conf)
    network_name, network = gen_network()
    compose_yaml['networks'][network_name] = network

    name, service, node_conf = gen_service(shard=0, az=num_az, block_type='client')
    compose_yaml['services'][name] = service
    nodes_conf['node_client'] = (node_conf)

    if num_shard > 1:
        db_type = 'dist'
    else:
        db_type = 'share'
    if binding:
        b_type = 'tb'
    else:
        b_type = 'lb'
    with open(r'docker-compose.{}.{}.yml'.format(db_type, b_type), 'w') as file:
        documents = yaml.dump(compose_yaml, file)
        file.close()
    with open(r'node.{}.{}.docker.json'.format(db_type, b_type), 'w') as file:
        file.write(json.dumps(nodes_conf, indent=4))
        file.close()


generate_docker_compose(5, 3)
generate_docker_compose(1, 3, True)
generate_docker_compose(1, 3, False)
