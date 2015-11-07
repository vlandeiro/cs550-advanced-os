import boto.ec2
import sys
import json
from subprocess import call

log_level = 'INFO'
download_dir = '../data/download/'

default_distributed = {
    'idx_type': 'distributed',
    'log_level': log_level,
    'replica': 0,
    'max_connections': 20,
    'download_dir': download_dir,
    'timeout_value': 0.2,
    'file_server_port': 4000,
    'idx_server_port': 5000
}

default_centralized_peer = {
    'idx_type': 'centralized',
    'file_server_port': 4000,
    'idx_server_ip': None,
    'idx_server_port': 5000,
    'download_dir': download_dir,
    'log_level': log_level,
    'max_connections': 20,
    'timeout_value': 0.2
}

default_centralized_idx_server = {
    'log_level': log_level,
    'replica': 0,
    'max_connections': 20,
    'timeout_value': 0.2,
    'idx_server_port': 5000
}


def print_usage(args):
    print("Usage: python %s (centralized|distributed) credentials.csv ssh_key.pem" % args[0])


def get_running_instances(access_id, secret_key):
    conn = boto.ec2.connect_to_region("us-west-2",
                                      aws_access_key_id=access_id,
                                      aws_secret_access_key=secret_key)
    reservations = conn.get_all_reservations()
    instances = []
    for reservation in reservations:
        for instance in reservation.instances:
            if instance.state == 'running':
                instances.append(instance)
    return conn, instances


def deploy_centralized_config(access_id, secret_key, ssh_key_file):
    print("Start creation of config file.")
    config_peer = default_centralized_peer
    config_idx_server = default_centralized_idx_server

    # getting instances
    conn, instances = get_running_instances(access_id, secret_key)

    # define first instance as the indexing server and copy configuration
    nodes_config = {'nodes':[]}
    print("Configure central indexing server.")
    with open('config_server.json', 'w') as fd:
        fd.write(json.dumps(config_idx_server))
    idx_server = instances[0]
    nodes_config['nodes'].append(idx_server.ip_address)
    idx_server.add_tag("Name", "CIS")
    cmd = "scp -q -i %s config_server.json ubuntu@%s:/home/ubuntu/cs550-advanced-os/PA3/dfs/config.json" % (ssh_key_file, idx_server.ip_address)
    call(cmd.split())
    print("Indexing server is %s, configuration copied." % idx_server.ip_address)

    # copy the peer configuration to all the other nodes
    print("Configure the other nodes.")
    config_peer['idx_server_ip'] = idx_server.ip_address
    with open('config_peer.json', 'w') as fd:
        fd.write(json.dumps(config_peer))
    node_id = 0
    for inst in instances[1:]:
        nodes_config['nodes'].append(inst.ip_address)
        inst.add_tag("Name", "Node_%d" % node_id)
        node_id += 1
        print("Copy configuration file to %s." % inst.ip_address)
        cmd = "scp -q -i %s config_peer.json ubuntu@%s:/home/ubuntu/cs550-advanced-os/PA3/dfs/config.json" % (ssh_key_file, inst.ip_address)
        call(cmd.split())
    with open('config.json', 'w') as fd:
        fd.write(json.dumps(nodes_config))
    conn.close()

def deploy_distributed_config(access_id, secret_key, ssh_key_file):
    print("Start creation of config file.")
    config = default_distributed
    conn, instances = get_running_instances(access_id, secret_key)
    nodes_list = []
    node_id = 0
    for inst in instances:
        nodes_list.append(inst.ip_address)
        inst.add_tag("Name", "DFSNode_%d" % node_id)
        node_id += 1
    config['nodes'] = nodes_list
    with open("config.json", "w") as config_fd:
        config_fd.write(json.dumps(config))
    print("Configuration file created.")
    print("Start deploying config file.")
    for inst in instances:
        print("Copy configuration file to %s" % inst.ip_address)
        cmd = "scp -q -i %s config.json ubuntu@%s:/home/ubuntu/cs550-advanced-os/PA3/dfs/config.json" % (
        ssh_key_file, inst.ip_address)
        call(cmd.split())
    conn.close()

if __name__ == '__main__':
    args = sys.argv
    if len(args) != 4:
        print_usage(args)
        sys.exit(1)
    with open(args[2]) as credentials_fd:
        credentials_fd.readline()
        username, access_id, secret_key = credentials_fd.readline().strip().split(",")
    conf_type = args[1]
    ssh_key_file = args[3]
    if conf_type not in ['centralized', 'distributed']:
        raise AttributeError(
            'Configuration cannot be deployed with type = %s. Must be centralized or distributed.' % conf_type)
    elif conf_type == 'centralized':
        deploy_centralized_config(access_id, secret_key, ssh_key_file)
    else:
        deploy_distributed_config(access_id, secret_key, ssh_key_file)
