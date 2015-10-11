import boto.ec2
import sys
import json
from subprocess import call

def print_usage(args):
    print("Usage: python %s credentials.csv ssh_key.pem" % args[0])

def deploy_config(access_id, secret_key, ssh_key_file):
    print("Start creation of config file.")
    conn = boto.ec2.connect_to_region("us-west-2",
                                      aws_access_key_id=access_id,
                                      aws_secret_access_key=secret_key)
    reservations = conn.get_all_reservations()
    config = {}
    count = 0
    instances = []
    for reservation in reservations:
        for instance in reservation.instances:
            if instance.state == 'running':
                instances.append(instance)
                config[count] = {'ip': instance.ip_address, 'port':5000}
                count += 1
    with open("config.json", "w") as config_fd:
        config_fd.write(json.dumps(config))
    print("Configuration file created.")
    print("Start deploying config file.")
    for instance in instances:
        print("Copy config on %s" % instance.ip_address)
        cmd = "scp -i %s config.json ubuntu@%s:/home/ubuntu/cs550-advanced-os/PA2/dht/config.json" % (ssh_key_file, instance.ip_address)
        call(cmd.split())
    print("Configuration deployed on DHT nodes.")
    
if __name__ == '__main__':
    args = sys.argv
    if len(args) != 3:
        print_usage(args)
        sys.exit(1)
    with open(args[1]) as credentials_fd:
        credentials_fd.readline()
        username, access_id, secret_key = credentials_fd.readline().strip().split(",")
    ssh_key_file = args[2]
    deploy_config(access_id, secret_key, ssh_key_file)
    
