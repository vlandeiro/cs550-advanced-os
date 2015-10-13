import boto.ec2
import sys
import json
from subprocess import call

def print_usage(args):
    print("Usage: python %s id config.json ssh_key.pem" % args[0])

def connect(ip, ssh_key_file):
    cmd = "ssh -i %s ubuntu@%s" % (ssh_key_file, ip)
    call(cmd.split())
    
if __name__ == '__main__':
    args = sys.argv
    if len(args) != 4:
        print_usage(args)
        sys.exit(1)
    id_inst = args[1]
    config_path = args[2]
    ssh_key_path = args[3]
    
    with open(config_path) as config_fd:
        config = json.load(config_fd)
    connect(config[id_inst]["ip"], ssh_key_path)
    
