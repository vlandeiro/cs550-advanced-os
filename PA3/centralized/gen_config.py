#!/usr/bin/python
import json
import sys

from urllib2 import urlopen

doc = """
Usage:
   ./gen_config.py server [listening_port=P] [max_connections=S]
   ./gen_config.py peer server_ip server_port [listening_port=P] [max_connections=S] [download_dir=D]
"""

def print_err(msg):
    sys.stderr.write("Error: %s\n" % msg)
    sys.stderr.write("%s\n" % doc.strip())
    sys.exit(1)

if __name__ == '__main__':
    args = sys.argv
    if len(args) < 2:
        print_err("Error: missing arguments.")
    conf_type = args[1]
    if conf_type not in ['peer', 'server']:
        print_err("Error: second argument should be 'peer' or 'server'.")
    
    types_dict = {
        "listening_port": int,
        "max_connections": int,
        "idxserv_port": int,
        "download_dir": str,
        "log": str,
        "replica": int
    }

    template_files = {
        "peer": "templates/config_peer_template.json",
        "server": "templates/config_server_template.json"
    }
    with open(template_files[conf_type], 'r') as template_fd:
        template_conf = json.load(template_fd)

    if conf_type == 'peer':
        if len(args) < 4:
            print_err("Error: missing arguments.")
        template_conf['idxserv_ip'] = args[2]
        template_conf['idxserv_port'] = int(args[3])
        opt_args = args[4:]
    else: # conf_type == 'server'
        opt_args = args[3:]

    for arg in opt_args:
        arg_name, arg_val = arg.split('=')
        if arg_name in types_dict:
            template_conf[arg_name] = types_dict[arg_name](arg_val)
        else:
            print_err("Wrong argument: %s" % arg)
            
    ip = json.load(urlopen('https://api.ipify.org/?format=json'))['ip']
    template_conf['listening_ip'] = str(ip)

    print json.dumps(template_conf, indent=4, separators=(',', ': '))
    
