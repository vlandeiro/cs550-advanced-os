#!/usr/bin/python
import json
from subprocess import call

if __name__ == '__main__':
    config_path = 'dfs/config.json'
    with open(config_path) as config_fd:
        config = json.load(config_fd)
    cd_cmd = 'cd dfs;'
    if 'idx_type' not in config: # centralized indexing server
        cmd = 'exec python central_indexing_server.py config.json'
    else: # peer
        cmd = 'exec python node.py config.json'
    call(cd_cmd + cmd, shell=True)

