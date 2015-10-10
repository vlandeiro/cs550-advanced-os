import json
import sys
import logging

from urllib2 import urlopen
from hashlib import md5
from dht_client import DHTClient
from dht_server import DHTServer
from multiprocessing import Manager

logging.basicConfig(level=logging.DEBUG)

class DHT:
    def __init__(self, peers_map):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        
        self.peers_map = peers_map
        self.peers_count = len(peers_map)
        
        # get this server info from config file
        this_ip = str(json.load(urlopen('https://api.ipify.org/?format=json'))['ip'])
        this_server = filter(lambda s: s[1]['ip'] == this_ip, self.peers_map.iteritems())
        if not this_server:
            raise ValueError("peer %s is not included in the config file." % this_ip)
        self.id, config = this_server[0]
        self.ip = config['ip']
        self.port = config['port']

        self.manager = Manager()
        self.hashmap = self.manager.dict()
        
        self.server = DHTServer(self)
        self.client = DHTClient(self)

    def put(self, key, value):
        self.hashmap[key] = value
        return True

    def get(self, key):
        return self.hashmap.get(key)

    def del(self, key):
        del self.hashmap[key]
        return True

    def server_hash(self, key):
        return int(md5(key).hexdigest(), 16) % self.peers_count


def print_usage(args):
    sys.stderr.write("Usage: python %s config.json\n" % args[0])
    
if __name__ == '__main__':
    args = sys.argv
    if len(args) != 2:
        print_usage(args)
        sys.exit(1)
    with open(args[1], 'r') as config_fd:
        peers_map = json.load(config_fd)
        print peers_map
        peers_map = {int(id): peers_map[id] for id in peers_map}
        print peers_map
    dht_node = DHT(peers_map)
    dht_node.server.start()
    dht_node.client.start()
