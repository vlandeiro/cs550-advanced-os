import json
import sys
import logging
import os
import time

from urllib2 import urlopen
from hashlib import md5
from dht_client import DHTClient
from dht_server import DHTServer
from multiprocessing import Manager, Value, Lock

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
        self.map_lock = self.manager.Lock()

        self.stdin = os.fdopen(os.dup(sys.stdin.fileno()))
        self.terminate = Value('i', 0)
        
        self.server = DHTServer(self)
        self.client = DHTClient(self)

    def put(self, key, value):
        """Fill the hashmap with a key and value."""
        self.map_lock.acquire()
        self.hashmap[key] = value
        self.map_lock.release()
        return True

    def get(self, key):
        """Get value of a given key."""
        self.map_lock.acquire()
        val = self.hashmap.get(key)
        self.map_lock.release()
        return val

    def rem(self, key):
        """Delete entry in the hashmap."""
        self.map_lock.acquire()
        del self.hashmap[key]
        self.map_lock.release()
        return True

    def server_hash(self, key):
        """Return the peer id to contact given a key."""
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
        peers_map = {int(id): peers_map[id] for id in peers_map}
    dht_node = DHT(peers_map)
    try:
        dht_node.server.start()
        time.sleep(1)
        dht_node.client.run()
        dht_node.server.join()
    except KeyboardInterrupt as e:
        sys.stderr.write("KeyboardInterrupt")
