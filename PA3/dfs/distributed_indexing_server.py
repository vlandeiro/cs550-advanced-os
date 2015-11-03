import json
import sys
import logging
import os
import time

from urllib2 import urlopen
from hashlib import md5
from dht_server import DHTServer
from multiprocessing import Manager, Value, Lock, Process

logging.basicConfig(level=logging.DEBUG)

class DHT():
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)

        self.nodes_list = config['nodes']
        self.nodes_count = len(self.nodes_list)

        # get this server info from config file
        self.nodes_list = config['nodes']
        self.replica = config['replica']
        # get this server info from config file
        self.this_ip = str(json.load(urlopen('https://api.ipify.org/?format=json'))['ip'])
        if self.this_ip not in self.nodes_list:
            raise ValueError("peer %s is not included in the config file." % self.this_ip)
        self.id = self.nodes_list.index(self.this_ip)

        self.ip = self.this_ip
        self.port = config['idx_server_port']

        self.manager = Manager()
        self.hashmap = self.manager.dict()
        self.map_lock = self.manager.Lock()

        self.terminate = Value('i', 0)

        self.server = DHTServer(self)

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

    def keys(self):
        self.map_lock.acquire()
        keys_list = self.hashmap.keys()
        self.map_lock.release()
        return keys_list

    def server_hash(self, key):
        """Return the peer id to contact given a key."""
        return int(md5(key).hexdigest(), 16) % self.nodes_count
