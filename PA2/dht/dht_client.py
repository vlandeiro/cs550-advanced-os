import json
import logging
import sys
import time

from multiprocessing import Process
from dht_protocol import *
from socket import *
from collections import Counter

logging.basicConfig(level=logging.DEBUG)

class DHTClient(Process):
    def __init__(self, dht):
        super(DHTClient, self).__init__()

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.dht = dht

        # load key/val data for benchmark
        self.logger("Loading data in memory for the benchmark...")
        self.data = {}
        with open("keyval.data") as data_fd:
            for line in data_fd:
                k, v = line.strip().split()
                self.data[k] = v
        self.logger("Benchmark data successfully loaded.")
        # keep map of sockets connected with peers
        # init connection at first client request
        self.actions_list = {
            "put": self.__put,
            "get": self.__get,
            "del": self.__del,
            "benchmark": self.__benchmark,
            "exit": self.__exit,
        }
        self.socket_map = {}

    def run(self):
        self.logger.debug('Starting DHT client.')

        stop = False
        while not stop:
            try:
                # Getting user input
                cmd = raw_input("$> ")
                
                # cmd = self.dht.stdin.readline()
                cmd_vec = cmd.split()

                action = cmd_vec[0] if cmd_vec else ''
                args = cmd_vec[1:]
                if action in self.actions_list:
                    try:
                        stop, res = self.actions_list[action](*args)
                    except TypeError as t:
                        sys.stdout.write("ERR> Wrong number of arguments.\n")
                
            except KeyboardInterrupt as e:
                sys.stderr.write("\r\n")

    def __get_peer_sock(self, server_id):
        if server_id not in self.socket_map:
            sock = socket(AF_INET, SOCK_STREAM)
            peer = self.dht.peers_map[server_id]
            sock.connect((peer['ip'], peer['port']))
            self.socket_map[server_id] = sock
        return self.socket_map[server_id]

    def __generic_action(self, action, key, args):
        # hash key to get the server id
        server_id = self.dht.server_hash(key)
        # if local call parent, else call or connect to server
        if server_id == self.dht.id:
            method = getattr(self.dht, action)
            res = method(*args)
        else:
            sock = self.__get_peer_sock(server_id)
            exch = MessageExchanger(sock)
            exch.send("%s %s" % (action, " ".join(args)))
            res = exch.recv()
            if res in str2py:
                res = str2py[res]
        return False, res

    def __benchmark(self, cmd_vec):
        # benchmark action first_key count
        action, first_key, count = cmd_vec
        first_key = int(first_key)
        count = int(count)
        results = []
        t0 = time.time()
        for k in range(first_key, first_key+count+1):
            k = str(k)
            if action == "put":
                args = [k, self.data[k]]
            else:
                args = [k]
            _ res = get_attr("__" + action, k, args)
            results.append(res)
        t1 = time.time()
        delta = t1 - t0
        self.logger.info("%d %s operations completed in %.3f seconds." % (count, action, delta))
        self.logger.info("Results: %s" % (Counter(results).most_common(2)))
    
    def __put(self, key, value):
        self.logger.debug("put")
        return self.__generic_action("put", key, [key, value])
    
    def __get(self, key):
        self.logger.debug("get")
        return self.__generic_action("get", key, [key])

    def __del(self, key):
        self.logger.debug("del")
        return self.__generic_action("rem", key, [key])

    def __exit(self):
        self.logger.debug("exit")
        self.dht.terminate.value = 1
        return True, None
        
