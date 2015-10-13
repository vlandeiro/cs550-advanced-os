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
        self.logger.setLevel(logging.INFO)
        self.dht = dht

        # load key/val data for benchmark
        self.logger.info("Loading data in memory for the benchmark...")
        self.data = {}
        with open("keyval.data") as data_fd:
            for line in data_fd:
                k, v = line.split()
                self.data[k] = v
        self.logger.info("Benchmark data successfully loaded.")
        # keep map of sockets connected with peers
        # init connection at first client request
        self.actions_list = {
            "put": self._put,
            "get": self._get,
            "del": self._del,
            "benchmark": self._benchmark,
            "exit": self._exit,
        }
        self.socket_map = {}

    def run(self):
        self.logger.info('Starting DHT client.')

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
                        self.logger.error(repr(t))
            except KeyboardInterrupt as e:
                sys.stderr.write("\r\n")
            except EOFError as e:
                self.dht.terminate.value = 1

    def _get_peer_sock(self, server_id):
        if server_id not in self.socket_map:
            sock = socket(AF_INET, SOCK_STREAM)
            peer = self.dht.peers_map[server_id]
            sock.connect((peer['ip'], peer['port']))
            self.socket_map[server_id] = sock
        return self.socket_map[server_id]

    def _generic_action(self, action, key, args, print_output=True):
        # hash key to get the server id
        server_id = self.dht.server_hash(key)
        # if local call parent, else call or connect to server
        if server_id == self.dht.id:
            self.logger.debug("local %s" % (action))
            method = getattr(self.dht, action)
            res = method(*args)
        else:
            self.logger.debug("network %s" % (action))
            sock = self._get_peer_sock(server_id)
            exch = MessageExchanger(sock)
            exch.send("%s %s" % (action, " ".join(args)))
            res = exch.recv()
            if res in str2py:
                res = str2py[res]
        if print_output:
            print("RET> %s" % res)
        return False, res

    def _benchmark(self, action, first_key, count):
        # benchmark action first_key count
        first_key = int(first_key)
        count = int(count)
        results = []
        t0 = time.time()
        R = range(first_key, first_key+count)
        for k in R:
            k = str(k)
            if action == "put":
                args = [k, self.data[k], False]
            else:
                args = [k, False]
            func = getattr(self, "_" + action)
            _, ret = func(*args)
            results.append(ret)
        t1 = time.time()
        delta = t1 - t0
        self.logger.info("Results check:")
        if action == 'get':
            results = [True if results[i] == self.data[str(k)] else False for i, k in enumerate(R)]
        self.logger.info("Results: %s" % (repr(Counter(results).most_common(2))))
        self.logger.info("%d %s operations completed in %.3f seconds." % (count, action, delta))
        return False, None

    
    def _put(self, key, value):
        self.logger.debug("put")
        return self._generic_action("put", key, [key, value])
    
    def _get(self, key):
        self.logger.debug("get")
        return self._generic_action("get", key, [key])

    def _del(self, key):
        self.logger.debug("del")
        return self._generic_action("rem", key, [key])

    def _exit(self):
        self.logger.debug("exit")
        self.dht.terminate.value = 1
        return True, None
        
