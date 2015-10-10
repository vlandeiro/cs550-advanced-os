import json
import logging
import sys

from multiprocessing import Process
from dht_protocol import *

logging.basicConfig(level=logging.DEBUG)

class DHTClient(Process):
    def __init__(self, dht):
        super(DHTClient, self).__init__()

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)

        self.dht = dht
        # keep map of sockets connected with peers, init connection at first client request

        self.actions_list = {
            "put": self.__put,
            "get": self.__get,
            "del": self.__del,
            "exit": self.__exit,
        }
        self.socket_map = {}

    def run(self):
        self.logger.debug('Starting DHT client.')

        stop = False
        while not stop:
            try:
                print("$>", end='', flush=True)
                # Getting user input
                cmd = raw_input()
                cmd_vec = cmd_str.split()

                action = cmd_vec[0] if cmd_vec else ''
                args = cmd_vec[1:]
                if action in actions_list:
                    stop, res = action(*args)

            except KeyboardInterrupt as e:
                sys.stderr.write("\r\n")

    def __get_peer_sock(self, server_id):
        if server_id not in self.socket_map:
            sock = socket(AF_INET, SOCK_STREAM)
            peer = self.dht.peers_map[server_id]
            sock.connect((peer['ip'], peer['port']))
            self.socket_map[server_id] = sock
        return self.socket_map[server_id]

    def __generic_action(self, action, args):
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
            res = str2py(exch.recv())
        return False, res
        
    def __put(self, key, value):
        return self.__generic_action("put", [key, value])
    
    def __get(self, key):
        return self.__generic_action("get", [key])

    def __del(self, key):
        return self.__generic_action("del", [key])
        return False, res

    def __exit(self):
        return True, None
        
