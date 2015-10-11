import json
import logging
import sys

from select import select
from dht_protocol import *
from multiprocessing import Process
from socket import *

logging.basicConfig(level=logging.DEBUG)

class DHTServer(Process):
    def __init__(self, dht):
        super(DHTServer, self).__init__()

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)

        self.dht = dht
        self.listening_socket = None
        
        self.actions_list = {
            "put": self.__put,
            "get": self.__get,
            "del": self.__del
        }
        self.socket_list = []

    def __put(self, key, value):
        self.logger.debug("put")
        return self.dht.put(key, value)

    def __get(self, key):
        self.logger.debug("get")
        return self.dht.get(key)

    def __del(self, key):
        self.logger.debug("del")
        return self.dht.rem(key)
    
    def __message_handler(self, sock, addr):
        try:
            sock.setblocking(0)
            self.socket_list.append(sock)
            
            exch = MessageExchanger(sock)

            while True:
                readable, _, _ = select([sock], [], [], 0.1)
                if readable:
                    cmd = exch.recv()
                    cmd_vec = cmd.split()
                    action = cmd_vec[0]
                    args = cmd_vec[1:]

                    if action not in self.actions_list:
                        exch.send(py2str[None])
                    else:
                        res = self.actions_list[action](*args)
                        if res in py2str:
                            exch.send(py2str[res])
                        else:
                            exch.send(res)
                if self.dht.terminate.value == 1:
                    break
        except KeyboardInterrupt as e:
            self.dht.terminate.value = 1
    
    def run(self):
        self.logger.debug('Starting DHT server.')
        self.listening_socket = socket(AF_INET, SOCK_STREAM)
        self.listening_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.listening_socket.bind(("0.0.0.0", self.dht.port))
        self.listening_socket.listen(self.dht.peers_count)
        self.logger.debug("DHT server listening on port %d", self.dht.port)

        read_list = [self.listening_socket]
        try:
            while True:
                readable, _, _ = select(read_list, [], [], 0.1)
                if self.dht.terminate.value == 1:
                    break
                elif readable:
                    in_sock, in_addr = self.listening_socket.accept()
                    handler = Process(target=self.__message_handler, args=(in_sock, in_addr))
                    handler.daemon = True
                    handler.start()
        except KeyboardInterrupt:
            sys.stderr.write("\r")
            self.logger.debug("Shutting down DHT server.")
            self.dht.terminate.value = 1
        finally:
            self.listening_socket.close()


    