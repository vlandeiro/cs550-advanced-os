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
            "put": self._put,
            "get": self._get,
            "rem": self._del
        }
        self.socket_list = []

    def _put(self, key, value):
        self.logger.debug("put")
        self.dht.put(key, value)
        return True

    def _get(self, key):
        self.logger.debug("get")
        return self.dht.get(key)

    def _del(self, key):
        self.logger.debug("del")
        self.dht.rem(key)
        return True
    
    def _message_handler(self, sock, addr):
        try:
            sock.setblocking(0)
            self.socket_list.append(sock)
            
            exch = MessageExchanger(sock)

            while True:
                try:
                    readable, _, _ = select([sock], [], [], 0.1)
                except error:
                    sock.shutdown(2)
                    sock.close()
                    self.socket_list.remove(sock)
                    break
                if readable:
                    cmd = exch.recv()
                    if cmd is None:
                        continue
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
                    handler = Process(target=self._message_handler, args=(in_sock, in_addr))
                    handler.daemon = True
                    handler.start()
        except KeyboardInterrupt:
            sys.stderr.write("\r")
            self.logger.debug("Shutting down DHT server.")
            self.dht.terminate.value = 1
        finally:
            self.listening_socket.close()


    
