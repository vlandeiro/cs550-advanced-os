import json
import logging
import sys

from select import select
from CommunicationProtocol import *
from multiprocessing import Process
from socket import *

logging.basicConfig(level=logging.DEBUG)

class DHTServer(Process):
    def __init__(self, parent):
        super(DHTServer, self).__init__()

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

        self.parent = parent
        self.listening_socket = None
        
        self.actions_list = {
            "put": self._put,
            "get": self._get,
            "rem": self._del,
            "keys": self._keys
        }
        self.socket_list = []

    def _keys(self):
        self.logger.debug("keys")
        return self.parent.keys()

    def _put(self, key, value):
        self.logger.debug("put")
        self.parent.put(key, value)
        return True

    def _get(self, key):
        self.logger.debug("get")
        return self.parent.get(key)

    def _del(self, key):
        self.logger.debug("del")
        self.parent.rem(key)
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
                    self.logger.debug("Request %s received." % (cmd))

                    if action not in self.actions_list:
                        exch.send(py2str[None])
                    else:
                        res = self.actions_list[action](*args)
                        if res in py2str:
                            exch.send(py2str[res])
                        else:
                            exch.send(res)
                if self.parent.terminate.value == 1:
                    break
        except KeyboardInterrupt as e:
            self.parent.terminate.value = 1
            sock.close()
    
    def run(self):
        self.logger.info('Starting DHT server.')
        self.listening_socket = socket(AF_INET, SOCK_STREAM)
        self.listening_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.listening_socket.bind(("0.0.0.0", self.parent.port))
        self.listening_socket.listen(self.parent.peers_count)
        self.logger.info("DHT server listening on port %d." % (self.parent.port))

        read_list = [self.listening_socket]
        try:
            while True:
                readable, _, _ = select(read_list, [], [], 0.1)
                if self.parent.terminate.value == 1:
                    break
                elif readable:
                    in_sock, in_addr = self.listening_socket.accept()
                    handler = Process(target=self._message_handler, args=(in_sock, in_addr))
                    handler.daemon = True
                    handler.start()
        except KeyboardInterrupt:
            sys.stderr.write("\r")
            self.logger.debug("Shutting down DHT server.")
            self.parent.terminate.value = 1
        finally:
            self.listening_socket.close()


    
