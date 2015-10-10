import json
import logging

from dht_protocol import *
from multiprocessing import Process

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
        return self.dht.put(key, value)

    def __get(self, key):
        return self.dht.put(key)

    def __del(self, key):
        return self.dht.del(key)
    
    def __message_handler(self, sock, addr):
        self.socket_list.append(sock)
        exch = MessageExchanger(sock)

        while True:
            cmd = exch.recv()
            cmd_vec = cmd.split()
            action = cmd_vec[0]
            args = cmd_vec[1:]

            if action not in self.actions_list:
                exch.send(py2str(None))
            else:
                res = self.actions_list[action](*args)
                exch.send(py2str(res))
    
    def run(self):
        self.logger.debug('Starting DHT server.')
        self.listening_socket = socket(AF_INET, SOCK_STREAM)
        self.listening_socket.bind(("0.0.0.0", self.dht.port))
        self.listening_socket.listen(self.dht.peers_count)
        self.logger.info("DHT server listening on port %d", self.dht.port)

        try:
            while True:
                in_sock, in_addr = self.listening_socket.accept()
                handler = Process(target=self.__message_handler, args=(in_sock, in_addr))
                handler.daemon = True
                handler.start()
        except KeyboardInterrupt:
            sys.stderr.write("\r")
            logger.info("Shutting down DHT server.")
        finally:
            self.listening_socket.close()


    
