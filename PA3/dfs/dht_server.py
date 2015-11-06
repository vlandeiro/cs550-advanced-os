import logging
import sys

from select import select
from CommunicationProtocol import *
from multiprocessing import Process
from socket import *

logging.basicConfig(level=logging.DEBUG)


class DHTServer(Process):
    def __init__(self, parent):
        """
        Initialize a distributed hash table server object.
        :param parent: DHT object.
        :return: None
        """
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
        """
        Call its parent function to get the keys of local hashtable.
        :return: keys of the local hash table.
        """
        self.logger.debug("keys")
        return self.parent.keys()

    def _put(self, key, value):
        """
        Call its parent function to put a key-value pair in the local hashtable.
        :param key: key to put in the local hashtable.
        :param value: value to put in the local hashtable.
        :return: True
        """
        self.logger.debug("put")
        self.parent.put(key, value)
        return True

    def _get(self, key):
        """
        Call its parent function to get the value associated with a given key in the local hashtable.
        :param key: key to put in the local hashtable.
        :return: value associated with the key.
        """
        self.logger.debug("get")
        return self.parent.get(key)

    def _del(self, key):
        """
        Call its parent function to remove an entry from the local hashtable.
        :param key: key to remove in the local hashtable.
        :return: True
        """
        self.logger.debug("del")
        self.parent.rem(key)
        return True

    def _message_handler(self, sock, addr):
        """
        Handle the message received by this server by parsing the messages and calling the corresponding action.
        :param sock: socket to listen to new messages.
        :param addr: address of the client sending messages.
        :return: None
        """
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
                    dht_action = exch.pkl_recv()
                    if dht_action is None:
                        break
                    action = dht_action['action']
                    args = dht_action['args']
                    self.logger.debug("Request %s received." % (action))

                    if action not in self.actions_list:
                        exch.pkl_send(None)
                    else:
                        res = self.actions_list[action](*args)
                        exch.pkl_send(res)
                if self.parent.terminate.value == 1:
                    break
        except KeyboardInterrupt as e:
            self.parent.terminate.value = 1
            sock.close()

    def run(self):
        self.logger.info('Starting the distributed indexing server.')
        self.listening_socket = socket(AF_INET, SOCK_STREAM)
        self.listening_socket.setblocking(0)
        self.listening_socket.bind(("0.0.0.0", self.parent.port))
        self.listening_socket.listen(self.parent.nodes_count)

        read_list = [self.listening_socket]
        try:
            while True:
                readable, _, _ = select(read_list, [], [], self.parent.timeout_value)
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
