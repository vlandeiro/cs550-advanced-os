from multiprocessing import Process, Manager, Value, Lock
import random
from select import select
from socket import *

import sys
import json
import logging
import numpy as np
from CommunicationProtocol import MessageExchanger

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class IndexingServer:
    def __init__(self, config):
        self.replica = config['replica']
        self.timeout_value = config['timeout_value']
        self.listening_ip = config['listening_ip']
        self.listening_port = config['listening_port']
        self.max_connections = config['max_connections']

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)

        self.manager = Manager()
        self.file2peers = self.manager.dict()
        self.peer_status = self.manager.dict()
        self.terminate = Value('i', 0)

        self.listening_socket = None

        self.actions = {
            'register': self._register,
            'list': self._list,
            'search': self._search,
            'close': self._close_connection,
            'init': self._init_connection,
        }

    def _init_connection(self, exch, id):
        self.logger.debug("ID: %s", id)
        peer_status = self.peer_status
        peer_status[id] = 1
        self.peer_status = peer_status
        self.logger.debug(self.peer_status)
        return True

    # TODO: implement automatic unregistration of files
    def _close_connection(self, exch, id):
        return False

    def local_register(self, id, name):
        # add to index
        peers_list = self.file2peers.get(name, set())
        peers_list.add(id)
        self.file2peers[name] = peers_list

    def _register(self, exch, id, name):
        self.local_register(id, name)
        # send back the peers where he need to replicate this file
        self.logger.debug(self.peer_status)
        other_peers = self.peer_status.keys()
        other_peers.remove(id)
        other_peers = list(other_peers)
        self.logger.debug(other_peers)
        if not other_peers:
            other_peers = []
        elif len(other_peers) > self.replica:
            other_peers = list(np.random.choice(other_peers, self.replica, replace=False))

        exch.pkl_send(other_peers)
        for k in other_peers:
            self.local_register(k, name)

        return True

    def _list(self, exch):
        exch.pkl_send(self.file2peers.keys())
        return True

    def _search(self, exch, id, name):
        to_return = []
        if name in self.file2peers:
            # return ids of the peers that have this file
            to_return = [pid for pid in self.file2peers[name] if pid != id]
        exch.pkl_send(to_return)
        return True

    def _generic_action(self, action):
        t = action['type']
        self.logger.debug('Action type: %s', (t))
        if t in self.actions:
            kwargs = {k: action[k] for k in action.keys() if k != 'type'}
            return self.actions[t](**kwargs)
        return None

    def _message_handler(self, client_sock, client_addr):
        self.logger.info("Accepted connection from %s", (client_addr))

        read_list = [client_sock]
        open_conn = True
        while open_conn:
            readable, _, _ = select(read_list, [], [], self.timeout_value)
            if self.terminate.value == 1:
                break
            elif readable:
                exch = MessageExchanger(client_sock)
                action = exch.pkl_recv()
                self.logger.debug(repr(action))
                action['exch'] = exch
                open_conn = self._generic_action(action)
        client_sock.close()

    def run(self):
        """Main function. It handles the connection from peers to the indexing
        server. Everytime a peer connects to the server, a new socket
        is spawned to handle the communication with this specific
        peer. Once the communication is over, this socket is closed.

        """
        self.listening_socket = socket(AF_INET, SOCK_STREAM)
        self.listening_socket.setblocking(0)
        self.listening_socket.bind(("0.0.0.0", self.listening_port))
        self.listening_socket.listen(self.max_connections)
        logger.info("Indexing server listening on port %d", self.listening_port)

        read_list = [self.listening_socket]
        try:
            while True:
                readable, _, _ = select(read_list, [], [], self.timeout_value)
                if readable:
                    client_sock, client_addr = self.listening_socket.accept()
                    handler = Process(target=self._message_handler, args=(client_sock, client_addr))
                    handler.daemon = True
                    handler.start()
        except KeyboardInterrupt:
            sys.stderr.write("\r")
            logger.info("Shutting down Indexing Server.")
        finally:
            self.terminate.value = 1
            self.listening_socket.close()


def print_usage(args):
    print "Usage: python %s config.json" % args[0]
    sys.exit(1)


if __name__ == '__main__':
    args = sys.argv
    if len(args) != 2:
        print_usage(args)

    with open(args[1]) as config_fd:
        run_args = json.load(config_fd)
    indexingServer = IndexingServer(run_args)
    indexingServer.run()
