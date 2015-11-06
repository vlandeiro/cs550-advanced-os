import logging
import os

from multiprocessing import Process
from socket import *
import time
from CommunicationProtocol import MessageExchanger
from select import select

logging.basicConfig(level=logging.DEBUG)


class PeerServer(Process):
    def __init__(self, parent):
        """
        Initialize the PeerServer object.
        :param parent: Node object.
        :return: None
        """
        super(PeerServer, self).__init__()

        self.parent = parent
        self.socket = None
        self.logger = logging.getLogger(self.__class__.__name__)
        level = logging.getLevelName(parent.log_level)
        self.logger.setLevel(level)
        self.actions = {
            'obtain': self._obtain,
            'replicate': self._recv_replica,
            'exit': self._close_socket
        }
        self.ip = parent.ip
        self.port = parent.file_server_port
        self.listening_socket = socket(AF_INET, SOCK_STREAM)
        self.listening_socket.setblocking(0)
        self.listening_socket.bind(("0.0.0.0", self.port))
        self.listening_socket.listen(parent.max_connections)


    def _close_socket(self, exch, id):
        nodes_status = self.parent.nodes_status
        nodes_status[id] = False
        self.parent.nodes_status = nodes_status

    def _obtain(self, name, exch):
        """
        Action executed when a peer ask for a file. Acknowledge the request and then send the file.
        :param name: name of the file requested.
        :param exch: MessageExchanger with the peer that made the request.
        :return: False
        """
        try:
            fpath = self.parent.local_files[name]
            exch.pkl_send(True)
            exch.file_send(fpath)
            return True
        except KeyError as e:
            exch.pkl_send(False)
            return True

    def _recv_replica(self, exch, name):
        """
        Action executed when another peer pushes a replica to this peer.
        :param name: name of the file requested.
        :param exch: MessageExchanger with the peer that made the request.
        :return: False
        """
        local_files = self.parent.local_files
        fpath = os.path.join(self.parent.download_dir, name)
        local_files[name] = os.path.abspath(fpath)
        self.parent.local_files = local_files
        exch.file_recv(fpath, show_progress=False)
        return True

    def _generic_action(self, action):
        """
        Parse the action given as a parameter and call the corresponding function.
        :param action: action to call passed as a python dictionary.
        :return: result of the method called.
        """
        t = action['type']
        if t in self.actions:
            kwargs = {k: action[k] for k in action.keys() if k != 'type'}
            return self.actions[t](**kwargs)

    def _peer_message_handler(self, peer_sock, peer_addr):
        """
        Handle messages sent by another peer.
        :param peer_sock: socket to communicate with the other peer.
        :param peer_addr: address of the other peer.
        :return: None
        """
        self.logger.debug("Accepted connection from %s", peer_addr)
        peer_exch = MessageExchanger(peer_sock)
        open_conn = True
        while open_conn != False:
            self.logger.debug("%s: %s", repr(peer_sock), open_conn)
            action = peer_exch.pkl_recv()
            self.logger.debug(repr(action))
            action['exch'] = peer_exch
            open_conn = self._generic_action(action)
        peer_sock.close()
        self.parent.client.peers_sock[peer_addr] = None
        self.parent.client.peers_check[peer_addr] = time.time()

    def run(self):
        """
        This function handles the connection from other peer to obtain files.
        :return: None
        """
        self.logger.info("Starting the file server.")

        read_list = [self.listening_socket]
        try:
            while True:
                readable, _, _ = select(read_list, [], [], self.parent.config['timeout_value'])
                if self.parent.terminate.value == 1:
                    break
                elif readable:
                    peer_sock, peer_addr = self.listening_socket.accept()
                    handler = Process(target=self._peer_message_handler,
                                      args=(peer_sock, peer_addr))
                    handler.daemon = True
                    handler.start()
        except KeyboardInterrupt:
            pass
        finally:
            self.listening_socket.close()
