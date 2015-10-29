import logging
import os

from socket import *
from CommunicationProtocol import MessageExchanger
from select import select

logging.basicConfig(level=logging.DEBUG)

class PeerServer(Process):
    def __init__(self, parent):
        super(PeerServer, self).__init__()

        self.parent = parent
        self.socket = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self.actions = {
            'obtain': self._obtain,
            'replicate': self._recv_replica
        }
        self.listening_socket = socket(AF_INET, SOCK_STREAM)
        self.listening_socket.bind(("0.0.0.0", self.listening_port))
        self.listening_socket.listen(self.pool_size)

    def _obtain(self, filename, exch):
        files_dict = self.parent.files_dict
        if filename in files_dict:
            f_name, f_size, f_path = files_dict[filename]
            return exch.file_send(f_path)
        return False

    def _recv_replica(self, filename, exch):
        files_dict = self.parent.files_dict
        fpath = os.path.join(self.parent.download_dir, filename)
        exch.file_recv(fpath, show_progress=False)
    
    def _init_server_connection(self):
        idx_action = dict(
            type='init',
            addr=self.ip,
            port=self.port
        )
        self.idxserv_exch.pkl_send(idx_action)
        return True

    def _generic_action(self, action):
        if action['type'] in self.actions:
            kwargs = {k: action['k'] for k in action.keys() if k != 'type'}
            self.actions(**kwargs)
            
    def _peer_message_handler(self, peer_sock, peer_addr):
        logger.debug("Accepted connection from %s", peer_addr)
        peer_exch = MessageExchanger(peer_sock)

        open_conn = True
        while open_conn:
            action = peer_exch.pkl_recv()
            action['exch'] = peer_exch
            open_conn = self._generic_action(action)
        peer_so.close()

    def run(self):
        """This function handles the connection from other peer to obtain
        files.
        
        """
        self.listening_socket = socket(AF_INET, SOCK_STREAM)
        self.listening_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.listening_socket.bind(("0.0.0.0", self.parent.this_port))
        self.listening_socket.listen()
        self.logger.debug("Peer server listening on port %d", self.listening_port)

        read_list = [self.listening_socket]
        try:
            while True:
                readable, _, _ = select(read_list, [], [], self.parent.timeout_value)
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
