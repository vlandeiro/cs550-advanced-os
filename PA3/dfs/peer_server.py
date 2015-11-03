import logging
from multiprocessing import Process
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
        level = logging.getLevelName(parent.config.get('log', 'INFO'))
        self.logger.setLevel(level)
        self.actions = {
            'obtain': self._obtain,
            'replicate': self._recv_replica
        }
        self.ip = parent.this_ip
        self.port = parent.config['file_server_port']
        self.listening_socket = socket(AF_INET, SOCK_STREAM)
        self.listening_socket.setblocking(0)
        self.listening_socket.bind(("0.0.0.0", self.port))
        self.listening_socket.listen(parent.config['max_connections'])

    def _obtain(self, name, exch):
        try:
            fpath = self.parent.local_files[name]
            exch.pkl_send(True)
            exch.file_send(fpath)
            return False
        except KeyError as e:
            exch.pkl_send(False)
            return False

    def _recv_replica(self, exch, name):
        local_files = self.parent.local_files
        fpath = os.path.join(self.parent.download_dir, name)
        local_files[name] = os.path.abspath(fpath)
        self.parent.local_files = local_files
        exch.file_recv(fpath, show_progress=False)
        return False
    
    def _generic_action(self, action):
        t = action['type']
        if t in self.actions:
            kwargs = {k: action[k] for k in action.keys() if k != 'type'}
            return self.actions[t](**kwargs)
            
    def _peer_message_handler(self, peer_sock, peer_addr):
        self.logger.debug("Accepted connection from %s", peer_addr)
        peer_exch = MessageExchanger(peer_sock)

        open_conn = True
        while open_conn != False:
            self.logger.debug("%s: %s", repr(peer_sock), open_conn)
            action = peer_exch.pkl_recv()
            action['exch'] = peer_exch
            open_conn = self._generic_action(action)
        peer_sock.close()

    def run(self):
        """This function handles the connection from other peer to obtain
        files.
        
        """
        self.logger.debug("Peer server listening on port %d", self.port)

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
