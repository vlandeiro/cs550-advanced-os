"""
Usage:
   IndexingServer.py run <ip> <port> [--max-conn=M]

Options:
    --help -h       Display this screen.
    --max-conn=M    Maximum number of allowed connections to the server [default: 10].
"""
from docopt import docopt
from multiprocessing import Process, Manager
from socket import *

import logging
import CommunicationProtocol as proto

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class IndexingServer:
    def __init__(self, host, port, max_connect=10):
        self.host = host
        self.port = port
        self.max_connect = max_connect

        self.manager = Manager()
        self.peers_info = self.manager.dict()
        self.file2peers = self.manager.dict()
        self.files_info = self.manager.dict()
        # TODO (optional): add a poll mechanism to watch dead peers
        
        self.listening_socket = None

        self.actions = {
            'echo': self.__action_echo,
            'register': self.__action_register,
            'list': self.__action_list,
            'lookup': self.__action_lookup,
            'close_connection': self.__action_close_connection,
            'init': self.__action_init
        }

    def __action_init(self, msg_exch, cmd_vec):
        logger.debug("Action is an init")
        type_val = {"port": int, "addr": str}
        peerid = int(cmd_vec[1])
        peer_dict = self.peers_info.get(peerid, {})
        k = cmd_vec[2]
        v = type_val[k](cmd_vec[3])
        peer_dict[k] = v
        self.peers_info[peerid] = peer_dict
        return True

    def __action_close_connection(self, msg_exch, cmd_vec):
        peerid = int(cmd_vec[1])
        logger.debug("Closing connection to peer")
        # remove all registered files
        if 'files' in self.peers_info[peerid]:
            for f_name in self.peers_info[peerid]['files']:
                l = self.file2peers[f_name]
                l.remove(peerid)
                # if no other peer has this file, remove entry
                if not l:
                    del self.file2peers[f_name]
                    del self.files_info[f_name]
                # else update list of peers
                else:
                    self.file2peers[f_name] = l
        del self.peers_info[peerid]
        return False
    
    def __action_echo(self, msg_exch, cmd_vec):
        logger.debug("Action is an echo")
        dummy = msg_exch.recv()
        response = " ".join(cmd_vec[1:])
        msg_exch.send(response)
        return True

    def __action_register(self, msg_exch, cmd_vec):
        logger.debug("Action is a register")
        peerid = int(cmd_vec[1])
        while True:
            f_tuple = msg_exch.pkl_recv()
            if f_tuple is None:
                break
            f_name = f_tuple[0]
            # add to file info dict
            self.files_info[f_name] = f_tuple
            # add to peer info
            peer_dict = self.peers_info[peerid]
            if "files" in peer_dict:
                peer_dict['files'].append(f_name)
            else:
                peer_dict['files'] = [f_name]
            self.peers_info[peerid] = peer_dict
            # add to index
            if f_name in self.file2peers:
                self.file2peers[f_name].append(peerid)
            else:
                self.file2peers[f_name] = [peerid]
            msg_exch.send_ack()
        return True
    
    def __action_list(self, msg_exch, cmd_vec):
        logger.debug("Action is a list")
        dummy = msg_exch.recv()
        available_files = self.files_info.values()
        msg_exch.pkl_send(available_files)
        return True

    def __action_lookup(self, msg_exch, cmd_vec):
        logger.debug("Action is a lookup")
        filename = cmd_vec[1]
        peerid = -1
        if len(cmd_vec) > 2:
            peerid = int(cmd_vec[2])
        if filename in self.file2peers:
            # return ids of the peers that have this file
            peer_ids = [x for x in self.file2peers[cmd_vec[1]] if x != peerid]
            msg_exch.pkl_send(peer_ids)
        else: # file not registered by any peer
            msg_exch.pkl_send([])
        return True
    
    def message_handler(self, client_so, client_addr):
        logger.debug("Accepted connection from %s", client_addr)
        msg_exch = proto.MessageExchanger(client_so)
        
        open_conn = True
        while open_conn:
            msg = msg_exch.recv()
            cmd_vec = msg.split()
            action = cmd_vec[0]
            if action not in self.actions:
                msg_exch.send_err()
            else:
                msg_exch.send_ack()
                open_conn = self.actions[action](msg_exch, cmd_vec)
                logger.debug(self.peers_info)
                logger.debug(self.file2peers)
                logger.debug(self.files_info)
        client_so.close()

    def run(self):
        """Main function. It handles the connection from peers to the indexing
        server. Everytime a peer connects to the server, a new socket
        is spawned to handle the communication with this specific
        peer. Once the communication is over, this socket is closed.

        """
        self.listening_socket = socket(AF_INET, SOCK_STREAM)
        self.listening_socket.bind((self.host, self.port))
        self.listening_socket.listen(self.max_connect)
        logger.debug("Indexing server listening on port %d", self.port)

        while True:
            client_so, client_addr = self.listening_socket.accept()
            handler = Process(target=self.message_handler, args=(client_so, client_addr))
            handler.daemon = True
            handler.start()
        self.listening_socket.close()

if __name__ == '__main__':
    args = docopt(__doc__)
    #print args
    indexingServer = IndexingServer(args['<ip>'], int(args['<port>']), int(args['--max-conn']))
    indexingServer.run()
