"""
Usage:
   python IndexingServer.py <config.json>
"""
from multiprocessing import Process, Manager
from socket import *

import sys
import json
import logging
import CommunicationProtocol as proto

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# TODO:
# - avoid registering of same peer
# - select folder to output downloaded files

class IndexingServer:
    def __init__(self, listening_ip, listening_port, pool_size=10):
        self.listening_ip = listening_ip
        self.listening_port = listening_port
        self.pool_size = pool_size

        self.manager = Manager()
        self.peers_info = self.manager.dict()
        self.file2peers = self.manager.dict()
        self.files_info = self.manager.dict()
        
        self.listening_socket = None

        self.actions = {
            'echo': self.__action_echo,
            'register': self.__action_register,
            'list': self.__action_list,
            'lookup': self.__action_lookup,
            'close_connection': self.__action_close_connection,
            'init': self.__action_init,
            'get_peer': self.__action_get_peer
        }

    def __action_get_peer(self, msg_exch, cmd_vec):
        peerid = int(cmd_vec[1])
        if peerid in self.peers_info:
            d = self.peers_info[peerid]
            msg_exch.pkl_send(d)
        else:
            msg_exch.pkl_send(None)
        return True

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
        logger.debug("Closing connection to peer " + str(peerid))
        
        # remove all files registered by this peer
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
                peer_dict['files'].add(f_name)
            else:
                peer_dict['files'] = set([f_name])
            self.peers_info[peerid] = peer_dict
            # add to index
            peers_list = self.file2peers.get(f_name, set())
            peers_list.add(peerid)
            self.file2peers[f_name] = peers_list
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
        dummy = msg_exch.recv()

        search = cmd_vec[1]
        peerid = -1
        if len(cmd_vec) > 2:
            peerid = int(cmd_vec[2])
        if search in self.file2peers:
            # return ids of the peers that have this file
            to_return = []
            for pid in self.file2peers[search]:
                if pid in self.peers_info and pid != peerid:
                    to_return.append(self.peers_info[pid])
            msg_exch.pkl_send(to_return)
        else: # file not registered by any peer
            msg_exch.pkl_send([])
        return True
    
    def __message_handler(self, client_so, client_addr):
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
        client_so.close()

    def run(self):
        """Main function. It handles the connection from peers to the indexing
        server. Everytime a peer connects to the server, a new socket
        is spawned to handle the communication with this specific
        peer. Once the communication is over, this socket is closed.

        """
        self.listening_socket = socket(AF_INET, SOCK_STREAM)
        self.listening_socket.bind((self.listening_ip, self.listening_port))
        self.listening_socket.listen(self.pool_size)
        logger.debug("Indexing server listening on port %d", self.listening_port)

        try:
            while True:
                logger.debug("Entering the infinite loop")
                client_so, client_addr = self.listening_socket.accept()
                handler = Process(target=self.__message_handler, args=(client_so, client_addr))
                handler.daemon = True
                handler.start()
        except KeyboardInterrupt:
            sys.stderr.write("\r")
            logger.info("Shutting down Indexing Server.")
        finally:
            self.listening_socket.close()

def usage_error():
    print(__doc__.strip())
    sys.exit(1)

if __name__ == '__main__':
    args = sys.argv
    if len(args) != 2:
        usage_error()

    with open(args[1]) as config_fd:
        run_args = json.load(config_fd)
    indexingServer = IndexingServer(**run_args)
    indexingServer.run()
        
