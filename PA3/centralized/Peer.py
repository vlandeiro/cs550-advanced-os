"""
Usage:
    python Peer.py <config.json>
 """
from multiprocessing import Process, Manager, Queue, Value
from socket import *

import logging
import json
import sys
import errno
import CommunicationProtocol as proto
import glob
import textwrap
import os
import os.path
import time
import random

logging.basicConfig(level=logging.DEBUG)
LISTENING_TIMEOUT = 0.2

class PeerServer(Process):
    def __init__(self, parent):
        super(PeerServer, self).__init__()

        self.parent = parent
        self.socket = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        self.actions = {
            'obtain': self._action_obtain
        }

    def _action_obtain(self, msg_exch, cmd_vec):
        f_req = cmd_vec[1]
        files_dict = self.parent.files_dict
        self.logger.debug("FILES DICT KEYS: " + str(files_dict.keys()))
        if f_req in files_dict:
            f_name, f_size, f_path = files_dict[f_req]
            return msg_exch.file_send(f_path)
        return False

    def run(self):
        pass
    
class Peer:
    def __init__(self, listening_ip, listening_port, idxserv_ip, idxserv_port,
                 pool_size=10, download_dir="./"):
        self.download_dir = os.path.abspath(download_dir)
        if not os.path.isdir(self.download_dir):
            raise ValueError("Download directory does not exist")

        self.listening_socket = None
        self.idxserv_socket = None
        self.idxserv_exch = None
        self.peer_socket = None

        self.manager = Manager()
        self.files_dict = self.manager.dict()
        self.terminate = Value('i', 0)
    
    def _init_connection(self):
        idx_action = dict(
            type='init',
            addr=self.ip,
            port=self.port
        )
        self.idxserv_exch.pkl_send(idx_action)
        return True
    
    def __peer_message_handler(self, peer_so, peer_addr):
        logger.debug("Accepted connection from %s", peer_addr)
        fc_msg_exch = proto.MessageExchanger(peer_so)

        open_conn = True
        while open_conn:
            msg = fc_msg_exch.recv()
            cmd_vec = msg.split()
            action = cmd_vec[0]
            if action not in self.client_actions:
                fc_msg_exch.send_err()
            else:
                fc_msg_exch.send_ack()
                open_conn = self.client_actions[action](fc_msg_exch, cmd_vec)
        peer_so.close()

    def __quit_server(self):
        if self.listening_socket is not None:
            self.listening_socket.close()
            self.listening_socket = None

    def __run_server(self):
        """This function handles the connection from other peer to obtain
        files.
        
        """
        self.listening_socket = socket(AF_INET, SOCK_STREAM)
        self.listening_socket.settimeout(LISTENING_TIMEOUT)
        self.listening_socket.bind(("0.0.0.0", self.listening_port))
        self.listening_socket.listen(self.pool_size)
        logger.debug("Peer server listening on port %d", self.listening_port)

        self.server_running.value = 1
        while True:
            try:
                peer_so, peer_addr = self.listening_socket.accept()
                handler = Process(target=self.__peer_message_handler,
                                  args=(peer_so, peer_addr))
                handler.daemon = True
                handler.start()
            except KeyboardInterrupt:
                pass
            except timeout as e:
                try:
                    if not self.server_running.value:
                        print("Shutting down service to other peers.")
                # avoid Broken Pipe error registered as a bug in python 2.7
                except IOError as e: 
                    if e.errno == 32:
                        pass
                    else:
                        raise
                    break

        self.__quit_server()
        
    def __quit_ui(self):
        logger.debug("Quitting UI")
        if self.idxserv_socket is not None and self.ui_running:
            print("Closing connection to indexing server.")
            self.idxserv_msg_exch.send("close_connection %d" % id(self), ack=True)
            self.idxserv_socket.close()
            self.idxserv_socket = None
        try:
            self.server_running.value = 0
        except IOError as e:
            if e.errno == 32:
                pass
            else:
                raise
        finally:
            self.ui_running = False
        return False

    def run(self):
        """Function that launches the different parts (server, user interface,
        client, file management, ...) of the peer.

        """
        try:
            # First, start the server in a dedicated thread.
            logger.debug("Starting the peer server.")
            server = Process(target=self.__run_server)
            server.start()
            logger.debug("Peer server running.")

            # Then, start the user interface
            logger.debug("Starting the user interface.")
            self.__run_ui()
        except EOFError as e:
            print "\nShutting down peer."
        except:
            self.__quit_ui()
            raise

def usage_error():
    print(__doc__.strip())
    sys.exit(1)

def format_filesize(f_size):
    prefixes = ['', 'K', 'M', 'G', 'T', 'P']
    # change file size to human readable
    for prefix in prefixes:
        if f_size < 1024:
            break
        f_size = 1.*f_size/1024.
    f_size_str = "%.1f%sB" % (f_size, prefix)
    return f_size_str

def sample_with_replacement(l, k):
    if l:
        lt = l*k
        return random.sample(lt, k)
    else:
        return []

if __name__ == '__main__':
    # parse arguments
    args = sys.argv
    if len(args) != 2:
        usage_error()
    with open(args[1]) as config_fd:
        run_args = json.load(config_fd)
        
    peer = Peer(**run_args)
    peer.run()
