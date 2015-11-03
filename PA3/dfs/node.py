from multiprocessing import Manager, Value

import logging
import json
import sys
import os

from urllib2 import urlopen
from distributed_indexing_server import DHT
from peer_client import PeerClient
from peer_server import PeerServer

logging.basicConfig(level=logging.DEBUG)
    
class Peer:
    def __init__(self, config):
        # set up logger
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)

        # read and parse configuration
        self.config = config
        self.idx_type = config['idx_type'] # centralized or distributed

        # configuration parameters shared by both implementations
        self.download_dir = os.path.abspath(config['download_dir'])
        if not os.path.isdir(self.download_dir):
            raise ValueError("Download directory does not exist")
        self.idx_server_port = config['idx_server_port']
        self.file_server_port = config['file_server_port']
        self.log_level = config['log_level']
        self.timeout_value = config['timeout_value']
        self.max_connections = config['max_connections']

        if self.idx_type == 'distributed':
            self.nodes_list = config['nodes']
            self.nodes_count = len(self.nodes_list)
            self.replica = config['replica']
            # get this server info from config file
            self.this_ip = str(json.load(urlopen('https://api.ipify.org/?format=json'))['ip'])
            if self.this_ip not in self.nodes_list:
                raise ValueError("peer %s is not included in the config file." % self.this_ip)
            self.id = self.nodes_list.index(self.this_ip)

        # create shared dictionary to store the paths to the local files
        self.manager = Manager()
        self.local_files = self.manager.dict()
        self.terminate = Value('i', 0)

        # create main processes
        self.client = PeerClient(self)
        self.file_server = PeerServer(self)
        if self.idx_type == 'distributed':
            self.dht = DHT(config, self.terminate)

    def run(self):
        """Function that launches the different parts (server, user interface,
        client, file management, ...) of the peer.

        """
        try:
            # First, start the file server in a dedicated thread.
            self.file_server.start()
            # After that, run the indexing server if the config is set to distributed
            if self.idx_type == 'distributed':
               self.dht.server.start()
            
            # Then, start the user interface
            self.logger.debug("Starting the user interface.")
            self.client.run()
            # Join the background processes
            self.file_server.join()
            if self.idx_type == 'distributed':
                self.dht.server.join()
        except EOFError as e:
            print "\nShutting down peer."
        except:
            raise

def format_filesize(f_size):
    prefixes = ['', 'K', 'M', 'G', 'T', 'P']
    # change file size to human readable
    for prefix in prefixes:
        if f_size < 1024:
            break
        f_size = 1.*f_size/1024.
    f_size_str = "%.1f%sB" % (f_size, prefix)
    return f_size_str

def print_usage(args):
    print("Usage: python %s config.json" % args[0])
    sys.exit(1)

if __name__ == '__main__':
    # parse arguments
    args = sys.argv
    if len(args) != 2:
        print_usage(args)
    with open(args[1]) as config_fd:
        run_args = json.load(config_fd)
        
    peer = Peer(run_args)
    peer.run()
