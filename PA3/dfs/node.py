from multiprocessing import Manager, Value

import time
import logging
import json
import sys
import os

from urllib2 import urlopen
from distributed_indexing_server import DHT
from peer_client import PeerClient
from peer_server import PeerServer

logging.basicConfig(level=logging.DEBUG)


class Node:
    def __init__(self, config):
        """
        Initialize a node given parameters.
        :param config: paramaters to set up the node given as a python dictionary.
        :return: None
        """
        # read and parse configuration
        self.config = config
        self.idx_type = config['idx_type']  # centralized or distributed

        # configuration parameters shared by both implementations
        self.download_dir = os.path.abspath(config['download_dir'])
        if not os.path.isdir(self.download_dir):
            raise ValueError("Download directory does not exist")
        self.idx_server_port = config['idx_server_port']
        self.file_server_port = config['file_server_port']
        self.log_level = config['log_level']
        self.timeout_value = config['timeout_value']
        self.max_connections = config['max_connections']
        self.ip = str(json.load(urlopen('https://api.ipify.org/?format=json'))['ip'])

        # set up logger
        self.logger = logging.getLogger(self.__class__.__name__)
        level = logging.getLevelName(self.log_level)
        self.logger.setLevel(level)

        # create shared dictionary to store the local paths to the registered files
        self.manager = Manager()
        self.local_files = self.manager.dict()
        self.sock_status = self.manager.dict()

        # configuration when the indexing server is distributed
        if self.idx_type == 'distributed':
            self.nodes_list = config['nodes']
            self.nodes_count = len(self.nodes_list)
            self.replica = config['replica']
            # get this server info from config file
            if self.ip not in self.nodes_list:
                raise ValueError("peer %s is not included in the config file." % self.ip)
            self.id = self.nodes_list.index(self.ip)
        else:
            self.idx_server_ip = config['idx_server_ip']

        self.terminate = Value('i', 0)

        # create main processes
        self.client = PeerClient(self)
        self.file_server = PeerServer(self)
        if self.idx_type == 'distributed':
            self.dht = DHT(config, self.terminate)

    def run(self):
        """
        Launch the different pieces of this node: distributed indexing server if requested, file server, and user
        interface.
        :return: None
        """
        try:
            # First, start the file server in a dedicated thread.
            self.file_server.start()
            # After that, run the indexing server if the config is set to distributed
            if self.idx_type == 'distributed':
                self.dht.server.start()

            # Then, start the user interface
            time.sleep(1)
            self.client.run()
            # Join the background processes
            self.file_server.join()
            if self.idx_type == 'distributed':
                self.dht.server.join()
        except EOFError as e:
            print "\nShutting down peer."
        except:
            raise


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

    peer = Node(run_args)
    peer.run()
