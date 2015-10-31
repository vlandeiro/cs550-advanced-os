from multiprocessing import Manager, Value

import logging
import json
import sys
import os
from peer_client import PeerClientUI
from peer_server import PeerServer

logging.basicConfig(level=logging.DEBUG)
    
class Peer:
    def __init__(self, config):
        self.config = config
        self.download_dir = os.path.abspath(config['download_dir'])
        if not os.path.isdir(self.download_dir):
            raise ValueError("Download directory does not exist")

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)

        self.manager = Manager()
        self.local_files = self.manager.dict()
        self.terminate = Value('i', 0)

        self.client = PeerClientUI(self)
        self.server = PeerServer(self)

    def run(self):
        """Function that launches the different parts (server, user interface,
        client, file management, ...) of the peer.

        """
        try:
            # First, start the server in a dedicated thread.
            self.server.start()
            # Then, start the user interface
            self.logger.debug("Starting the user interface.")
            self.client.run()
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
