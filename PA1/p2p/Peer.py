"""
Usage:
    Peer.py run <config_file> [--max-conn=M]
 
 Options:
     --help -h       Display this screen.
     --max-conn=M    Maximum number of allowed connections to the server [default: 10].
 """
from docopt import docopt
from multiprocessing import Process, Manager
from socket import *

import logging
import json
import sys
import errno

BUFFER_SIZE = 4096
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class Peer:
    def __init__(self, listening_ip, listening_port,
                 idxserv_ip, idxserv_port,
                 max_connect=10):
        self.listening_ip = listening_ip
        self.listening_port = listening_port
        self.idxserv_ip = idxserv_ip
        self.idxserv_port = idxserv_port
        self.max_connect = max_connect
    
        self.manager = Manager()
        self.listening_socket = None
        self.idxserv_socket = None
        self.peer_socket = None

    def run_server(self):
        """This function handles the connection from other peer to obtain
        files.
        
        """
        self.listening_socket = socket(AF_INET, SOCK_STREAM)
        self.listening_socket.bind((self.listening_ip, self.listening_port))
        self.listening_socket.listen(self.max_connect)
        logger.debug("Peer server listening on port %d", self.port)

        while True:
            peer_so, peer_addr = self.listening_socket.accept()
            handler = Process(target=self.peer_message_handler, args=(peer_so, peer_addr))
            handler.daemon = True
            handler.start()
        self.listening_socket.close()

    def display_help(self, help_dict):
        keys = sorted(help_dict.keys())
        for k in keys:
            sys.stdout.write("{:<15}{:<20}\n".format(k, help_dict[k]))

    def quit_ui(self):
        if self.idxserv_socket is not None:
            self.idxserv_socket.close()
        sys.exit(0)
        
    def run_ui(self):
        """This function handles the user input and the connections to the
        indexing server and the other peers.

        """
        # Start by connecting to the Indexing Server
        try:
            self.idxserv_socket = socket(AF_INET, SOCK_STREAM)
            self.idxserv_socket.connect((self.idxserv_ip, self.idxserv_port))
        except error as e:
            if e.errno == errno.ECONNREFUSED:
                sys.stderr.write("Connection refused by the Indexing Server. Are you sure the Indexing Server is running?\n")
                sys.exit(1)
        
        valid_ui_commands = ['echo', 'exit', 'lookup', 'register', 'catalog', 'help', 'getid']
        help_ui = {
            'exit': 'Shut down this peer.',
            'lookup': 'Ask the indexing server for the lists of other peers that have a given file.',
            'register': 'Register to the indexing server.',
            'catalog': 'List all the available files in the indexing server.',
            'help': 'Display the help screen.',
            'getid': 'Return the peer id.'}

        while True:
            sys.stdout.write("$> ")
            try:
                cmd_str = raw_input()
            except EOFError as e:
                self.quit_ui()
            cmd_vec = cmd_str.split()
            if cmd_vec[0] not in valid_ui_commands:
                print "- Error: unvalid command."
                self.display_help(help_ui)
            elif cmd_vec[0] == 'exit':
                self.quit_ui()
            elif cmd_vec[0] == 'help':
                self.display_help(help_ui)
            elif cmd_vec[0] == 'lookup':
                print("TODO: implement lookup")
            elif cmd_vec[0] == 'register':
                print("TODO: implement register")
            elif cmd_vec[0] == 'catalog':
                print("TODO: implement catalog")
            elif cmd_vec[0] == 'getid':
                print(id(self))
            elif cmd_vec[0] == 'echo':
                if len(cmd_vec) == 1:
                    print("- Error: echo command need at least one argument.")
                else:
                    msg = " ".join(cmd_vec[1:])
                    self.idxserv_socket.send(msg)
            
        
    def run(self):
        """Function that launches the different parts (server, user interface,
        client, file management, ...) of the peer.

        """
        # Start by running the server in a dedicated thread.
        logger.debug("Starting the peer server.")
        server = Process(target=self.run_server)
        server.daemon = True
        server.start()
        logger.debug("Peer server running.")

        # Now run the user interface
        logger.debug("Starting the user interface.")

if __name__ == '__main__':
    args = docopt(__doc__)
    with open(args['<config_file>']) as config_fd:
        run_args = json.load(config_fd)
    
    peer = Peer(**run_args)
    peer.run_ui()
    
