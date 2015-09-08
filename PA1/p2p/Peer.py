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
import CommunicationProtocol as proto
import glob
import textwrap

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
    
        self.listening_socket = None
        self.idxserv_socket = None
        self.idxserv_msg_exch = None
        self.peer_socket = None
        self.files_dict = None
        
        self.actions = {
            'exit': lambda x: self.__quit_ui(),
            'help': lambda x: self.__display_help_ui(),
            'lookup': self.__action_lookup,
            'register': self.__action_register,
            'list': self.__action_list,
            'getid': self.__action_getid,
            'echo': self.__action_echo,
        }

    def __IS_print(self, msg):
        arg = msg if type(msg) == str else repr(msg)
        print("IS> %s" % arg)

    def __block_print(self, msg, col_width=80):
        print textwrap.fill(textwrap.dedent(msg).strip(), width=80)
    
    def __action_lookup(self, cmd_vec):
        if len(cmd_vec) != 2:
            err_lookup = """
            Error: lookup command needs exactly one argument: the name of the
            file to lookup.
            """
            self.__block_print(err_lookup)
        else:
            msg = " ".join(cmd_vec)
            ack = self.idxserv_msg_exch.send(msg, ack=True)
            if not ack:
                logger.error("Error in communication with indexing server")
                return False
            else:
                peers_with_file = self.idxserv_msg_exch.pkl_recv()
                self.__IS_print(peers_with_file)

                if not peers_with_file:
                    print("This file is not available in the registered peers")
                else:
                    l_str = "" if len(peers_with_file) == 1 else "-%d" % len(peers_with_file) 
                    print("Select amongst these peers [1%s]" % l_str)
                    for i, peer in enumerate(peers_with_file):
                        print("{:<4}{:<50}".format("[%d]" % i, peer))
                    user_choice = raw_input()
                    print("TODO: Implement user choice + connection to the matching peer + file transfer")

    def __action_register(self, cmd_vec):
        if len(cmd_vec) != 2:
            err_register = """
            Error: register command needs exactly one argument: the regular
            expression to identify all the files to register (e.g db/*.txt for
            all the files with a txt extension in the db folder).
            """
            self.__block_print(err_register)
        else:
            file_regex = cmd_vec[1]
            # send information about every file sequentially
            files_to_send = [f for f in glob.glob(file_regex) if os.path.isfile(f)]

            # exit if the list of files to send is empty
            if not files_to_send:
                err_register_2 = """
                Error: the regular expression does not match with any file.
                """
                self.__block_print(err_register_2)
                return False
            
            ack = self.idxserv_msg_exch.send('register', ack=True)
            if not ack:
                logger.error("Error in communication with indexing server.")
                return False

            for f_name in files_to_send:
                stats = os.stat(f_name)
                f_size = stats.st_size
                f_path = os.path.abspath(f_name)
                f_tuple = (f_name, f_size, f_path)
                self.idxserv_msg_exch.pkl_send(f_tuple)
                self.files_dict[f_name] = f_tuple
            
            poison_pill = None
                
                
            
    def __action_list(self, cmd_vec):
        ack = self.idxserv_msg_exch.send('list', ack=True)
        if not ack:
            logger.error("Error in communication with indexing server.")
            return False

        file_list = self.idxserv_msg_exch.pkl_recv()
        self.__IS_print(file_list)

    def __action_getid(self, cmd_vec=None):
        print(id(self))

    def __action_echo(self, cmd_vec):
        if len(cmd_vec) == 1:
            err_echo = """
            Error: echo command needs at least one argument.
            """
            self.__block_print(err_echo)
        else:
            msg = " ".join(cmd_vec)
            ack = self.idxserv_msg_exch.send(msg, ack=True)
            if not ack:
                logger.error("Error in communication with indexing server")
                return False
            else:
                response = self.idxserv_msg_exch.recv()
                self.__IS_print(response)

    def __display_help_ui(self):
        help_ui = {
            'exit': 'Shut down this peer.',
            'lookup': 'Ask the indexing server for the lists of other peers that have a given file.',
            'register': 'Register to the indexing server.',
            'list': 'List all the available files in the indexing server.',
            'help': 'Display the help screen.',
            'getid': 'Return the peer id.'
        }
        keys = sorted(help_ui.keys())
        for k in keys:
            print("{:<15}{:<20}".format(k, help_ui[k]))

    def __quit_ui(self):
        if self.idxserv_socket is not None:
            self.idxserv_msg_exch.send("close_connection", ack=True)
            self.idxserv_socket.close()
        sys.exit(0)
        
    def __run_server(self):
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

    def run_ui(self):
        """This function handles the user input and the connections to the
        indexing server and the other peers.

        """
        # Start by connecting to the Indexing Server
        try:
            self.idxserv_socket = socket(AF_INET, SOCK_STREAM)
            self.idxserv_socket.connect((self.idxserv_ip, self.idxserv_port))
            self.idxserv_msg_exch = proto.MessageExchanger(self.idxserv_socket)
        except error as e:
            if e.errno == errno.ECONNREFUSED:
                logger.error("Connection refused by the Indexing Server. Are you sure the Indexing Server is running?\n")
                sys.exit(1)
        
        while True:
            sys.stdout.write("$> ")
            sys.stdout.flush()
            
            # Getting user input
            try:
                cmd_str = raw_input()
            except KeyboardInterrupt as e:
                self.__quit_ui()
            except EOFError as e:  
                self.__quit_ui()
            cmd_vec = cmd_str.split()

            # Parsing user command
            cmd_action = cmd_vec[0] if len(cmd_vec) >= 1 else ''
            # If invalid command, print error message to user
            if cmd_action not in self.actions.keys():
                self.__block_print("Error: unvalid command.")
                self.actions['help'](None)
            # If valid command, execute the matching action
            else:
                self.actions[cmd_action](cmd_vec)

    def run(self):
        """Function that launches the different parts (server, user interface,
        client, file management, ...) of the peer.

        """
        # Start by running the server in a dedicated thread.
        logger.debug("Starting the peer server.")
        server = Process(target=self.__run_server)
        server.daemon = True
        server.start()
        logger.debug("Peer server running.")

        # Now run the user interface
        logger.debug("Starting the user interface.")
        self.__run_ui()
        
if __name__ == '__main__':
    args = docopt(__doc__)
    with open(args['<config_file>']) as config_fd:
        run_args = json.load(config_fd)
    
    peer = Peer(**run_args)
    peer.run_ui()
    
