"""
Usage:
    python Peer.py <config.json>
 """
from multiprocessing import Process, Manager, Queue
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

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
LISTENING_TIMEOUT = 0.2


class Peer:
    def __init__(self, listening_ip, listening_port, idxserv_ip, idxserv_port,
                 pool_size=10, files_regex="./*", download_dir="./"):
        self.listening_ip = listening_ip
        self.listening_port = listening_port
        
        self.idxserv_ip = idxserv_ip
        self.idxserv_port = idxserv_port
        self.pool_size = pool_size
        self.files_regex = files_regex
        self.download_dir = os.path.abspath(download_dir)
        if not os.path.isdir(self.download_dir):
            raise ValueError("Download directory does not exist")

        
        self.listening_socket = None
        self.idxserv_socket = None
        self.idxserv_msg_exch = None
        self.peer_socket = None

        self.manager = Manager()
        self.files_dict = self.manager.dict()
        self.server_running = self.manager.Value('i', 0)
        
        # TODO (optional): create checksum of each file using hashlib
        # to identify it instead of identifying it by its name.
        
        self.ui_actions = {
            'exit': self.__ui_action_exit,
            'help': lambda x: self.__ui_display_help(),
            'lookup': self.__ui_action_lookup,
            'register': self.__ui_action_register,
            'list': self.__ui_action_list,
            'getid': self.__ui_action_getid,
            'echo': self.__ui_action_echo,
            'search': self.__ui_action_search,
            'benchmark_search': self.__ui_action_benchmark_search
        }

        self.client_actions = {
            'obtain': self.__client_action_obtain
        }
        
    def __IS_print(self, msg):
        arg = msg if type(msg) == str else repr(msg)
        print("IS> %s" % arg)

    def __block_print(self, msg, col_width=80):
        print textwrap.fill(textwrap.dedent(msg).strip(), width=80)

    def __client_action_obtain(self, msg_exch, cmd_vec):
        msg_exch.recv() # dummy
        f_req = cmd_vec[1]
        files_dict = self.files_dict
        logger.debug("FILES DICT KEYS: " + str(files_dict.keys()))
        if f_req in files_dict:
            f_name, f_size, f_path = files_dict[f_req]
            ack = msg_exch.pkl_send(f_size, ack=True)
            if ack:
                logger.debug("ACK: " + str(ack))
                msg_exch.file_send(f_path)
            else:
                logger.debug("ACK not ok")
        return False
    
    def __init_connection(self):
        self.idxserv_msg_exch.send("init %d addr %s" % (id(self), self.listening_ip), ack=True)
        self.idxserv_msg_exch.send("init %d port %d" % (id(self), self.listening_port), ack=True)
        return True

    def __ui_action_exit(self, cmd_vec):
        time.sleep(2*LISTENING_TIMEOUT)
        self.__quit_ui()
        return False

    def __search_file(self, cmd_vec):
        if len(cmd_vec) != 2:
            err_lookup = """
            Error: %s command needs exactly one argument: the name of the
            file to lookup.
            """ % cmd_vec[0]
            self.__block_print(err_lookup)
        else:
            cmd_vec[0] = "lookup"
            msg = " ".join(cmd_vec)
            f_name = cmd_vec[1]
            # send lookup message to indexing server
            ack = self.idxserv_msg_exch.send("%s %d" % (msg, id(self)), ack=True)
            if not ack:
                logger.error("Error in communication with indexing server")
                return None
            else:
                self.idxserv_msg_exch.send_dummy()
                peers_with_file = self.idxserv_msg_exch.pkl_recv()
        return peers_with_file
    
    def __ui_action_search(self, cmd_vec):
        peers_with_file = self.__search_file(cmd_vec)
        self.__IS_print(peers_with_file)
        return True
    
    def __ui_action_benchmark_search(self, cmd_vec):
        if len(cmd_vec) < 2:
            err_print = """
            The benchmark command is missing arguments. You must
            indicate the number of loops you want to run.
            """
            self.__block_print(err_print)
            return True
        nb_loops = int(cmd_vec[1])
        t0 = time.time()
        for i in range(nb_loops):
            peers_with_file = self.__search_file(cmd_vec)
            if peers_with_file is None:
                logger.info('Request %d failed.' % i)
        t1 = time.time()
        delta = t1-t0
        avg_delta = delta*1000./nb_loops
        self.__block_print("Total time: %.2fs\nAverage time: %.2fms" % (delta, avg_delta))
        return True
    
    def __ui_action_lookup(self, cmd_vec):
        f_name = cmd_vec[1]
        peers_with_file = self.__search_file(cmd_vec)
        if peers_with_file is None:
            resp_print == """
            Search request failed.
            """
            self.__block_print(resp_print)
            return True
        elif not peers_with_file:
            resp_print = """
            This file is not available in the other registered peers.                    
            """
            self.__block_print(resp_print)
            return True
        else:
            l_str = "" if len(peers_with_file) == 1 else "-%d" % len(peers_with_file) 
            print("Select amongst these peers [1%s]" % l_str)
            for i, peer in enumerate(peers_with_file):
                num = i+1
                peer_str = "%s:%s" % (peer['addr'], peer['port'])
                print("{:<4}{:<50}".format("[%d]" % num, peer_str))
            while True:
                sys.stdout.write("Choice: ")
                raw_inp = raw_input()
                user_choice = int(raw_inp)
                if user_choice > len(peers_with_file) or user_choice < 1:
                    self.__block_print("Error in your choice. The value you entered is either invalid.")
                else:
                    break
            actual_idx = user_choice - 1
            choosen_peer = peers_with_file[actual_idx]

            # Establish connection to the peer to obtain file
            conn_param = (choosen_peer['addr'], choosen_peer['port'])
            fs_peer_so = socket(AF_INET, SOCK_STREAM)
            fs_peer_so.connect(conn_param)
            fs_msg_exch = proto.MessageExchanger(fs_peer_so)
            
            ack = fs_msg_exch.send("obtain %s" % f_name, ack=True)
            if not ack:
                logger.error("Problem when sending message to peer.")
                return True
            fs_msg_exch.send_dummy()
            f_size = fs_msg_exch.pkl_recv()
            f_size_str = format_filesize(f_size)

            sys.stdout.write("Download %s of size %s? [Y/n] " % (f_name, f_size_str))
            raw_inp = raw_input()
            user_choice = False if raw_inp.lower() == 'n' else True
            if user_choice:
                fs_msg_exch.send_ack()
                f_fullpath = os.path.join(self.download_dir, f_name)
                fs_msg_exch.file_recv(f_fullpath, f_size)
                print("File received and stored locally.")
            else:
                fs_msg_exch.send_err()
                print("Abort file transfer.")
        return True
    
    def __ui_action_register(self, cmd_vec):
        # send information about every file sequentially
        files_to_send = [f for f in glob.glob(self.files_regex) if os.path.isfile(f)]
        
        # exit if the list of files to send is empty
        if not files_to_send:
            err_register_2 = """
            Error: the regular expression does not match with any file to register.
            """
            self.__block_print(err_register_2)
            return True
            
        ack = self.idxserv_msg_exch.send('register %d' % (id(self)), ack=True)
        if not ack:
            logger.error("Error in communication with indexing server.")
            return True

        logger.debug(files_to_send)
        files_dict = self.files_dict
        for f in files_to_send:
            f_name = os.path.basename(f)
            stats = os.stat(f)
            f_size = stats.st_size
            f_path = os.path.abspath(f)
            f_tuple = (f_name, f_size, f_path)
            self.idxserv_msg_exch.pkl_send(f_tuple, ack=True)
            files_dict[f_name] = f_tuple
        self.files_dict = files_dict
            
        poison_pill = None
        self.idxserv_msg_exch.pkl_send(poison_pill)
        return True
        
    def __ui_action_list(self, cmd_vec):
        ack = self.idxserv_msg_exch.send('list', ack=True)
        if not ack:
            logger.error("Error in communication with indexing server.")
            return True
        self.idxserv_msg_exch.send_dummy()
        file_list = self.idxserv_msg_exch.pkl_recv()
        if not file_list:
            print("There is no file available on the Indexing Server.")
        else:
            print("{:<30}{:<10}{:<40}".format("Filename", "Size", "Path"))
            print("-"*80)
            for f_name, f_size, f_path in file_list:
                f_size_str = format_filesize(f_size)
                # reduce absolute path
                if len(f_path) > 40:
                    f_path = f_path[:15] + " ... " + f_path[-15:]
                    print("{:<30}{:<10}{:<40}".format(f_name, f_size_str, f_path))
        return True

    def __ui_action_getid(self, cmd_vec=None):
        print(id(self))
        return True

    def __ui_action_echo(self, cmd_vec):
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
                return True
            else:
                self.idxserv_msg_exch.send_dummy()
                response = self.idxserv_msg_exch.recv()
                self.__IS_print(response)
        return True
    
    def __ui_display_help(self):
        help_ui = {
            'exit': 'Shut down this peer.',
            'lookup': 'Request the indexing server for the lists of other peers that have a given file and give the choice to the user to download the file from a peer.',
            'search': 'Only request the indexing server for the lists of other peers having that file',
            'register': 'Register files to the indexing server.',
            'list': 'List all the available files in the indexing server.',
            'help': 'Display the help screen.',
            'getid': 'Return the peer id.',
            'echo': 'Simple function that send a message to the server, wait for the same message and print it.',
            'benchmark_search': 'Run a given number of sequential search and print the average time for one search request.'
        }
        keys = sorted(help_ui.keys())
        for k in keys:
            print("{:<15}{:<20}".format(k, help_ui[k]))
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
        self.listening_socket.bind(("localhost", self.listening_port))
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

    def __run_ui(self):
        """This function handles the user input and the connections to the
        indexing server and the other peers.
        """
        self.ui_running = False
        # Start by connecting to the Indexing Server
        try:
            self.idxserv_socket = socket(AF_INET, SOCK_STREAM)
            self.idxserv_socket.connect((self.idxserv_ip, self.idxserv_port))
            self.idxserv_msg_exch = proto.MessageExchanger(self.idxserv_socket)
            self.__init_connection()
        except error as e:
            if e.errno == errno.ECONNREFUSED:
                logger.error("Connection refused by the Indexing Server. Are you sure the Indexing Server is running?")
                sys.exit(1)

        self.ui_running = True
        retval = True
        while retval:
            sys.stdout.write("$> ")
            sys.stdout.flush()

            try:
                # Getting user input
                cmd_str = raw_input()
                cmd_vec = cmd_str.split()
                
                # Parsing user command
                cmd_action = cmd_vec[0] if len(cmd_vec) >= 1 else ''
                # If invalid command, print error message to user
                if cmd_action not in self.ui_actions.keys():
                    self.__block_print("Error: unvalid command.")
                    self.ui_actions['help'](None)
                # If valid command, execute the matching action
                else:
                    retval = self.ui_actions[cmd_action](cmd_vec)
            except KeyboardInterrupt as e:
                sys.stderr.write("\r\n")
                pass

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
            raise
            self.__quit_ui()

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

if __name__ == '__main__':
    # parse arguments
    args = sys.argv
    if len(args) != 2:
        usage_error()
    with open(args[1]) as config_fd:
        run_args = json.load(config_fd)
        
    peer = Peer(**run_args)
    peer.run()
