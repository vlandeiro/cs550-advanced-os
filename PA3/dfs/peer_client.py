import glob
import logging
import random
import time
import os
import sys

from multiprocessing import Process
from socket import *
from CommunicationProtocol import MessageExchanger
from indexing_server_proxy import CentralizedISProxy, DistributedISProxy

logging.basicConfig(level=logging.DEBUG)


def sample_with_replacement(l, k):
    if l:
        lt = l * k
        return random.sample(lt, k)
    else:
        return []


class PeerClient(Process):
    def __init__(self, parent):
        """
            Initialize the user interface using the configuration stored in its parent.
            :param parent: Node object.
            :return: None
            """
        super(PeerClient, self).__init__()
        self.parent = parent
        self.idx_server_port = parent.idx_server_port
        self.ip = parent.ip
        self.download_dir = parent.download_dir
        self.max_connections = parent.max_connections
        self.id = ":".join([self.ip, str(parent.file_server_port)])
        self.logger = logging.getLogger(self.__class__.__name__)
        level = logging.getLevelName(parent.log_level)
        self.logger.setLevel(level)
        self.idx_server_sock = None
        self.idx_server_proxy = None
        self.actions = {
            'exit': self._exit,
            'lookup': self._lookup,
            'search': self._search,
            'register': self._register,
            'list': self._ls,
            'help': self._display_help,
            'ls': self._local_ls
        }

    def _exit(self):
        """
        Set the terminate shared variable to 1 to exit all the running processes.
        :return: True, None
        """
        self.parent.terminate.value = 1
        return True, None

    def _lookup(self, name):
        """
        Search for peers where a given file is stored and then request these peers for the file until the file
        is entirely stored locally.
        :param name: name of the file to obtain.
        :return: False, False
        """
        _, available_peers = self._search(name)
        file_obtained = False
        while not file_obtained and available_peers:
            peer = available_peers.pop(0)
            self.logger.debug(peer)
            # Establish connection to the peer to obtain file
            addr, port = peer.split(':')
            port = int(port)
            conn_param = (addr, port)
            peer_sock = socket(AF_INET, SOCK_STREAM)
            try:
                peer_sock.connect(conn_param)
                peer_exch = MessageExchanger(peer_sock)
                peer_action = dict(type='obtain', name=name)
                peer_exch.pkl_send(peer_action)
                filepath = os.path.join(self.download_dir, name)
                file_exists = peer_exch.pkl_recv()
                if file_exists:
                    peer_exch.file_recv(filepath, show_progress=False)
                    return False, True
                else:
                    return False, False
            except timeout:
                # peer not reachable
                continue
            finally:
                peer_sock.close()
        return False, False

    def _search(self, name, pprint=True):
        """
        Request all the peers where a given file is available.
        :param name: name of the file to search for.
        :param pprint: if True, then the peers are printed in a "pretty format".
        :return: False, list of peers where this file is stored.
        """
        available_peers = self.idx_server_proxy.search(self.id, name)

        if pprint:
            if available_peers == []:
                print("File unavailable in other peers.")
            elif available_peers is not None:
                print("File available at the following peers:")
                for p in available_peers:
                    print "\t- %s" % p
        return False, available_peers

    def _register(self, f_path):
        """
        Register a given file to the indexing server.
        :param f_path: path to the file to register.
        :return: False, True if replication has been done or False otherwise.
        """
        if not os.path.isfile(f_path):
            print("Error: %s does not exist or is not a file." % f_path)
            return False, False

        # Register to the indexing server
        name = os.path.basename(f_path)
        replicate_to = self.idx_server_proxy.register(self.id, name)

        # Register locally
        local_files = self.parent.local_files
        local_files[name] = os.path.abspath(f_path)
        self.parent.local_files = local_files

        # Replicate files
        self.logger.debug("Replicate to %s", replicate_to)
        if replicate_to:
            for peer in replicate_to:
                addr, port = peer.split(':')
                port = int(port)
                conn_param = (addr, port)
                peer_sock = socket(AF_INET, SOCK_STREAM)
                try:
                    peer_sock.connect(conn_param)
                    peer_exch = MessageExchanger(peer_sock)
                    peer_action = dict(type='replicate', name=name)
                    peer_exch.pkl_send(peer_action)
                    peer_exch.file_send(f_path)
                except timeout:  # peer unreachable
                    self.logger.debug('Cannot replicate, timeout reached.')
                    continue

        ret = True if replicate_to else False
        return False, ret

    def _local_ls(self, regex="./*"):
        """
        Print a list of local files matched by a given regular expression.
        :param regex: regular expression to match files.
        :return: False, None
        """
        ls = glob.glob(regex)
        sizes = [os.path.getsize(f) for f in ls]
        for name, size in zip(ls, sizes):
            print("%40s - %40d" % (name, size))
        return False, None

    def _ls(self, pprint=True):
        """
        List all the files available in the indexing server.
        :param pprint: if True, print one file name per line.
        :return: False, list of all available files.
        """
        available_files = self.idx_server_proxy.list()
        if pprint != False:
            for f in available_files:
                print f
        return False, available_files

    def _display_help(self):
        """
        Show the commands available to the user.
        :return: False, True
        """
        help_ui = {
            'exit': 'Shut down this peer.',
            'lookup': 'Download a given file from an available peer.',
            'search': 'Return the list of other peers having a given file.',
            'register': 'Register a given file to the indexing server.',
            'ls': 'Local listing of files',
            'list': 'List all the available files through the indexing server.',
            'help': 'Display the help screen.',
        }
        keys = sorted(help_ui.keys())
        for k in keys:
            print("{:<20}{:<20}".format(k, help_ui[k]))
        return False, True

    def _init_connection(self):
        """
        Initialize the connection with the indexing server.
        :return: False, None
        """
        self.idx_server_proxy.init_connection(self.id)
        return False, None

    def close_connection(self):
        """
        Close the connection with the indexing server.
        :return: None
        """
        self.idx_server_proxy.close_connection(self.id)

    def do(self, action, args):
        """
        Generic function that parse a given action and call the corresponding methods with the given arguments.
        :param action: action to execute.
        :param args: arguments to pass to the method associated with the action.
        :return: result of the action called.
        """
        if action not in self.actions.keys():
            print "Error: unvalid command '%s'" % " ".join([action] + args)
            print "Use the help command to get more informations."
        else:
            try:
                return self.actions[action](*args)
            except TypeError as e:
                self.logger.error(e.message)
                raise
        return False, False

    def _idx_server_connect(self):
        """
        Connect to the indexing server and set up the indexing server proxy.
        :return: None
        """
        try:
            if self.parent.idx_type == 'centralized':
                self.idx_server_sock = socket(AF_INET, SOCK_STREAM)
                self.idx_server_sock.connect((self.parent.idx_server_ip, self.idx_server_port))
                self.idx_server_proxy = CentralizedISProxy(self.idx_server_sock)
            else:
                self.idx_server_proxy = DistributedISProxy(self.parent.dht)
            self._init_connection()
        except error as e:
            if e.errno == errno.ECONNREFUSED:
                self.logger.error(
                    "Connection refused by the Indexing Server. Are you sure the Indexing Server is running?")
                sys.exit(1)

    def run(self):
        """
        Handle the user input and the connections to the indexing server and the other peers.
        :return: None
        """

        # Start by connecting to the Indexing Server
        self._idx_server_connect()

        # Run the user interface
        terminate = False
        try:
            while not terminate:
                sys.stdout.write("$> ")
                sys.stdout.flush()

                # Getting user input
                cmd_str = raw_input()
                cmd_vec = cmd_str.split()

                # Parsing user command
                action = cmd_vec[0] if len(cmd_vec) >= 1 else ''
                args = cmd_vec[1:]

                terminate, res = self.do(action, args)
                print res
        except KeyboardInterrupt as e:
            sys.stderr.write("\r\n")
        finally:
            self.parent.terminate.value = 1
            self.close_connection()
            self.idx_server_proxy.close_connection(self.id)
