import glob
import logging
from multiprocessing import Process
import random
import time
import os
import sys
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
        self.idxserv_port = parent.idx_server_port
        self.ip = parent.this_ip
        port = parent.config['file_server_port']
        self.download_dir = parent.config['download_dir']
        self.max_connections = parent.config['max_connections']
        self.parent = parent
        self.logger = logging.getLogger(self.__class__.__name__)
        level = logging.getLevelName(parent.config.get('log', 'INFO'))
        self.logger.setLevel(level)

        self.id = ":".join([self.ip, str(port)])
        self.idxserv_sock = None
        self.idxserv_proxy = None

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
        self.parent.terminate.value = 1
        return True, None

    def _lookup(self, filename):
        _, available_peers = self._search(filename)
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
                peer_action = dict(type='obtain', name=filename)
                peer_exch.pkl_send(peer_action)
                filepath = os.path.join(self.download_dir, filename)
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
        available_peers = self.idxserv_proxy.search(self.id, name)

        if pprint:
            if available_peers is None:
                pass
            elif available_peers == []:
                print("File unavailable in other peers.")
            else:
                print("File available at the following peers:")
                for p in available_peers:
                    print "\t- %s" % p
        return False, available_peers

    def _register(self, filename):
        if not os.path.isfile(filename):
            print("Error: %s does not exist or is not a file." % filename)
            return False, False

        # Register to the indexing server
        name = os.path.basename(filename)
        replicate_to = self.idxserv_proxy.register(self.id, name)

        # Register locally
        local_files = self.parent.local_files
        local_files[name] = os.path.abspath(filename)
        self.parent.local_files = local_files

        # Replicate files
        self.logger.debug("Replicate to %s", replicate_to)
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
                peer_exch.file_send(filename)
            except timeout:  # peer unreachable
                continue

        return False, True

    def _local_ls(self, regex="./*"):
        ls = glob.glob(regex)
        sizes = [os.path.getsize(f) for f in ls]
        for name, size in zip(ls, sizes):
            print("%40s - %40d" % (name, size))
        return False, None

    def _ls(self, pprint=True):
        available_files = self.idxserv_proxy.list()
        if pprint != False:
            for f in available_files:
                print f
        return False, available_files

    def _display_help(self):
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
        :return:
        """
        self.idxserv_proxy.init_connection(self.id)
        return False, None

    def close_connection(self):
        self.idxserv_proxy.close_connection(self.id)

    def do(self, action, args):
        if action not in self.actions.keys():
            print "Error: unvalid command '%s'" % " ".join([action] + args)
            print "Use the help command to get more informations."
        else:
            try:
                return self.actions[action](*args)
            except TypeError as e:
                self.logger.error(e.message)
                raise e
        return False, False

    def run(self):
        """This function handles the user input and the connections to the
        indexing server and the other peers.
        """
        # Start by connecting to the Indexing Server
        try:
            if self.parent.idx_type == 'centralized':
                self.idxserv_sock = socket(AF_INET, SOCK_STREAM)
                self.idxserv_sock.connect((self.idxserv_ip, self.idxserv_port))
                self.idxserv_proxy = CentralizedISProxy(self.idxserv_sock)
            else:
                self.idxserv_proxy = DistributedISProxy(self.parent.dht)
            self._init_connection()
        except error as e:
            if e.errno == errno.ECONNREFUSED:
                self.logger.error("Connection refused by the Indexing Server. Are you sure the Indexing Server is running?")
                sys.exit(1)

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
            self.idxserv_proxy.close_connection(self.id)
