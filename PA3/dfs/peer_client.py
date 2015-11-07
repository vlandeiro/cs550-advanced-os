import glob
import logging
import random
import time
import os
import sys
import errno
import re
import numpy as np

from collections import Counter
from multiprocessing import Process
from socket import *
from CommunicationProtocol import MessageExchanger
from subprocess import call
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

        self.peers_sock = {}
        self.peers_check = {}
        self.check_timeout = 5

        self.actions = {
            'exit': self._exit,
            'lookup': self._lookup,
            'search': self._search,
            'register': self._register,
            'list': self._ls,
            'help': self._display_help,
            'ls': self._local_ls,
            'benchmark': self._benchmark
        }

    def _benchmark1(self, cmd):
        bench_map = {
            'register': self._register,
            'search': lambda x: self._search(x, pprint=False),
            'lookup': self._lookup
        }
        if cmd not in bench_map.keys():
            raise AttributeError("%s not supported for benchmark. Should be one of %s" % (cmd, bench_map.keys()))
        _, all_files = self._ls(pprint=False)
        if cmd in ['search', 'lookup']:
            files = [f for f in all_files if not re.match('f.*%s' % self.ip, f)]
            files = np.random.choice(files, 10000, replace=False)
        else:
            files = glob.glob('../data/local/exp1/*')
        results = []
        t0 = time.time()
        for f in files:
            _, ret = bench_map[cmd](f)
            results.append(True if ret else False)
        delta = time.time() - t0
        self.logger.info('Benchmark %s on %d files took %.3f seconds.', cmd, len(files), delta)
        return Counter(results)

    def _benchmark2(self, file_size):
        bench_map = ['1K', '10K', '100K', '1M', '10M', '100M', '1G']
        if file_size not in bench_map:
            raise AttributeError("%s is a size not supported in this benchmark: should be one of %s" % (file_size, bench_map))
        idx = bench_map.index(file_size)
        
        _, all_files = self._ls(pprint=False)
        files = [f for f in all_files if re.match('f%d.*' % idx, f)]
        local_files = [os.path.basename(f) for f in glob.glob('../data/local/exp2/*')]
        self.logger.info('all_files = %d, files = %d, local_files = %d', len(all_files), len(files), len(local_files))
        files = [f for f in files if f not in local_files]
        self.logger.info('final_files = %d', len(files))
        self.logger.info('%d files to lookup.' % len(files))
        results = []
        t0 = time.time()
        for f in files:
            _, ret = self._lookup(f)
            results.append(ret)
        delta = time.time() - t0
        self.logger.info('Benchmark to obtain %d files of size %s took %.3f seconds.', len(files), file_size, delta)
        return Counter(results)

    def _benchmark(self, exp, cmd):
        try:
            exp_num = int(exp)
            bench_meth = getattr(self, "_benchmark%d" % exp_num)
            results = bench_meth(cmd)
        except (AttributeError, ValueError) as e:
            self.logger.error(e)
            return False, False
        finally:
            call('rm -rf ../data/download/*', shell=True)
        return False, results


    def _exit(self):
        """
        Set the terminate shared variable to 1 to exit all the running processes.
        :return: True, None
        """
        self.parent.terminate.value = 1
        self.close_connection()
        return True, None

    def _get_peer_sock(self, peer_id):
        addr, port = peer_id.split(':')
        self.logger.debug(addr)
        self.logger.debug(self.peers_sock.keys())
        ret = False
        if addr not in self.peers_sock.keys() or self.peers_sock[addr] is None:
            # connection to peer
            try:
                port = int(port)
                conn_param = (addr, port)
                self.logger.debug('Connect to: %s', repr(conn_param))
                peer_sock = socket(AF_INET, SOCK_STREAM)
                self.peers_sock[addr] = peer_sock
                self.peers_check[addr] = time.time()
                peer_sock.connect(conn_param)
                ret = self.peers_sock[addr]
            except error as e: # peer unreachable
                if e.errno == errno.ECONNREFUSED:
                    self.peers_sock[addr] = False
                    self.peers_check[addr] = time.time()
                    ret = False
        elif self.peers_sock[addr] is False:
            # check if last status change was more than n seconds ago
            # try to reconnect if timeout has expired
            if time.time()-self.peers_check[addr] > self.parent.check_timeout:
                self.peers_sock[addr] = None
                ret = self._get_peer_sock(addr)
        else:
            ret = self.peers_sock[addr]
        return ret

    def _lookup(self, name):
        """
        Search for peers where a given file is stored and then request these peers for the file until the file
        is entirely stored locally.
        :param name: name of the file to obtain.
        :return: False, True if the file has been downloaded or False if not.
        """
        _, available_peers = self._search(name, pprint=False)
        file_obtained = False
        while not file_obtained and available_peers:
            peer_id = available_peers.pop(0)
            # Establish connection to the peer to obtain file
            peer_sock = self._get_peer_sock(peer_id)
            self.logger.debug('peer_sock in lookup: %s', repr(peer_sock))
            if peer_sock:
                peer_exch = MessageExchanger(peer_sock)
                peer_action = dict(type='obtain', name=name)
                peer_exch.pkl_send(peer_action)
                f_path = os.path.join(self.download_dir, name)
                file_exists = peer_exch.pkl_recv()
                if file_exists:
                    peer_exch.file_recv(f_path, show_progress=False)
                    file_obtained = True
        return False, file_obtained

    def _search(self, name, pprint=True):
        """
        Request all the peers where a given file is available.
        :param name: name of the file to search for.
        :param pprint: if True, then the peers are printed in a "pretty format".
        :return: False, list of peers where this file is stored.
        """
        available_peers = self.idx_server_proxy.search(self.id, name)

        if pprint:
            if available_peers is False:
                print("Other peers are offline.")
            elif available_peers == []:
                print("File unavailable in other peers.")
            elif available_peers is not None:
                print("File available at the following peers:")
                for p in available_peers:
                    print "\t- %s" % p
        return False, available_peers

    def _register(self, f_path, regex=False):
        """
        Register a given file to the indexing server.
        :param f_path: path to the file to register.
        :param regex: if not False, then f_path is processed as a regular expression.
        :return: False, True if replication has been done or False otherwise.
        """
        if regex != False:
            files = glob.glob(f_path)
            results = []
            for f in files:
                term, ret = self._register(f)
                results.append(ret)
            return False, Counter(results)

        if not os.path.isfile(f_path):
            self.logger.error("%s does not exist or is not a file." % f_path)
            return False, False

        # Register to the indexing server
        name = os.path.basename(f_path)
        replicate_to = self.idx_server_proxy.register(self.id, name)

        if replicate_to == False:
            self.logger.debug("Main node and replica are done for metadata.")
            return False, False

        # Register locally
        local_files = self.parent.local_files
        local_files[name] = os.path.abspath(f_path)
        self.parent.local_files = local_files

        # Replicate files
        if replicate_to:
            self.logger.debug("Replicate to %s", replicate_to)
            for peer_id in replicate_to:
                peer_sock = self._get_peer_sock(peer_id)
                if peer_sock:
                    peer_exch = MessageExchanger(peer_sock)
                    peer_action = dict(type='replicate', name=name)
                    peer_exch.pkl_send(peer_action)
                    peer_exch.file_send(f_path)

        return False, True

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
        Close the connection with the indexing server and the peers.
        :return: None
        """
        self.idx_server_proxy.close_connection(self.id)
        for peer_id, sock in self.peers_sock.iteritems():
            if sock:
                try:
                    exch = MessageExchanger(sock)
                    peer_action = dict(type='exit', id=peer_id)
                    exch.pkl_send(peer_action)
                    sock.shutdown(1)
                    sock.close()
                except error:
                    pass

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
                self.logger.error("Connection refused by the Indexing Server. Are you sure the Indexing Server is running?")
                sys.exit(1)

    def run(self):
        """
        Handle the user input and the connections to the indexing server and the other peers.
        :return: None
        """
        self.logger.info("Start the user interface.")

        # Start by connecting to the Indexing Server
        self._idx_server_connect()

        # Run the user interface
        terminate = False
        while not terminate:
            try:
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
            except EOFError:
                self._exit()
                break
