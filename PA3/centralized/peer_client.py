class PeerClient(Process):
    def __init__(self, parent):
        super(PeerClient, self).__init__()

        self.parent = parent
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

        self.actions = {
            'exit': self._exit,
            'lookup': self._lookup,
            'search': self._search,
            'register': self._register,
            'list': self._ls,
            'help': lambda x: self._display_help(),
            'benchmark': self._benchmark
        }

    def _exit(self):
        self.parent.terminate.value = 1
        return True, None

    def _lookup(self, filename):
        _, available_peers = self._search(filename)
        file_obtained = False
        while not file_obtained and available_peers:
            peer = available_peers.pop(0)
            
            # Establish connection to the peer to obtain file
            conn_param = (peer['addr'], peer['port'])
            peer_sock = socket(AF_INET, SOCK_STREAM)
            try:
                peer_so.connect(conn_param)
            except timeout:
                # TODO: peer not reachable
                continue
            peer_exch = proto.MessageExchanger(peer_sock)
            peer_action = dict(type='obtain', name=filename)
            peer_exch.pkl_send(peer_action)
            filepath = os.path.join(self.download_dir, filename)
            peer_exch.file_recv(filepath, progress=False)
        return False, True

    def _search(self, filename, pprint=False):
        idx_action = dict(type='search', name=filename)
        # request indexing server
        self.idxserv_exch.pkl_send(idx_action)
        available_peers = self.idxserv_exch.pkl_recv()

        if pprint:
            if available_peers is None:
                pass
            elif available_peers == []:
                print("File unavailable in other peers.")
            else:
                print("File available at the following peers:")
            for p in available_peers:
                print("\t- %s:%d" % (p['addr'], p['port']))
        return False, available_peers

    def _register(self, filename):
        if not os.path.isfile(filename):
            print("Error: %s does not exist or is not a file." % filename)
            return True, False

        # Register to the indexing server
        idx_action = dict(
            type='register',
            name=os.path.basename(filename),
            size=os.path.getsize(filename),
            path=os.path.abspath(filename)
        )
        self.idxserv_exch.pkl_send(idx_action)

        # Register locally
        files_dict = self.files_dict
        files_dict[f_name] = (idx_action['name'], idx_action['size'], idx_action['path'])
        self.files_dict = files_dict
        return False, True

    def _ls(self, pprint=False):
        idx_action = dict(type='list')
        self.idxserv_exch.pkl_send(idx_action)
        available_files = self.idxserv_msg_exch.pkl_recv()
        if pprint:
            for f in available_files:
                print f
        return False, available_files

    def _display_help(self):
        help_ui = {
            'exit': 'Shut down this peer.',
            'lookup': 'Download a given file from an available peer.',
            'search': 'Return the list of other peers having a given file.',
            'register': 'Register a given file to the indexing server.',
            'list': 'List all the available files through the indexing server.',
            'help': 'Display the help screen.',
            'benchmark': 'Benchmark a function (lookup, search, or register) by running this command N times and averaging the runtime.'
        }
        keys = sorted(help_ui.keys())
        for k in keys:
            print("{:<20}{:<20}".format(k, help_ui[k]))
        return False, True

    def _benchmark(self, bench_cmd, nb_loops):
        # dictionary of possible actions to benchmark
        benchmark_actions = {
            'search': self._search,
            'lookup': self._lookup,
            'register': self._register
        }

        benchmark_files = []
        # get all available files in other peers in case of a lookup/search benchmark
        if bench_cmd in ['lookup', 'search']:
            file_list = self._ls()
            for f_name, f_size, f_path in file_list:
                _, available_peers = self._search(f_name)
                if available_peers:
                    benchmark_files.append(f_name)
        else: # get files to register by this node
            # TODO
            pass
        
        # build list of random files for lookup/search benchmark
        if not benchmark_files:
            print "There are no file available for this type of benchmark."
            return False, True
        query_files = sample_with_replacement(other_peers_file, nb_loops)

        # Start time and run loop of actions
        t0 = time.time()
        for i in range(nb_loops):
            # add file to cmd when benchmarking search/lookup
            _, results = benchmark_actions[bench_cmd](new_cmd_vec + [query_files[i]])
        t1 = time.time()
        delta = t1-t0
        avg_delta = delta*1000./nb_loops
        self.__block_print("Total time: %.2fs\nAverage time: %.2fms" % (delta, avg_delta))
        return False, True
    
    def run(self):
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
                    self.__block_print("Error: unvalid command '%s'" % cmd_str)
                    print("Use the help command to get more informations.")
                    #self.ui_actions['help'](None)
                # If valid command, execute the matching action
                else:
                    retval = self.ui_actions[cmd_action](cmd_vec)
            except KeyboardInterrupt as e:
                sys.stderr.write("\r\n")

