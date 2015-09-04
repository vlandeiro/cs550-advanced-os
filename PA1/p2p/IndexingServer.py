"""
Usage:
   IndexingServer.py run <ip> <port> [--max-conn=M]

Options:
    --help -h       Display this screen.
    --max-conn=M    Maximum number of allowed connections to the server [default: 10].
"""
from docopt import docopt
from multiprocessing import Process, Manager
from socket import *
import logging

BUFFER_SIZE = 4096
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class IndexingServer:
    def __init__(self, host, port, max_connect=10):
        self.host = host
        self.port = port
        self.max_connect = max_connect

        self.manager = Manager()
        self.peers_dict = self.manager.dict()
        self.files_dict = self.manager.dict()
        self.listening_socket = None

    def message_handler(self, conn):
        client_so, client_addr = conn
        logger.debug("Accepted connection from %s" % client_addr)
        client_so.send('Hi you!')
        msg = ''
        while True:
            shard = client_so.recv(BUFFER_SIZE)
            if not shard:
                break
            else:
                msg += shard
        print msg
        client_so.close()

    def run(self):
        socket_server = socket(AF_INET, SOCK_STREAM)
        socket_server.bind((self.host, self.port))
        socket_server.listen(self.max_connect)
        logger.debug("Server running on port %d" % self.port)

        while True:
            client_conn = socket_server.accept()
            handler = Process(target=self.message_handler, args=(client_conn))
            #handler.daemon = True
            handler.start()
        socket_server.close()

if __name__ == '__main__':
    args = docopt(__doc__)
    #print args
    indexingServer = IndexingServer(args['<ip>'], int(args['<port>']), int(args['--max-conn']))
    indexingServer.run()

