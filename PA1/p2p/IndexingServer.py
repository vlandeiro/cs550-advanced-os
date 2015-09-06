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

    def message_handler(self, client_so, client_addr):
        logger.debug("Accepted connection from %s", client_addr)
        pickled_msg = ''
        while True:
            shard = client_so.recv(BUFFER_SIZE)
            logger.debug("shard: " + shard)
            if shard:
                pickled_msg += shard
            else:
                break
        logger.debug("pickle: " + pickled_msg)
        client_so.close()

    def run(self):
        """Main function. It handles the connection from peers to the indexing
        server. Everytime a peer connects to the server, a new socket
        is spawned to handle the communication with this specific
        peer. Once the communication is over, this socket is closed.

        """
        self.listening_socket = socket(AF_INET, SOCK_STREAM)
        self.listening_socket.bind((self.host, self.port))
        self.listening_socket.listen(self.max_connect)
        logger.debug("Indexing server listening on port %d", self.port)

        while True:
            client_so, client_addr = self.listening_socket.accept()
            handler = Process(target=self.message_handler, args=(client_so, client_addr))
            handler.daemon = True
            handler.start()
        self.listening_socket.close()

if __name__ == '__main__':
    args = docopt(__doc__)
    #print args
    indexingServer = IndexingServer(args['<ip>'], int(args['<port>']), int(args['--max-conn']))
    indexingServer.run()
