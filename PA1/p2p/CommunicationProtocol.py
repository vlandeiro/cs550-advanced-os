from socket import *

import logging
import pickle

END_MSG = '\r\n\n'
ACK = "0"
ERR = "1"
BUFFER_SIZE = 4096

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class MessageExchanger:
    def __init__(self, sock):
        self.sock = sock

    def send_ack(self):
        self.send(ACK)

    def send_err(self):
        self.send(ERR)
    
    def send(self, msg, ack=False):
        """Send a message through the socket as well as the end marker. Wait
        for acknowledgment if ack is True.

        """
        self.sock.send(msg)
        self.sock.send(END_MSG)
        if ack:
            ack_val = self.recv()
            return ack_val == ACK
        return True

    def recv(self):
        """Read the content in the socket until the end marker is reached.

        """
        msg = ''
        l = len(END_MSG)
        while True:
            shard = self.sock.recv(BUFFER_SIZE)
            if shard[-l:] == END_MSG:
                msg += shard[:-l]
                break
            else:
                msg += shard
        return msg
        
    def pkl_send(self, obj):
        """Pickle an object and send it through the socket with the end marker.

        """
        str_obj = pickle.dumps(obj)
        self.send(str_obj)

    def pkl_recv(self):
        """Read from the socket and unpickle the content.

        """
        str_obj = self.recv()
        return pickle.loads(str_obj)
        
        
