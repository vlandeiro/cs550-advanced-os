from socket import *

import logging
import pickle
import sys

END_MSG = '\r\n\n'
ACK = "0"
ERR = "1"
BUFFER_SIZE = 4096
DUMMY = "DUMMY"

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class MessageExchanger:
    def __init__(self, sock):
        self.sock = sock

    def send_ack(self):
        logger.debug("SEND ACK")
        self.send(ACK)

    def send_err(self):
        logger.debug("SEND ERR")
        self.send(ERR)

    def send_dummy(self):
        logger.debug("SEND DUMMY")
        self.send(DUMMY)
    
    def send(self, msg, ack=False):
        """Send a message through the socket as well as the end marker. Wait
        for acknowledgment if ack is True.

        """
        self.sock.send(msg)
        logger.debug("SEND: " + msg)
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
            logger.debug("SHARD: " + shard)
            if shard[-l:] == END_MSG:
                msg += shard[:-l]
                break
            else:
                msg += shard
        logger.debug("RECV: " + msg)
        return msg
        
    def pkl_send(self, obj, ack=False):
        """Pickle an object and send it through the socket with the end marker.

        """
        str_obj = pickle.dumps(obj)
        return self.send(str_obj, ack=ack)

    def pkl_recv(self):
        """Read from the socket and unpickle the content.

        """
        str_obj = self.recv()
        return pickle.loads(str_obj)

    def file_send(self, f_path):
        logger.debug("SEND FILE " + f_path)
        with open(f_path, "rb") as f_to_send:
            data = True
            while data:
                data = f_to_send.read(BUFFER_SIZE)
                self.sock.send(data)
        self.sock.send(END_MSG)

    def file_recv(self, f_name, f_size=None):
        logger.debug("RECV FILE " + f_name)
        l = len(END_MSG)

        total_size = 0
        sys.stdout.write("Downloading file...")
        with open(f_name, "wb") as out_f:
            while True:
                shard = self.sock.recv(BUFFER_SIZE)
                total_size += sys.getsizeof(shard)
                if f_size is None:
                    sys.stdout.write("\rDownloading file... %d bytes" % total_size)
                    # print downloaded size
                    pass
                else:
                    # print percentage downloaded
                    perc = int(100.*total_size/f_size)
                    sys.stdout.write("\rDownloading file... %3d%%" % perc)
                if shard[-l:] == END_MSG:
                    out_f.write(shard[:-l])
                    break
                else:
                    out_f.write(shard)
        print
