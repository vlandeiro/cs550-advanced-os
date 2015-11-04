from socket import *

import struct
import pickle
import sys
import os
import logging

BUFFER_SIZE = 4096

logging.basicConfig(level=logging.DEBUG)

class MessageExchanger:
    def __init__(self, sock, log='INFO'):
        self.sock = sock
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.getLevelName(log))

    def send(self, msg):
        # send length of the message
        msg = struct.pack('>I', len(msg)) + msg
        self.sock.sendall(msg)

    def recv(self):
        # get message length
        raw_msglen = self.recvall(4)
        if not raw_msglen:
            return None
        msglen = struct.unpack('>I', raw_msglen)[0]
        # get message
        return self.recvall(msglen)

    def recvall(self, n):
        # Helper function to recv n bytes or return None if EOF is hit
        data = ''
        while len(data) < n:
            packet = self.sock.recv(n - len(data))
            if not packet:
                return None
            data += packet
        return data
        
    def pkl_send(self, obj):
        """Pickle an object and send it through the socket with the end marker.

        """
        self.logger.debug("send pkl: %s", repr(obj))
        str_obj = pickle.dumps(obj)
        return self.send(str_obj)

    def pkl_recv(self):
        """Read from the socket and unpickle the content.

        """
        str_obj = self.recv()
        if str_obj is None:
            return None
        self.logger.debug("recv pkl str: %s", str_obj)
        obj = pickle.loads(str_obj)
        self.logger.debug("recv pkl: %s", repr(obj))
        return obj

    def file_send(self, f_path):
        try:
            fs = os.path.getsize(f_path)
            header = struct.pack('>L', fs)
        except OSError:
            return False

        self.logger.debug("send file: %s %d", f_path, fs)
        self.sock.send(header)
        with open(f_path, "rb") as f_to_send:
            data = True
            while data:
                data = f_to_send.read(BUFFER_SIZE)
                self.sock.send(data)
        return True

    def file_recv(self, f_name, show_progress=True):
        bytes_received = 0

        raw_filesize = self.recvall(4)
        if not raw_filesize:
            return None
        filesize = struct.unpack('>I', raw_filesize)[0]

        self.logger.debug("recv file: %s %d", f_name, filesize)
        out_f = os.open(f_name, os.O_WRONLY|os.O_CREAT)
        while bytes_received < filesize:
            shard = self.sock.recv(BUFFER_SIZE)
            bytes_written = os.write(out_f, shard)
            bytes_received += bytes_written
            self.logger.debug("Received %d bytes", bytes_received)
            # print percentage downloaded
            if show_progress:
                perc = int(100.*bytes_received/filesize)
                self.logger.debug("Downloading file... %3d%% [%d/%d]" % (perc, bytes_received, filesize))
        if show_progress:
            print
        os.close(out_f)
