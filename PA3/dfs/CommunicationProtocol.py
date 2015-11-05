from socket import *

import struct
import pickle
import sys
import os
import logging
import errno
import time

BUFFER_SIZE = 4096

logging.basicConfig(level=logging.DEBUG)


class MessageExchanger:
    def __init__(self, sock, log='INFO'):
        """
        Initialize a MessageExchanger object.
        :param sock: socket through which communication is done.
        :param log: level of logging for this object.
        :return: None
        """
        self.sock = sock
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.getLevelName(log))

    def send(self, msg, trials=5):
        """
        Send a message by sending its length first and then the content.
        :param msg: message to send as a string.
        :return: None
        """
        # send length of the message
        try:
            msg = struct.pack('>I', len(msg)) + msg
            self.sock.sendall(msg)
        except error as e:
            if e.errno == errno.EAGAIN:
                if trials == 0:
                    raise
                else:
                    time.sleep(.1)
                    self.send(msg, trials=trials-1)

    def recv(self):
        """
        Receive a message by getting its length first and then the content.
        :return: message received as a string.
        """
        # get message length
        raw_msglen = self.recvall(4)
        if not raw_msglen:
            return None
        msglen = struct.unpack('>I', raw_msglen)[0]
        # get message
        return self.recvall(msglen)

    def recvall(self, n):
        """
        Helper function to receive n bytes or return None if EOF is hit.
        :param n: number of bytes to receive.
        :return: data received as a string.
        """
        data = ''
        while len(data) < n:
            packet = self.sock.recv(n - len(data))
            if not packet:
                return None
            data += packet
        return data

    def pkl_send(self, obj):
        """
        Pickle an object and send it through the socket with the end marker.
        :param obj: python object to send.
        :return: None
        """
        self.logger.debug("send pkl: %s", repr(obj))
        str_obj = pickle.dumps(obj)
        return self.send(str_obj)

    def pkl_recv(self):
        """
        Read from the socket and unpickle the content.
        :return: python object received.
        """
        str_obj = self.recv()
        if str_obj is None:
            return None
        self.logger.debug("recv pkl str: %s", str_obj)
        obj = pickle.loads(str_obj)
        self.logger.debug("recv pkl: %s", repr(obj))
        return obj

    def file_send(self, f_path):
        """
        Send a file by first sending its size in bytes and t hen sending the file.
        :param f_path: local path to the file to send.
        :return: False if the file does not exist, else True.
        """
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

    def file_recv(self, f_path, show_progress=True):
        """
        Receive a file and store it on disk.
        :param f_path: local path to store the file.
        :param show_progress: if True, messages will be printed to show the progress of the file transfer.
        :return: None
        """
        bytes_received = 0

        raw_filesize = self.recvall(4)
        if not raw_filesize:
            return None
        filesize = struct.unpack('>I', raw_filesize)[0]

        self.logger.debug("recv file: %s %d", f_path, filesize)
        out_f = os.open(f_path, os.O_WRONLY | os.O_CREAT)
        while bytes_received < filesize:
            shard = self.sock.recv(BUFFER_SIZE)
            bytes_written = os.write(out_f, shard)
            bytes_received += bytes_written
            self.logger.debug("Received %d bytes", bytes_received)
            # print percentage downloaded
            if show_progress:
                perc = int(100. * bytes_received / filesize)
                self.logger.debug("Downloading file... %3d%% [%d/%d]" % (perc, bytes_received, filesize))
        if show_progress:
            print
        os.close(out_f)
