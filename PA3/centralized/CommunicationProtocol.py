from socket import *

import struct
import pickle
import sys
import os

BUFFER_SIZE = 4096

py2str = {
    True: "SUCCESS",
    False: "FAILURE",
    None: "NONE"
}

str2py = {
    "SUCCESS": True,
    "FAILURE": False,
    "NONE": None
}

class MessageExchanger:
    def __init__(self, sock):
        self.sock = sock

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
        str_obj = pickle.dumps(obj)
        return self.send(str_obj)

    def pkl_recv(self):
        """Read from the socket and unpickle the content.

        """
        str_obj = self.recv()
        return pickle.loads(str_obj)

    def file_send(self, f_path):
        try:
            fs = os.path.getsize(f_path)
            header = struct.pack('>L', fs)
        except OSError:
            return False

        self.sock.send(header)
        with open(f_path, "rb") as f_to_send:
            data = True
            while data:
                data = f_to_send.read(BUFFER_SIZE)
                self.sock.send(data)

    def file_recv(self, f_name, show_progress=True):
        bytes_received = 0

        raw_filesize = self.recvall(4)
        if not raw_filesize:
            return None
        filesize = struct.unpack('>I', raw_msglen)[0]

        out_f = os.open(f_name, os.O_WRONLY|os.O_CREAT)
        keep_reading = True
        while keep_reading:
            shard = self.sock.recv(BUFFER_SIZE)
            bytes_written = os.write(out_f, shard)
            bytes_received += bytes_written
            # print percentage downloaded
            if show_progress:
                perc = int(100.*total_size/filesize)
                sys.stdout.write("\rDownloading file... %3d%%" % perc)
        if show_progress:
            print
        os.close(out_f)
