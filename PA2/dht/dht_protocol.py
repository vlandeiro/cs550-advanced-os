import struct

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
