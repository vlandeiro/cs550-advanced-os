import abc

from hashlib import md5
from CommunicationProtocol import MessageExchanger
from socket import *

class HT():
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def put(self, key, val):
        """Send message to the indexing server to add the key/value
        pair to the index."""
        return
    
    @abc.abstractmethod
    def get(self, key):
        """Send message to the indexing server to retrieve the value
        associated with the given key."""
        return

class CentralizedHT(HT):
    def __init__(self, config):
        self.ip = config['ip']
        self.port = config['port']

        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.connect((self.ip, self.port))
        self.msg_exch = MessageExchanger(self.sock)

    def put(self, key, val):
        msg = "put %s %s" % (key, val)
        try:
            self.msg_exch.send(msg)
        except timeout:
            return False
        return True

    def get(self, key):
        msg = "get %s" % key
        try:
            self.msg_exch.send(msg)
        except timeout:
            return False
        return self.msg_exch.recv()

class DecentralizedISCom(HT):
    def __init__(self, config):
        self.nodes = config['nodes']
        self.nodes_count = len(self.nodes)
        self.id = config['id']

        self.sockets = {}
        self.msg_exchs = {}
        for n_id, n_attr in self.nodes.iteritems():
            if n_id != self.id:
                sock = socket(AF_INET, SOCK_STREAM)
                sock.connect((n_attr['ip'], n_attr['port']))
                self.sockets[n_id] = sock
                msg_exch = MessageExchanger(sock)
                self.msg_exchs[n_id] = msg_exch

    def sid(self, key):
        sid = int(md5(key).hexdigest(), 16) % self.nodes_count
        alt_sid = (sid + 1) % self.nodes_count
        return sid, alt_sid
    
    def put(self, key, val):
        sid, alt_sid = self.sid(key)
        msg = "put %s %s" % (key, val)
        try:
            self.msg_exchs[sid].send(msg)
        except timeout:
            try:
                self.msg_exchs[alt_sid].send(msg)
            except timeout:
                return False
        return True

    def get(self, key):
        sid, alt_sid = self.sid(key)
        msg = "get %s" % key
        try:
            self.msg_exchs[sid].send(msg)
            return self.msg_exchs[sid].recv()
        except timeout:
            try:
                self.msg_exchs[alt_sid].send(msg)
                return self.msg_exchs[alt_sid].recv()
            except timeout:
                return False
        
        
