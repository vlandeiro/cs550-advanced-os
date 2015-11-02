import abc

from hashlib import md5
from CommunicationProtocol import *
from socket import *

class ISProxy():
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def init_connection(self, id):
        return

    @abc.abstractmethod
    def close_connection(self, id):
        return

    @abc.abstractmethod
    def list(self):
        return

    @abc.abstractmethod
    def register(self, id, name):
        return

    @abc.abstractmethod
    def search(self, id, name):
        return

class CentralizedISProxy(ISProxy):
    def __init__(self, sock):
        self.exch = MessageExchanger(sock)

    def init_connection(self, id):
        idx_action = dict(type='init', id=id)
        self.exch.pkl_send(idx_action)

    def close_connection(self, id):
        idx_action = dict(type='close', id=id)
        self.exch.pkl_send(idx_action)

    def list(self):
        idx_action = dict(type='list')
        self.exch.pkl_send(idx_action)
        return self.exch.pkl_recv()

    def register(self, id, name):
        idx_action = dict(type='register', name=name, id=id)
        self.exch.pkl_send(idx_action)
        replicate_to = self.exch.pkl_recv()
        return replicate_to

    def search(self, id, name):
        idx_action = dict(type='search', id=id, name=name)
        # request indexing server
        self.exch.pkl_send(idx_action)
        available_peers = self.exch.pkl_recv()
        return available_peers

class DistributedISProxy(ISProxy):
    def __init__(self, parent):
        self.parent = parent
        self.socket_map = [None]*self.parent.nodes_count

    def get_peer_sock(self, server_id):
        if server_id not in self.socket_map:
            sock = socket(AF_INET, SOCK_STREAM)
            peer_ip = self.parent.nodes_list[server_id]
            sock.connect((peer_ip, self.parent.idx_server_port))
            self.socket_map[server_id] = sock
        return self.socket_map[server_id]

    def init_connection(self, id):
        pass

    def close_connection(self, id):
        pass

    def list(self):
        ls = set(self.parent.keys())
        for sid in self.parent.nodes_list:
            sock = self.get_peer_sock(sid)
            exch = MessageExchanger(sock)
            exch.send("keys")
            res = exch.pkl_recv()
            ls |= set(res)
        return list(ls)

    def register(self, id, name):
        self._put(id, name)
        # TODO: file replication and metadata replication
        return []

    def search(self, id, name):
        self._get(name)

    def _generic_action(self, action, key, args):
        # hash key to get the server id
        server_id = self.parent.server_hash(key)
        # if local call parent, else call or connect to server
        if server_id == self.parent.id:
            method = getattr(self.parent, action)
            res = method(*args)
        else:
            sock = self.get_peer_sock(server_id)
            exch = MessageExchanger(sock)
            exch.send("%s %s" % (action, " ".join(args)))
            res = exch.recv()
            if res in str2py:
                res = str2py[res]
        return res

    def _put(self, key, value):
        return self._generic_action("put", key, [key, value])

    def _get(self, key):
        return self._generic_action("get", key, [key])

    def _del(self, key):
        return self._generic_action("rem", key, [key])