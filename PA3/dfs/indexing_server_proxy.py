import abc
import sys
import errno
import logging

from hashlib import md5
from CommunicationProtocol import *
from socket import *

logging.basicConfig(level=logging.DEBUG)

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
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)

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
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)

    def get_peer_sock(self, server_id):
        if server_id not in self.socket_map:
            sock = socket(AF_INET, SOCK_STREAM)
            peer_ip = self.parent.nodes_list[server_id]
            try:
                sock.connect((peer_ip, self.parent.port))
                self.socket_map[server_id] = sock
            except error as e:
                if e.errno == errno.ECONNREFUSED: # peer is not online
                    self.logger.debug("Peer %s seems to be offline.", server_id)
                    self.socket_map[server_id] = False
        return self.socket_map[server_id]

    def init_connection(self, id):
        pass

    def close_connection(self, id):
        pass

    def list(self):
        ls = set(self.parent.keys())
        for sid, ip in enumerate(self.parent.nodes_list):
            if ip != self.parent.ip:
                sock = self.get_peer_sock(sid)
                if not sock:
                    continue
                exch = MessageExchanger(sock)
                dht_action = dict(action="keys", args=[])
                exch.pkl_send(dht_action)
                res = exch.pkl_recv()
                ls |= set(res)
        return list(ls)

    def register(self, id, name):
        previous_peers = self._get(name)
        if previous_peers is False: # nodes are offline
            return False
        if previous_peers is None:
            previous_peers = []
        previous_peers.append(id)
        self._put(name, previous_peers)
        # TODO: file replication and metadata replication
        return []

    def search(self, id, name):
        available_peers = self._get(name)
        if id in available_peers:
            available_peers.remove(id)
        return available_peers

    # def _generic_action(self, action, key, args):
    #     # hash key to get the server id
    #     server_id = self.parent.server_hash(key)
    #     return self._generic_action_sid(server_id, action, key, args)

    def _generic_action_sid(self, sid, action, args):
        sys.stderr.write("%s\n" % action)
        # if local call parent, else call or connect to server
        if sid == self.parent.id:
            method = getattr(self.parent, action)
            res = method(*args)
        else:
            sock = self.get_peer_sock(sid)
            if sock is None:
                return False
            exch = MessageExchanger(sock)
            dht_action = dict(action=action, args=args)
            exch.pkl_send(dht_action)
            res = exch.pkl_recv()
        return res

    def _put(self, key, value):
        sid = self.parent.server_hash(key)
        sid_replica = (sid + 1) % self.parent.nodes_count
        self._generic_action_sid(sid_replica, "put", [key, value])
        return self._generic_action_sid(sid, "put", [key, value])

    def _get(self, key):
        sid = self.parent.server_hash(key)
        sid_replica = (sid + 1) % self.parent.nodes_count
        sids = [sid, sid_replica]
        obtained = False
        while not obtained and sids:
            sid = sids.pop(0)
            obtained = self._generic_action_sid(sid,"get", [key])

    def _del(self, key):
        sid = self.parent.server_hash(key)
        sid_replica = (sid + 1) % self.parent.nodes_count
        sids = [sid, sid_replica]
        for sid in sids:
            self._generic_action_sid(sid, "rem", [key])
        return True
