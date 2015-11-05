import abc
import sys
import errno
import logging
import numpy as np
import copy

from hashlib import md5
from CommunicationProtocol import *
from socket import *

logging.basicConfig(level=logging.DEBUG)


class ISProxy():
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def init_connection(self, id):
        """
        Initialize the connection with an indexing server.
        :param id: identifier of the peer.
        :return:
        """
        return

    @abc.abstractmethod
    def close_connection(self, id):
        """
        End the connection with an indexing server.
        :param id: identifier of the peer.
        :return:
        """
        return

    @abc.abstractmethod
    def list(self):
        """
        List all the files stored in the indexing server.
        :return: list of all the files available in the indexing server.
        """
        return

    @abc.abstractmethod
    def register(self, id, name):
        """
        Register a given file to the indexing server.
        :param id: identifier of the peer.
        :param name: name of the file to register.
        :return:
        """
        return

    @abc.abstractmethod
    def search(self, id, name):
        """
        Search a file in the indexing server.
        :param id: identifier of the peer.
        :param name: name of the file to search.
        :return: list of peers where the file is available.
        """
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
        self.socket_map = [None] * self.parent.nodes_count
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.getLevelName(self.parent.log_level))

    def get_peer_sock(self, server_id):
        self.logger.debug("Server id requested: %s", repr(server_id))
        if self.socket_map[server_id] is None:
            sock = socket(AF_INET, SOCK_STREAM)
            peer_ip = self.parent.nodes_list[server_id]
            try:
                sock.connect((peer_ip, self.parent.port))
                self.socket_map[server_id] = sock
            except error as e:
                if e.errno == errno.ECONNREFUSED:  # peer is not online
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
                self.logger.debug(repr(sock))
                exch = MessageExchanger(sock)
                dht_action = dict(action="keys", args=[])
                exch.pkl_send(dht_action)
                res = exch.pkl_recv()
                ls |= set(res)
        return list(ls)

    def _local_register(self, id, name, replicate=True):
        previous_value = self._get(name)
        self.logger.debug(repr(previous_value))
        if previous_value is False:  # nodes are offline
            return False
        if previous_value is None:
            previous_value = []
        previous_value.append(id)
        self._put(name, previous_value, replicate=replicate)

    def register(self, id, name):
        ret = self._local_register(id, name)
        if ret is False:
            return False
        other_peers = copy.copy(self.parent.nodes_list)
        other_peers.pop(self.parent.id)
        other_peers = [":".join([x, str(self.parent.config['file_server_port'])]) for x in other_peers]
        nb_replica = self.parent.replica
        replicate_to = []
        if nb_replica == 0:
            replicate_to = []
        elif len(other_peers) < nb_replica:
            replicate_to = other_peers
        else:
            replicate_to = np.random.choice(other_peers, nb_replica)
        for k in replicate_to:
            self._local_register(k, name, replicate=False)
        return replicate_to

    def search(self, id, name):
        available_peers = self._get(name)
        if available_peers and id in available_peers:
            available_peers.remove(id)
        return available_peers

    def _generic_action_sid(self, sid, action, args):
        self.logger.debug("%s in %s", action, sid)
        # if local, call parent
        if sid == self.parent.id:
            self.logger.debug("Local function call")
            method = getattr(self.parent, action)
            res = method(*args)
        else:
            self.logger.debug("Network function call")
            sock = self.get_peer_sock(sid)
            self.logger.debug(repr(sock))
            if not sock:
                return False
            exch = MessageExchanger(sock)
            dht_action = dict(action=action, args=args)
            exch.pkl_send(dht_action)
            res = exch.pkl_recv()
        return res

    def _put(self, key, value, replicate=True):
        sid = self.parent.server_hash(key)
        if replicate and self.parent.replica > 0:
            sid_replica = (sid + 1) % self.parent.nodes_count
            self._generic_action_sid(sid_replica, "put", [key, value])
        return self._generic_action_sid(sid, "put", [key, value])

    def _get(self, key):
        sid = self.parent.server_hash(key)
        sid_replica = (sid + 1) % self.parent.nodes_count
        sids = [sid, sid_replica]
        obtained = False
        while obtained == False and sids:
            sid = sids.pop(0)
            obtained = self._generic_action_sid(sid, "get", [key])
            self.logger.debug("obtained: %s", repr(obtained))
        return obtained

    def _del(self, key):
        sid = self.parent.server_hash(key)
        sid_replica = (sid + 1) % self.parent.nodes_count
        sids = [sid, sid_replica]
        for sid in sids:
            self._generic_action_sid(sid, "rem", [key])
        return True
