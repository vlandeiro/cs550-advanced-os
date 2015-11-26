#!/usr/bin/python
from workload import *
from cassandra.cluster import Cluster
import sys
import time

cluster = Cluster()
session = cluster.connect()

create_keyspace_cmd = """
CREATE KEYSPACE IF NOT EXISTS %s
WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};
"""

create_table_cmd = """
CREATE TABLE IF NOT EXISTS data (key varchar, val varchar, PRIMARY KEY (key));
"""

def benchmark_function(actions, f_name, workload):
    method = actions[f_name]
    t0 = time.clock()
    if f_name in ['get', 'del']:
        for k, v in workload.items():
            method(k)
    else:
        for k, v in workload.items():
            method(k, v)
    t1 = time.clock()
    delta = t1-t0
    print("Cassandra %s benchmark finished in %.3fs." % (f_name, delta))

def benchmark(size):
    keyspace = 'benchmark'
    workload = gen_workload(size)

    # create keyspace and table
    session.execute(create_keyspace_cmd % keyspace)
    session.execute("USE %s;" % keyspace)
    session.execute(create_table_cmd)

    put_cmd = session.prepare("INSERT INTO data (key, val) VALUES (?, ?)")
    get_cmd = session.prepare("SELECT * FROM data WHERE (key = ?)")
    del_cmd = session.prepare("DELETE FROM data WHERE (key = ?)")

    put = lambda k, v: session.execute(put_cmd.bind([k, v]))
    get = lambda k: session.execute(get_cmd.bind([k]))
    rem = lambda k: session.execute(del_cmd.bind([k]))

    actions = {
        'put': put,
        'get': get,
        'del': rem
    }

    benchmark_function(actions, 'put', workload)
    benchmark_function(actions, 'get', workload)
    benchmark_function(actions, 'del', workload)

    session.shutdown()
    cluster.shutdown()

if __name__ == '__main__':
    benchmark(1000)
