#!/usr/bin/python
from workload import *
from rediscluster import StrictRedisCluster
import sys
import time

startup_nodes = [{'host': '127.0.0.1', 'port':'7000'}]
rc = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)

def benchmark_function(actions, f_name, workload):
    method = actions[f_name]
    t0 = time.time()
    if f_name in ['get', 'del']:
        for k, v in workload.items():
            method(k)
    else:
        for k, v in workload.items():
            method(k, v)
    t1 = time.time()
    delta = t1-t0
    print("Redis %s benchmark finished in %.3fs." % (f_name, delta))

def benchmark(size):
    workload = gen_workload(size)
    actions = {
        'put': rc.set,
        'get': rc.get,
        'del': rc.delete
    }

    benchmark_function(actions, 'put', workload)
    benchmark_function(actions, 'get', workload)
    benchmark_function(actions, 'del', workload)

if __name__ == '__main__':
    benchmark(30000)
