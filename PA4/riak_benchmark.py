#!/usr/bin/python
from workload import *
import sys
import time
from riak import RiakClient, RiakNode

rc = RiakClient(protocol='pbc', host='127.0.0.1', pb_port=8087)
#rc = RiakClient(protocol='http', host='127.0.0.1', pb_port=8098)
bucket = rc.bucket('benchmark')

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
    print("Redis %s benchmark finished in %.3fs." % (f_name, delta))

def benchmark(size):
    workload = gen_workload(size)
    actions = {
        'put': lambda x,y: bucket.new(x, data=y).store(),
        'get': bucket.get,
        'del': bucket.delete
    }

    benchmark_function(actions, 'put', workload)
    benchmark_function(actions, 'get', workload)
    benchmark_function(actions, 'del', workload)

if __name__ == '__main__':
    benchmark(30000)
