import numpy as np
import matplotlib.pylab as plt
import cassandra
import riak
import pydht
import redis

systems = {
    'Cassandra': cassandra,
    'Riak': riak,
    'PyDHT': pydht,
    'Redis': redis
}
system_names = sorted(list(systems.keys()))
nops = 30000
X = [1, 2, 4, 8, 16]

def plot_latency(op_type):
    fig, ax = plt.subplots()
    for name in system_names:
        data = systems[name]
        for x in X:


def plot_throughput(op_type):
    pass
