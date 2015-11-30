import numpy as np
import matplotlib.pylab as plt
import cassandra
import riak
import pydht
import redis
from collections import defaultdict

systems = {
    'Cassandra': (cassandra,'kx-.'),
    'Riak': (riak, 'b+--'),
    'PyDHT': (pydht, 'r*:'),
    'Redis': (redis, 'g^-')
}
system_names = sorted(list(systems.keys()))
nops = 30000
X = [1, 2, 4, 8, 16]
op_types = ['put', 'get', 'delete']

def get_datapoints_latency(op_type, factor=1000):
    latencies = dict()
    for name in system_names:
        Y = []
        Yerr = [[], []]
        syst, fmt = systems[name]
        data = getattr(syst, op_type)
        for x in X:
            datapoints = np.array(data[x])
            results = datapoints*factor/nops
            avg = np.mean(results)
            M = np.max(results)
            m = np.min(results)
            Y.append(avg)
            Yerr[0].append(avg-m)
            Yerr[1].append(M-avg)
        latencies[name] = (Y,Yerr)
    return latencies

def plot_latency(op_type, factor=1000, tofile=None):
    fig, ax = plt.subplots()
    latencies = get_datapoints_latency(op_type, factor)
    for name in system_names:
        Y, Yerr = latencies[name]
        syst, fmt = systems[name]
        plt.errorbar(X,Y,yerr=Yerr,fmt=fmt,label=name)

    ax.set_xticks(X)
    ax.set_xlabel('Number of concurrent nodes.')
    ax.set_xlim([0,17])
    ax.set_ylabel('Latency in ms for %s operations.' % op_type)
    lgd = plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
                     ncol=4, mode="expand", borderaxespad=0.)
    plt.grid(True)
    if tofile is not None:
        plt.savefig(tofile, bbox_extra_artists=(lgd,), bbox_inches='tight')
    else:
        plt.show()

def get_datapoints_throughput(op_type, factor=1000):
    throughputs = dict()
    for name in system_names:
        Y = []
        syst, fmt = systems[name]
        data = getattr(syst, op_type)
        for x in X:
            datapoints = np.array(data[x])
            results = nops/(datapoints*factor)
            agg = np.sum(results)
            Y.append(agg)
        throughputs[name] = Y
    return throughputs

def plot_throughput(op_type, factor=1000, tofile=None):
    fig, ax = plt.subplots()

    Y = get_datapoints_throughput(op_type, factor)
    for name in system_names:
        syst, fmt = systems[name]
        plt.errorbar(X,Y[name],fmt=fmt,label=name)
    ax.set_xticks(X)
    ax.set_xlabel('Number of concurrent nodes.')
    ax.set_xlim([0,17])
    ax.set_ylabel('Throughput in KOps per second for %s operations.' % op_type)
    lgd = plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
                     ncol=4, mode="expand", borderaxespad=0.)
    plt.grid(True)
    if tofile is not None:
        plt.savefig(tofile, bbox_extra_artists=(lgd,), bbox_inches='tight')
    else:
        plt.show()

def plot_avg(measure_type, tofile=None):
    fig,ax = plt.subplots()
    for name in system_names:
        Y = []
        syst, fmt = systems[name]
        for op in op_types:
            if measure_type == 'latency':
                latencies = get_datapoints_latency(op)
                Y_op, _ = latencies[name]
            elif measure_type == 'throughput':
                throughputs = get_datapoints_throughput(op)
                Y_op = throughputs[name]
            Y.append(Y_op)
        Y = np.mean(Y, axis=0)
        plt.errorbar(X,Y,fmt=fmt,label=name)
    ax.set_xticks(X)
    ax.set_xlabel('Number of concurrent nodes.')
    ax.set_xlim([0,17])
    if measure_type == 'throughput':
        ax.set_ylabel('Average throughput in KOps per second.')
    elif measure_type == 'latency':
        ax.set_ylabel('Average latency in ms.')
    lgd = plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
                     ncol=4, mode="expand", borderaxespad=0.)
    plt.grid(True)
    if tofile is not None:
        plt.savefig(tofile, bbox_extra_artists=(lgd,), bbox_inches='tight')
    else:
        plt.show()


for metric in ['latency', 'throughput']:
    for op in op_types:
        globals()["plot_" + metric](op, tofile=metric + '_' + op + '.png')
# plot_latency('put', 1000., tofile='latency_put.png')
# plot_throughput('put', 1000., tofile='throughput_put.png')
    plot_avg(metric, tofile='avg_' + metric + '.png')
