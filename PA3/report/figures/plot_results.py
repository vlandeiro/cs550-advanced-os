from results import *
import matplotlib.pyplot as plt
import numpy as np

file_dict_distributed = {
    '1K': 10000,
    '10K': 15000,
    '100K': 1500,
    '1M': 960,
    '10M': 480,
    '100M': 120,
    '1G': 15
}

file_dict_centralized = {
    '1K': 10000,
    '10K': 14000,
    '100K': 1400,
    '1M': 896,
    '10M': 448,
    '100M': 112,
    '1G': 14
}


def aggregate_ops(exp_results, ops_per_node, factor=1.):
    X = sorted(exp_results.keys())
    Y = []
    for n_nodes in X:
        times = exp_results[n_nodes]
        throughputs = [1. * ops_per_node / t for t in times]
        throughputs = [factor*t for t in throughputs]
        Y.append(np.sum(throughputs))
    return X, Y


def avg_response_time(exp_results, ops_per_node, factor=1.):
    X = sorted(exp_results.keys())
    Y = []
    yerr = []
    for n_nodes in X:
        times = exp_results[n_nodes]
        times_per_op = [t / ops_per_node for t in times]
        times_per_op = [factor*t for t in times_per_op]
        Y.append(np.mean(times_per_op))
        yerr.append(np.std(times_per_op))
    return X, Y, yerr


def aggregate_bps(exp_results, n_files, f=1.):
    X = []
    Y = []
    yerr = []
    str2bytes = {
        '1K': 1024,
        '10K': 1024 * 10,
        '100K': 1024 * 100,
        '1M': 1024 ** 2,
        '10M': 10 * 1024 ** 2,
        '100M': 100 * 1024 ** 2,
        '1G': 1024 ** 3
    }
    for file_size in exp_results.keys():
        size_per_file = str2bytes[file_size]
        X.append(size_per_file)
        files_count = n_files[file_size]
        total_size = size_per_file * files_count
        throughputs = [1. * total_size / t for t in exp_results[file_size]]
        throughputs = [f*t for t in throughputs]
        Y.append(np.sum(throughputs))
        yerr.append(np.std(throughputs))
    idx_sort = np.argsort(X)
    X = np.array(X)[idx_sort]
    Y = np.array(Y)[idx_sort]
    yerr = np.array(yerr)[idx_sort]
    return X, Y, yerr


def aggregate_ops_plot(tofile=None):
    fig, ax = plt.subplots()
    f = 1./1000
    # register
    X, Y_distributed = aggregate_ops(exp1_register_distributed, 10000, f)
    plt.plot(X, Y_distributed, 'ro-', label='Dist. register')
    X, Y_centralized = aggregate_ops(exp1_register_centralized, 10000, f)
    plt.plot(X, Y_centralized, 'ro--', label='Cent. register')

    # search
    X, Y_distributed = aggregate_ops(exp1_search_distributed, 10000, f)
    plt.plot(X, Y_distributed, 'bs-', label='Dist. search')
    X, Y_centralized = aggregate_ops(exp1_search_centralized, 10000, f)
    plt.plot(X, Y_centralized, 'bs--', label='Cent. search')

    # lookup
    X, Y_distributed = aggregate_ops(exp1_lookup_distributed, 10000, f)
    plt.plot(X, Y_distributed, 'g^-', label='Dist. lookup')
    X, Y_centralized = aggregate_ops(exp1_lookup_centralized, 10000, f)
    plt.plot(X, Y_centralized, 'g^--', label='Cent. lookup')

    plt.grid(True)
    lgd = plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
                     ncol=3, mode="expand", borderaxespad=0.)
    ax.set_xticks([1,2,4,8,16])
    ax.set_xlim([.9, 16.1])
    ax.set_ylabel('Throughput in KOPS.')
    ax.set_xlabel('Number of concurrent nodes.')
    if tofile:
        plt.savefig(tofile, bbox_extra_artists=(lgd,), bbox_inches='tight')
    else:
        plt.show()


def avg_response_time_plot(tofile=None):
    fig, ax = plt.subplots()
    f = 1000
    # register
    X, Y, yerr = avg_response_time(exp1_register_distributed, 10000, f)
    plt.errorbar(X, Y, yerr=yerr, fmt='ro-', label='Dist. register')
    X, Y, yerr = avg_response_time(exp1_register_centralized, 10000, f)
    plt.errorbar(X, Y, yerr=yerr, fmt='ro--', label='Cent. register')

    # search
    X, Y, yerr = avg_response_time(exp1_search_distributed, 10000, f)
    plt.errorbar(X, Y, yerr=yerr, fmt='bs-', label='Dist. search')
    X, Y, yerr = avg_response_time(exp1_search_centralized, 10000, f)
    plt.errorbar(X, Y, yerr=yerr, fmt='bs--', label='Cent. search')

    # lookup
    X, Y, yerr = avg_response_time(exp1_lookup_distributed, 10000, f)
    plt.errorbar(X, Y, yerr=yerr, fmt='g^-', label='Dist. lookup')
    X, Y, yerr = avg_response_time(exp1_lookup_centralized, 10000, f)
    plt.errorbar(X, Y, yerr=yerr, fmt='g^--', label='Cent. lookup')

    plt.grid(True)
    lgd = plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
                     ncol=3, mode="expand", borderaxespad=0.)
    ax.set_xticks([1,2,4,8,16])
    ax.set_xlim([.9, 16.1])
    ax.set_ylabel('Average response time per node in ms.')
    ax.set_xlabel('Number of concurrent nodes.')
    if tofile:
        plt.savefig(tofile, bbox_extra_artists=(lgd,), bbox_inches='tight')
    else:
        plt.show()

def aggregate_bps_plot(tofile=None):
    fig, ax = plt.subplots()
    f = 1./1024**2
    X, Y, yerr = aggregate_bps(exp2_lookup_distributed, file_dict_distributed, f)
    plt.errorbar(X, Y, yerr=yerr, fmt='ko-', label='Distributed')
    X, Y, yerr = aggregate_bps(exp2_lookup_centralized, file_dict_centralized, f)
    plt.errorbar(X, Y, yerr=yerr, fmt='ks--', label='Centralized')

    plt.grid(True)
    lgd = plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
                     ncol=3, mode="expand", borderaxespad=0.)
    ax.set_xscale('log')
    ax.set_xticks(X)
    ax.set_xticklabels(['1K', '10K', '100K', '1M', '10M', '100M', '1G'])
    ax.set_xlim([1000, 1024**3+1000])
    ax.set_xlabel('File size used for lookup.')
    ax.set_ylabel('Throughput in MB/s.')
    if tofile:
        plt.savefig(tofile, bbox_extra_artists=(lgd,), bbox_inches='tight')
    else:
        plt.show()

aggregate_ops_plot('fig1.png')
avg_response_time_plot('fig2.png')
aggregate_bps_plot('fig3.png')
