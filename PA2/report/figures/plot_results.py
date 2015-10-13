import matplotlib.pyplot as plt
import numpy as np

X = np.array([1,2,4,8])
Y = {
    'put': [
        47.230,
        48.410,
        50.085,
        54.124
    ],
    'get': [
        46.612,
        46.630,
        49.500,
        53.545
    ],
    'del': [
        46.943,
        48.023,
        49.729,
        54.056
    ]
}

fig, ax = plt.subplots()
markers = ['o', 's', 'D']
colors = ['r', 'g', 'b']
linestyles = ['-', '--', '-.']
n_ops = 100000

for m, c, l, (name, vals) in zip(markers, colors, linestyles, Y.iteritems()):
    vals = np.array(vals)*1000
    vals /= n_ops
    plt.plot(X, vals, label=name, marker=m, c=c, linestyle=l)

plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
           ncol=3, mode="expand", borderaxespad=0.)
ax.set_xlim([.9,3.1])
ax.set_ylim([.4,.6])
ax.set_xticks(X)
ax.set_ylabel("Average response time (ms).")
ax.set_xlabel("Number of concurrent nodes.")
plt.grid(True)
plt.savefig("results.png", format="png")

fig, ax = plt.subplots()
for m, c, l, (name, vals) in zip(markers, colors, linestyles, Y.iteritems()):
    vals = X*n_ops/np.array(vals)
    plt.plot(X, vals, label=name, marker=m, c=c, linestyle=l)

plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
           ncol=3, mode="expand", borderaxespad=0.)
ax.set_xlim([.9,3.1])
ax.set_xticks(X)
ax.set_ylabel("Number of operations per second.")
ax.set_xlabel("Number of concurrent nodes.")
plt.grid(True)
plt.savefig("results_2.png", format="png")
