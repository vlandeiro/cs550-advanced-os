import matplotlib.pyplot as plt

X = [1,2,3]
Y = {
    'search': [156.16,157.545,155.97],
    'lookup': [255.45,253.695,252.807],
    'register': [943.14,950.44,939.28]
}

fig, ax = plt.subplots()
markers = ['+', 's', 'D']
colors = ['r', 'g', 'b']
linestyles = ['-', '--', '-.']

for m, c, l, (name, vals) in zip(markers, colors, linestyles, Y.iteritems()):
    plt.plot(X, vals, label=name, marker=m, c=c, linestyle=l)

plt.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc=3,
           ncol=3, mode="expand", borderaxespad=0.)
ax.set_xlim([.9,3.1])
ax.set_xticks(X)
ax.set_ylabel("Average time per operation (ms)")
ax.set_xlabel("Number of peers")
plt.grid(True)
plt.savefig("results.png", format="png")
