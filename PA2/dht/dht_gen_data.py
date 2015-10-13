import random
import string
import sys

val_size = 20
n_servers = 8
n_keys = 100000

choose_from = string.ascii_lowercase + string.ascii_uppercase + string.digits

for s in range(1, n_servers+1):
    sys.stderr.write("Create key value for server %d/%d.\n" % (s, n_servers))
    for i in range(n_keys):
        v = ''.join(random.choice(choose_from) for _ in range(val_size))
        print("%d%05d  %s" % (s, i, v))
