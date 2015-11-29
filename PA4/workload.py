import random
import string
from urllib2 import urlopen

KEYSIZE = 10
VALUESIZE = 90
SEED_BASE = "111191"

charset_list = list(string.ascii_lowercase + string.ascii_uppercase + string.digits)
l = len(charset_list)

def get_private_ip():
    return urlopen("http://169.254.169.254/latest/meta-data/hostname").read()

def gen_rand_string(size):
    return "".join([charset_list[int(random.random()*l)] for _ in range(size)])

def gen_workload(count):
    # same workload is generated every time for a given instance
    random.seed(SEED_BASE + get_private_ip())
    key_vals = {gen_rand_string(10): gen_rand_string(90) for _ in range(count)}
    # loop will be true when the same random key has been generated twice: that
    # is very unlikely!
    while len(key_vals) < count:
        key_vals[gen_rand_string(10)] = gen_rand_string(90)
    return key_vals
