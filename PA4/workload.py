import random
import string

KEYSIZE = 10
VALUESIZE = 90

charset_list = list(string.ascii_lowercase + string.ascii_uppercase + string.digits)
l = len(charset_list)

def gen_rand_string(size):
    return "".join([charset_list[int(random.random()*l)] for _ in range(size)])

def gen_workload(count):
    key_vals = {gen_rand_string(10): gen_rand_string(90) for _ in range(count)}
    # loop will be true when the same random key has been generated twice: that
    # is very unlikely!
    while len(key_vals) < count:
        key_vals[gen_rand_string(10)] = gen_rand_string(90)
