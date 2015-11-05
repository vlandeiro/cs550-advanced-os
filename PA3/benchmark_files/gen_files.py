import sys
from time import time

# generate files for first experiment
def files_exp1(): # around 10 seconds
    block_1K = "0"*1024
    nb_files = 10**4
    for i in xrange(nb_files):
        fname = "exp1/f%06d" % i
        with open(fname, 'wb') as fd:
            fd.write(block_1K)

def files_exp2():
    block_sizes = [1024, 1024, 10*1024, 128*1024, 1024**2, 10*1024**2, 128*1024**2]
    blocks = ["0"*bs for bs in block_sizes]
    file_sizes = [1024, 10*1024, 100*1024, 1024**2, 10*1024**2, 100*1024**2, 1024**3]
    blocks_count = [x/y for x,y in zip(file_sizes, block_sizes)]
    file_count = [10**4, 10**3, 10**2, 10, 4, 2, 1]

    for k, (b, bc, fc, fs) in enumerate(zip(blocks, blocks_count, file_count, file_sizes)):
        print "Creating %d files of size %d." % (fc, fs)
        sys.stdout.flush()
        t0 = time()
        for fi in xrange(fc):
            fname = "exp2/f%d_%06d" % (k, fi)
            with open(fname, "wb") as fd:
                for fb in xrange(bc):
                    fd.write(b)
        delta = time() - t0
        print "Took %.2f seconds." % delta
        sys.stdout.flush()

if __name__ == "__main__":
    files_exp2()