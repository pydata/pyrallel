"""
============================================
Numpy array broadcasting with memory mapping
============================================

This example demonstrates the usage of the
:func:`pyrallel.broadcast.bcast_memmap` function to efficiently give a
read access to the same data array to each engine of the cluster.

This function tries to spare the cluster resources (network and RAM) as
much as possible: a digest is first computed on the original data (living
in the client python process) and each engine of the cluster is asked
which store it is associated to and whether the store already as the data.

The data is then sent once to each datastore missing the data and
materialized there as a numpy.memmap array. Each engine of the view
can thus load the data in their active namespace using shared memory:
if several engines happen to run on the same node, the data will be only
loaded once in memory.

"""
import sys
import numpy as np

from IPython import parallel
from pyrallel.broadcast import bcast_memmap


if len(sys.argv) > 1:
    profile = sys.argv[1]
else:
    profile = None

rc = parallel.Client(profile=profile)
dv = rc[:]

A = np.memmap("/tmp/pyrallel_sample_array", dtype=float,
              shape=(100, 128), mode='write', order='F')

rng = np.random.RandomState(10)
A[:] = rng.random_integers(0, 100, A.shape)

# A = np.asfortranarray(rng.random_integers(0, 100, A.shape))

ars, engines_by_datastore = bcast_memmap(dv, 'B', A)

# block here to raise any potential exceptions:
[ar.get() for ar in ars]

for i, (datastore_id, targets) in enumerate(engines_by_datastore.iteritems()):
    print "Datastore #%d: %s" % (i, datastore_id)
    print "Engines:", targets
    print "Filename:", rc[targets].apply_sync(lambda: B.filename)
    print "MD5 digest:", rc[targets].apply_sync(
        lambda: md5(B).hexdigest()[:7])
    print 'F_CONTIGUOUS:', rc[targets].apply_sync(
        lambda: B.flags['F_CONTIGUOUS'])
    print
