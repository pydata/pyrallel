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
#with dv.sync_imports():
#    import numpy as np
#    from hashlib import md5

A = np.memmap("/tmp/pyrallel_sample_array", dtype=float,
                 shape=(100, 128), mode='write')

rng = np.random.RandomState(10)
A[:] = rng.random_integers(0, 100, A.shape)

ars, engines_by_datastore = bcast_memmap(dv, 'B', A)

# block here to raise any potential exceptions:
[ar.get() for ar in ars]

for datastore_id, targets in engines_by_datastore.iteritems():
    print datastore_id,
    print rc[targets].apply_sync(lambda: B.filename)
    print datastore_id,
    print rc[targets].apply_sync(lambda: md5(B).hexdigest()[:7])
