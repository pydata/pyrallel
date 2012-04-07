"""Optimized broadcast for numpy arrays

Data is sent from the client to each 'datastore' at most once,
and loaded into memmapped arrays.

In this example, the 'datastore' is the '~/.pyrallel/datastore' directory on
each physical machine.

General flow:

0. hash data
1. query all engines for datastore ID (default: hostname),
   remote filename as function of hash, and whether data
   is already present on the machine.
2. foreach datastore *not* local to the Client, which has not yet seen the
   data:
        * send data to one engine with access to the datastore
        * store in file for memmap loading
3. on *all* engines, load data as memmapped file from datastore

"""
# LICENSE: Simple BSD


from collections import defaultdict
from hashlib import md5

import numpy as np
from IPython import parallel


def get_datastore_path(create_if_missing=False):
    """Path to the store folder of the engine"""
    from os.path import expanduser, join, exists
    from os import makedirs

    datastore_path = expanduser(join('~', '.pyrallel', 'datastore'))
    if create_if_missing and not exists(datastore_path):
        makedirs(datastore_path)

    return datastore_path


@parallel.util.interactive
def load_memmap(name, fname, dtype, shape):
    import numpy
    data = numpy.memmap(fname, shape=shape, dtype=dtype, mode='copyonwrite')
    globals().update({name: data})


def datastore_by_hostname(checksum):
    """Default datastore-identification function.

    Identifies 'datastores' by hostname, and stores data files
    in ~/.pyrallel/datastore.

    Parameters
    ----------

    checksum : str
        The md5 hash of the array.  Cached filenames should be
        a function of this.

    Returns
    -------

    datastore_id : str
        This should identify a locality, wrt. the data storage mechanism.
        Data is only transferred to one engine for each datastore_id.

        This implementation returns ids with the form
        "hostname:/path/to/store/folder"

    filename : path
        The filename (or url, object id, etc.) for this data to
        be stored in.  This should be a unique function of the
        checksum.

    exists : bool
        Whether the data is already available in the datastore.
    """
    import os
    import socket
    from pyrallel.broadcast import get_datastore_path
    datastore_path = get_datastore_path(create_if_missing=True)
    datapath = os.path.join(datastore_path, checksum)
    datastore_id = socket.gethostname() + ":" + datastore_path
    return datastore_id, datapath, os.path.exists(datapath)


def save_for_memmap(data, filename, flush=False):
    """Save array data as memmapped array"""
    import os.path
    import numpy

    if os.path.exists(filename):
        # file already exists, nothing to do
        return

    mm_data = numpy.memmap(filename, mode='w+', dtype=data.dtype,
                           shape=data.shape, order=data.order)
    mm_data[:] = data
    if flush:
        mm_data.flush()


def datastore_mapping(view, identify_datastore_func, checksum):
    """Generate various mappings of datastores and paths"""
    mapping = view.apply_async(identify_datastore_func, checksum).get_dict()
    local_store_id, _, __ = identify_datastore_func(checksum)

    # reverse mapping, so we have a list of engine IDs per datastore
    engines_by_datastore = defaultdict(list)
    paths = {}
    for eid, (datastore_id, path, exists) in mapping.iteritems():
        engines_by_datastore[datastore_id].append(eid)
        paths[datastore_id] = (path, exists)

    return local_store_id, engines_by_datastore, paths


def _wait(async_results):
    [ar.get() for ar in async_results]


def bcast_memmap(view, name, data,
                 identify_datastore_func=datastore_by_hostname):
    """Broadcast data as memmapped arrays on all engines in the view

    Ultimate result: a memmapped array with the contents of data
    will be stored in globals()[name] on each engine of the view.

    Efforts are made to minimize network traffic:

    * only send to one engine per datastore (host)
    * only send if data is not already present in each store

    This model assumes that several engines can run on the same node in
    the cluster and can share the same datastore (e.g. a folder on a
    node local harddisk). By sharing the same memory mapped file, engines
    lying on the same node further reduce the total amount of data loaded
    in physical memory on each node of the cluster by avoiding loading
    several copy of the same data in the process memory of each engine.

    Parameters
    ----------
    view: IPython.parallel view
        Engines that will receive the data.

    name: string
        Name of the data in the namesapce of the engines.

    data: numpy ndarray
        Data to broadcast to each engine.

    identify_datastore_func: callable
        Functions that is called on each engine to check the datastore id
        of the engine and whether that store already as a copy of the data

    """
    client = view.client

    # checksum array for filename
    checksum = md5(data).hexdigest()

    local_store_id, engines_by_datastore, paths = datastore_mapping(
        view, identify_datastore_func, checksum)

    ars = []
    mm_ars = []

    # perform push to first engine of each non-local datastore:
    for datastore_id, targets in engines_by_datastore.iteritems():
        if datastore_id != local_store_id:
            fname, exists = paths[datastore_id]
            # if file exists, nothing to do this round
            if exists:
                print "nothing to send to", datastore_id
                continue

            print "sending data to", datastore_id
            # push to first target at datastore
            e0 = client[targets[0]]

            # save to file for memmapping on other engines
            ar = e0.apply_async(save_for_memmap, data, fname)
            ars.append(ar)
            mm_ars.append(ar)

    # wait on pushes, to ensure files are ready for memmap in next step
    # this could be done without blocking the client with a barrier
    # (MPI or ZMQ or file lock, etc.)
    _wait(ars)

    # loop through datastores
    for datastore_id, targets in engines_by_datastore.iteritems():
        if datastore_id != local_store_id:
            fname, exists = paths[datastore_id]

            # load from memmapped file on engines after the first for this
            # datastore
            other = client[targets]
            ar = other.apply_async(load_memmap, name, fname,
                                   data.dtype, data.shape)
            ars.append(ar)

        else:
            if not isinstance(data, np.memmap):
                fname, exists = paths[datastore_id]
                if not exists:
                    save_for_memmap(data, fname)
            else:
                fname = data.filename
            # local engines, load from original memmapped file
            ar = client[targets].apply_async(
                load_memmap, name, fname, data.dtype, data.shape)
            ars.append(ar)

    return ars, engines_by_datastore
