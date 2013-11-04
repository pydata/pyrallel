"""Utilities for Memory Mapping cross validation folds

Author: Olivier Grisel <olivier@ogrisel.com>
Licensed: MIT
"""
import os
from IPython.parallel import interactive

from pyrallel.common import get_host_view


@interactive
def persist_cv_splits(X, y, name=None, n_cv_iter=5, suffix="_cv_%03d.pkl",
                      train_size=None, test_size=0.25, random_state=None,
                      folder='.'):
    """Materialize randomized train test splits of a dataset."""
    from sklearn.externals import joblib
    from sklearn.cross_validation import ShuffleSplit
    import os
    import uuid

    if name is None:
        name = uuid.uuid4().get_hex()

    cv = ShuffleSplit(X.shape[0], n_iter=n_cv_iter,
                      test_size=test_size, random_state=random_state)
    cv_split_filenames = []

    for i, (train, test) in enumerate(cv):
        cv_fold = (X[train], y[train], X[test], y[test])
        cv_split_filename = os.path.join(folder, name + suffix % i)
        cv_split_filename = os.path.abspath(cv_split_filename)
        joblib.dump(cv_fold, cv_split_filename)
        # TODO: make it possible to ship the CV folds on each host for
        # non-NFS setups.
        cv_split_filenames.append(cv_split_filename)

    return cv_split_filenames


def warm_mmap(client, data_filenames, host_view=None):
    """Trigger a disk load on all the arrays data_filenames.

    Assume the files are shared on all the hosts using NFS or
    have been previously been dumped there with the host_dump function.
    """
    if host_view is None:
        host_view = get_host_view(client)

    # Second step: for each data file and host, mmap the arrays of the file
    # and trigger a sequential read of all the arrays' data
    @interactive
    def load_in_memory(filenames):
        from sklearn.externals import joblib
        for filename in filenames:
            arrays = joblib.load(filename, mmap_mode='r')
            for array in arrays:
                if hasattr(array, 'max'):
                    array.max()  # trigger the disk read

    data_filenames = [os.path.abspath(f) for f in data_filenames]
    host_view.apply_sync(load_in_memory, data_filenames)


# Backward compat
warm_mmap_on_cv_splits = warm_mmap


def _missing_file_engine_ids(view, filename):
    """Return the list of engine ids where filename does not exist"""

    @interactive
    def missing(filename):
        import os
        return not os.path.exists(filename)

    missing_ids = []
    for id_, is_missing in view.apply(missing, filename).get_dict().items():
        if is_missing:
            missing_ids.append(id_)
    return missing_ids


def host_dump(client, payload, target_filename, host_view=None, pre_warm=True):
    """Send payload to each host and dump it on the filesystem

    Nothing is done in case the file already exists.

    The payload is shipped only once per node in the cluster.

    """
    if host_view is None:
        host_view = get_host_view(client)

    client = host_view.client

    @interactive
    def dump_payload(payload, filename):
        from sklearn.externals import joblib
        import os
        folder = os.path.dirname(filename)
        if not os.path.exists(folder):
            os.makedirs(folder)
        return joblib.dump(payload, filename)

    missing_ids = _missing_file_engine_ids(host_view, target_filename)
    if missing_ids:
        first_id = missing_ids[0]

        # Do a first dispatch to the first node to avoid concurrent write in
        # case of shared filesystem
        client[first_id].apply_sync(dump_payload, payload, target_filename)

        # Refetch the list of engine ids where the file is missing
        missing_ids = _missing_file_engine_ids(host_view, target_filename)

        # Restrict the view to hosts where the target data file is still
        # missing for the final dispatch
        client[missing_ids].apply_sync(dump_payload, payload, target_filename)

    if pre_warm:
        warm_mmap(client, [target_filename], host_view=host_view)
