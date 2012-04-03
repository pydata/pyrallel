# Pyrallel - Parallel Data Analytics in Python

**Overview**: experimental project to investigate distributed computation
patterns for machine learning and other semi-interactive data analytics
tasks.

**Scope**:

- focus on small to medium dataset that fits in memory on a small
  (10+ nodes) to medium cluster (100+ nodes).

- focus on small to medium data (with data locality when possible).

- focus on CPU bound tasks (e.g. training Random Forests) while trying to
  limit disk / network access to a minimum.

- do not focus on HA / Fault Tolerance (yet).

- do not try to invent new set of high level programming abstractions
  (yet): use a low level programming model (IPython.parallel) to finely
  control the cluster elements and messages transfered and help identify
  what are the practical underlying constraints in distributed machine
  learning setting.


**Disclaimer**: the public API of this library will probably not be
stable soon as the current goal of this project is to experiment.


## Dependencies

The usual suspects: Python 2.7, NumPy, SciPy.

Fetch the development version (master branch) from:

- https://github.com/ipython/ipython

- https://github.com/scikit-learn/scikit-learn


## Patterns currently under investigation

- Broadcast numerical arrays efficiently over the nodes and make them
  available to concurrently running Python processes without making
  copies in memory using memory-mapped files.

- Asynchronous & randomized hyper-parameters search (a.k.a. Randomized Grid
  Search) for machine learning models

- Parallel implementation of online averaged models using a MPI AllReduce, for
  instance using MiniBatchKMeans on partitioned data.

- Distributed Random Forests fitting.

See the content of the `examples/` folder for more details.


## License

Simplified BSD.
