"""Tools for build ensembles on distributed compute resources

Author: Olivier Grisel <olivier@ogrisel.com>
Licensed: MIT
"""

from random import Random
from copy import copy

from IPython.parallel import interactive

from pyrallel.common import TaskManager


def combine(all_ensembles):
    """Combine the sub-estimators of a group of ensembles

        >>> from sklearn.datasets import load_iris
        >>> from sklearn.ensemble import ExtraTreesClassifier
        >>> iris = load_iris()
        >>> X, y = iris.data, iris.target

        >>> all_ensembles = [ExtraTreesClassifier(n_estimators=4).fit(X, y)
        ...                  for i in range(3)]
        >>> big = combine(all_ensembles)
        >>> len(big.estimators_)
        12
        >>> big.n_estimators_
        12
        >>> big.score(X, y)
        1.0

    """
    final_ensemble = copy(all_ensembles[0])

    for ensemble in all_ensembles[1:]:
        final_ensemble.estimators_ += ensemble.estimators_

    final_ensemble.n_estimators_ = len(final_ensemble.estimators_)
    return final_ensemble


def sub_ensemble(ensemble, n_estimators, seed=None):
    """Build a new ensemble with a random subset of the sub-estimators

        >>> from sklearn.datasets import load_iris
        >>> from sklearn.ensemble import ExtraTreesClassifier
        >>> iris = load_iris()
        >>> X, y = iris.data, iris.target

        >>> big = ExtraTreesClassifier(n_estimators=10).fit(X, y)
        >>> small = sub_ensemble(big, 3)
        >>> len(small.estimators_)
        3
        >>> small.n_estimators_
        3
        >>> big.score(X, y)
        1.0

    """
    rng = Random(seed)
    final_ensemble = copy(ensemble)
    if n_estimators > len(ensemble.estimators_):
        raise ValueError(
            "Cannot sample %d estimators from ensemble of %d"
            % (n_estimators, len(ensemble.estimators_)))

    final_ensemble.estimators_ = rng.sample(
        ensemble.estimators_, n_estimators)

    final_ensemble.n_estimators_ = len(final_ensemble.estimators_)
    return final_ensemble


@interactive
def train_model(model, data_filename):
    pass


class EnsembleGrower(TaskManager):

    def __init__(self, load_balanced_view, base_model):
        self.tasks = []
        self.base_model = base_model
        self.lb_view = load_balanced_view

    def ensemble(self):
        pass


