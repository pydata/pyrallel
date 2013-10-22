from copy import copy


def combine_tree_ensembles(ensembles):
    final_ensemble = copy(ensembles[0])
    for ensemble in ensembles[1:]:
        final_ensemble.estimators_ += ensemble.estimators_
    final_ensemble.n_estimators = len(final_ensemble.estimators_)
    return final_ensemble
