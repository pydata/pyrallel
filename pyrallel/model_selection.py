"""Utilities for Parallel Model Selection with IPython

Author: Olivier Grisel <olivier@ogrisel.com>
Licensed: MIT
"""
from time import sleep
from collections import namedtuple
import os

from IPython.parallel import interactive
from IPython.display import clear_output
from scipy.stats import sem
import numpy as np

from sklearn.utils import check_random_state
from sklearn.grid_search import ParameterGrid

from pyrallel.common import TaskManager
from pyrallel.common import is_aborted
from pyrallel.mmap_utils import warm_mmap_on_cv_splits
from pyrallel.mmap_utils import persist_cv_splits




@interactive
def compute_evaluation(model, cv_split_filename, params=None,
                       train_size=1.0, mmap_mode='r',
                       scoring=None, dump_model=False,
                       dump_predictions=False, dump_folder='.'):
    """Evaluate a model on a given CV split"""
    # All module imports should be executed in the worker namespace to make
    # possible to run an an engine node.
    from time import time
    from sklearn.externals import joblib

    X_train, y_train, X_test, y_test = joblib.load(
        cv_split_filename, mmap_mode=mmap_mode)

    # Slice a subset of the training set for plotting learning curves
    if train_size <= 1.0:
        # Assume that train_size is an relative fraction of the number of
        # samples
        n_samples_train = int(train_size * X_train.shape[0])
    else:
        # Assume that train_size is an absolute number of samples
        n_samples_train = int(train_size)
    X_train = X_train[:n_samples_train]
    y_train = y_train[:n_samples_train]

    # Configure the model
    if model is not None:
        model.set_params(**params)

    # Fit model and measure training time
    tick = time()
    model.fit(X_train, y_train)
    train_time = time() - tick

    # Compute score on training set
    train_score = model.score(X_train, y_train)

    # Compute score on test set
    test_score = model.score(X_test, y_test)

    # Wrap evaluation results in a simple tuple datastructure
    return (test_score, train_score, train_time,
            train_size, params)


# Named tuple to collect evaluation results
Evaluation = namedtuple('Evaluation', (
    'validation_score',
    'train_score',
    'train_time',
    'train_fraction',
    'parameters'))


class RandomizedGridSeach(TaskManager):
    """"Async Randomized Parameter search."""

    def __init__(self, load_balanced_view, random_state=0):
        self.task_groups = []
        self.lb_view = load_balanced_view
        self.random_state = random_state
        self._temp_files = []

    def reset(self):
        # Abort any other previously scheduled tasks
        self.abort()

        # Schedule a new batch of evaluation tasks
        self.task_groups, self.all_parameters = [], []

        # Collect temporary files:
        for filename in self._temp_files:
            os.unlink(filename)
        del self._temp_files[:]

    def launch_for_splits(self, model, parameter_grid, cv_split_filenames,
                          pre_warm=True, collect_files_on_reset=False):
        """Launch a Grid Search on precomputed CV splits."""

        # Abort any existing processing and erase previous state
        self.reset()
        self.parameter_grid = parameter_grid

        # Mark the files for garbage collection
        if collect_files_on_reset:
            self._temp_files.extend(cv_split_filenames)

        # Warm the OS disk cache on each host with sequential reads instead
        # of having concurrent evaluation tasks compete for the the same host
        # disk resources later.
        if pre_warm:
            warm_mmap_on_cv_splits(self.lb_view.client, cv_split_filenames)

        # Randomize the grid order
        random_state = check_random_state(self.random_state)
        self.all_parameters = list(ParameterGrid(parameter_grid))
        random_state.shuffle(self.all_parameters)

        for params in self.all_parameters:
            task_group = []

            for cv_split_filename in cv_split_filenames:
                task = self.lb_view.apply(
                    compute_evaluation,
                    model, cv_split_filename, params=params)
                task_group.append(task)

            self.task_groups.append(task_group)

        # Make it possible to chain method calls
        return self

    def launch_for_arrays(self, model, parameter_grid, X, y, n_cv_iter=5,
                          train_size=None, test_size=0.25, pre_warm=True,
                          folder=".", name=None, random_state=None):
        cv_split_filenames = persist_cv_splits(
            X, y, n_cv_iter=n_cv_iter, train_size=train_size,
            test_size=test_size, name=name, folder=folder,
            random_state=random_state)
        return self.launch_for_splits(
            model, parameter_grid, cv_split_filenames, pre_warm=pre_warm,
            collect_files_on_reset=True)

    def find_bests(self, n_top=5):
        """Compute the mean score of the completed tasks"""
        mean_scores = []

        for params, task_group in zip(self.all_parameters, self.task_groups):
            evaluations = [Evaluation(*t.get())
                           for t in task_group
                           if t.ready() and not is_aborted(t)]

            if len(evaluations) == 0:
                continue
            val_scores = [e.validation_score for e in evaluations]
            train_scores = [e.train_score for e in evaluations]
            mean_scores.append((np.mean(val_scores), sem(val_scores),
                                np.mean(train_scores), sem(train_scores),
                                params))

        return sorted(mean_scores, reverse=True)[:n_top]

    def report(self, n_top=5):
        bests = self.find_bests(n_top=n_top)
        output = "Progress: {0:02d}% ({1:03d}/{2:03d})\n".format(
            int(100 * self.progress()), self.completed(), self.total())
        for i, best in enumerate(bests):
            output += ("\nRank {0}: validation: {1:.5f} (+/-{2:.5f})"
                       " train: {3:.5f} (+/-{4:.5f}):\n {5}".format(
                       i + 1, *best))
        return output

    def __repr__(self):
        return self.report()

    def boxplot_parameters(self, display_train=False):
        """Plot boxplot for each parameters independently"""
        import pylab as pl
        results = [Evaluation(*task.get())
                   for task_group in self.task_groups
                   for task in task_group
                   if task.ready() and not is_aborted(task)]

        n_rows = len(self.parameter_grid)
        pl.figure()
        grid_items = self.parameter_grid.items()
        for i, (param_name, param_values) in enumerate(grid_items):
            pl.subplot(n_rows, 1, i + 1)
            val_scores_per_value = []
            train_scores_per_value = []
            for param_value in param_values:
                train_scores = [r.train_score for r in results
                                if r.parameters[param_name] == param_value]
                train_scores_per_value.append(train_scores)

                val_scores = [r.validation_score for r in results
                              if r.parameters[param_name] == param_value]
                val_scores_per_value.append(val_scores)

            widths = 0.25
            positions = np.arange(len(param_values)) + 1
            offset = 0
            if display_train:
                offset = 0.175
                pl.boxplot(
                    train_scores_per_value, widths=widths,
                    positions=positions - offset)

            pl.boxplot(
                val_scores_per_value, widths=widths,
                positions=positions + offset)

            pl.xticks(np.arange(len(param_values)) + 1, param_values)
            pl.xlabel(param_name)
            pl.ylabel("Val. Score")

    def monitor(self, plot=False):
        try:
            while not self.done():
                self.lb_view.spin()
                if plot:
                    import pylab as pl
                    pl.clf()
                    self.boxplot_parameters()
                clear_output()
                print(self.report())
                if plot:
                    pl.show()
                sleep(1)
        except KeyboardInterrupt:
            print("Monitoring interrupted.")
