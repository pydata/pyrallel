"""IPython.parallel helpers.

Author: Olivier Grisel <olivier@ogrisel.com>
Licensed: MIT
"""

from IPython.parallel import TaskAborted


def is_aborted(task):
    return isinstance(getattr(task, '_exception', None), TaskAborted)


class TaskManager(object):
    """Base class for managing tasks and groups of tasks"""

    def all_tasks(self, skip_aborted=True):
        all_tasks = []
        all_tasks += getattr(self, 'tasks', [])

        task_groups = getattr(self, 'task_groups', [])
        all_tasks += [t for task_group in task_groups
                        for t in task_group]

        if skip_aborted:
            all_tasks = [t for t in all_tasks if not is_aborted(t)]

        return all_tasks

    def map_tasks(self, f, skip_aborted=True):
        return map(f, self.all_tasks(skip_aborted=skip_aborted))

    def abort(self):
        for task in self.all_tasks(skip_aborted=True):
            if not task.ready():
                try:
                    task.abort()
                except AssertionError:
                    pass
        return self

    def wait(self):
        self.map_tasks(lambda t: t.wait(), skip_aborted=True)
        return self

    def completed_tasks(self):
        return [t for t in self.all_tasks(skip_aborted=True) if t.ready()]

    def completed(self):
        return sum(self.map_tasks(lambda t: t.ready(), skip_aborted=True))

    def done(self):
        return all(self.map_tasks(lambda t: t.ready(), skip_aborted=True))

    def total(self):
        return sum(self.map_tasks(lambda t: 1, skip_aborted=False))

    def progress(self):
        c = self.completed()
        if c == 0:
            return 0.0
        else:
            return float(c) / self.total()

    def elapsed(self):
        return max([t.elapsed
                    for t in self.all_tasks(skip_aborted=False)])
