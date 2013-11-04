"""IPython.parallel helpers.

Author: Olivier Grisel <olivier@ogrisel.com>
Licensed: MIT
"""

from IPython.parallel import TaskAborted
from IPython.parallel import interactive


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
        all_tasks = self.all_tasks(skip_aborted=False)
        if not all_tasks:
            return 0.0
        return max([t.elapsed for t in all_tasks])


def get_host_view(client):
    """Return an IPython parallel direct view with one engine per host."""
    # First step: query cluster to fetch one engine id per host
    all_engines = client[:]

    @interactive
    def hostname():
        import socket
        return socket.gethostname()

    hostnames = all_engines.apply(hostname).get_dict()
    one_engine_per_host = dict((hostname, engine_id)
                               for engine_id, hostname
                               in hostnames.items())
    return client[one_engine_per_host.values()]
