import itertools

class TaskContainer(list):
    """Contains tasks. Tasks can be accessed by task_no or by name"""

    def __init__(self, *args, **kwargs):
        self.by_name = dict()
        return super(TaskContainer, self).__init__(*args, **kwargs)


    def _update(self, task):
        self.by_name[task.name] = task


    def append(self, task):
        self._update(task)
        return super(TaskContainer, self).append(task)


    def extend(self, iterable):
        a, b = itertools.tee(iterable)
        for task in a:
            self._update(task)
        return super(TaskContainer, self).extend(b)


    def __setitem__(self, key, task):
        self._update(task)
        return super(TaskContainer, self).__settask__(key, task)


    def __getitem__(self, key):
        if isinstance(key, basestring):
            return self.by_name[key]
        return super(TaskContainer, self).__getitem__(key)


    def __contains__(self, item):
        if isinstance(item, basestring):
            return item in self.by_name
        return super(TaskContainer, self).__contains__(item)
