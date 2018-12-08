# -*- coding: utf-8 -*-
import re
import fnmatch
import itertools

import six

from .util import matcher


class TaskContainer(list):
    """Contains tasks. Tasks can be accessed by task_no or by name"""

    def __init__(self, *args, **kwargs):
        self.by_name = dict()
        return super(TaskContainer, self).__init__(*args, **kwargs)


    def _update(self, task):
        self.by_name[task.name] = task

        
    def _get_or_search(self, key):
        if '*' in key:
            hits = list(self.search(fnmatch.translate(key)))
            if not hits:
                raise KeyError
            return hits
        return self.by_name[key]


    def search(self, q):
        return iter(val for val in self if re.search(q, val.name))


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
        return super(TaskContainer, self).__setitem__(key, task)


    def __getitem__(self, key):
        try:
            if isinstance(key, six.string_types):
                return self._get_or_search(key)
            return super(TaskContainer, self).__getitem__(key)
        except KeyError:
            msg = "Unable to find task with `{}'. Perhaps you meant `{}'?"
            m = matcher.closest(key, iter(t.name for t in self))[0][1]
            raise KeyError(msg.format(key, m))
        except IndexError:
            msg = "No task with number {}. There are only {} tasks."
            raise IndexError(msg.format(key, len(self)))
 

    def __contains__(self, item):
        if isinstance(item, six.string_types):
            if '*' in item:
                try:
                    next(self.search(fnmatch.translate(item)))
                    return True
                except StopIteration:
                    return False
            else:
                return item in self.by_name
            
        return super(TaskContainer, self).__contains__(item)
