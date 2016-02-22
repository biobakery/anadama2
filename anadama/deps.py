import os

from .util import _adler32, find_on_path

class BaseDependency(object):

    def compare(self):
        """Produce the iterator that is used to determine if this dependency
        has changed. This method is called twice: once before the
        task's actions have been executed for all of a task's
        dependencies and once after all of a task's actions are
        executed for each of the task's targets.

        :returns: iterator

        """
        raise NotImplementedError()


    def key(self):
        """Returns the unique key for retrieving this dependency from the
        storage backend

        :rtype: str

        """
        raise NotImplementedError()



class StringDependency(BaseDependency):

    def __init__(self, s):
        """
        Initialize the dependency.
        :param s: The string to keep track of
        :type s: str or unicode
        """
        self.s = s


    def compare(self):
        yield self.s


    def key(self):
        return self.s


class FileDependency(BaseDependency):
    """Track a small file. Small being small enough that you don't mind
    that the entire file is read to create a checksum of its contents.

    This dependency class is the safest option for tracking file
    changes. Safety comes at the cost of disk IO; all file contents
    are read to create a checksum.

    """

    def __init__(self, fname):
        """
        Initialize the dependency.
        :param fname: The filename to keep track of
        :type fname: str
        """
        self.fname = fname


    def compare(self):
        stat = os.stat(self.fname)
        yield stat.st_size
        yield stat.st_mtime
        yield _adler32(self.fname)


    def key(self):
        return self.fname



class TaskDependency(BaseDependency):
    """Track another task."""

    def __init__(self, task):
        """Initialize the dependency
        
        :param task: the task to track
        :type task: :class:`runcontext.Task`
        """
        self.task = task


    def compare(self):
        return None


    def key(self):
        return self.task



class ExecutableDependency(BaseDependency):
    """Track a script or binary executable."""

    def __init__(self, name, version_cmd=None):
        """Initialize the dependency.

        :param name: Name of a script on the shell $PATH or name of
          the file to track
        :type name: str

        :keyword version_cmd: Shell string to execute to get the
        binary or script's version

        """

        self.name = name
        self.cmd = version_cmd

        if os.path.exists(self.name):
            return # nothing to do
        else:
            p = find_on_path(name)
            if not p:
                raise ValueError(
                    "Unable to find binary or script `{}'".format(name))
            self.name = p


    def compare(self):
        if self.cmd:
            yield sh(self.cmd)[0]
        stat = os.stat(self.fname)
        yield stat.st_size
        yield stat.st_mtime
        yield _adler32(self.fname)


    def key(self):
        return self.name
