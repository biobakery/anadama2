import os

from .util import _adler32

class BaseDependency(object):
    def compare(self):
        """Produce the iterator that is used to determine if this dependency
        has changed.

        :returns: iterator

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
        :type s: str
        """
        self.fname = fname

    def compare(self):
        yield self.fname
        yield os.stat(self.fname).st_size
        yield _adler32(self.fname)

