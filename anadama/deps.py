import os

from . import Task
from .util import _adler32, find_on_path


def auto(x):
    """Translate a string, function or task into the appropriate subclass
    of :class:`deps.BaseDependency`. The current mapping is as follows:

    - Subclasses of :class:`deps.BaseDependency` are returned as is

    - Strings ``->`` :class:`deps.FileDependency`

    - Instances of subclasses of :class:`Task` ``->`` :class:`deps.TaskDependency`

    - Functions or other callables ``->`` :class:`deps.FunctionDependency`

    :param x: The object to be translated into a dependency object

    """

    if isinstance(x, BaseDependency):
        return x
    elif isinstance(x, basestring):
        return FileDependency(x)
    elif isinstance(x, Task):
        return TaskDependency(x)
    elif callable(x):
        # no explicit key is given, so I have to make one up
        key = "Function ({}) <{}>".format(x.__name__, id(x))
        return FunctionDependency(key, x)
    else:
        raise ValueError(
            "Not sure how to make `{}' into a dependency".format(x))



class DependencyIndex(object):

    # TODO: doc?

    def __contains__(self, dep):
        return dep._key in _singleton_idx[dep.__class__.__name__]



class BaseDependency(object):

    """The Dependency object is the tool for specifying task father-child
    relationships. Dependency objects can be used in either
    ``targets`` or ``depends`` arguments of
    :meth:`runcontext.RunContext.add_task`. Often, these targets or
    dependencies are specified by strings or :class:`Task`
    objects and instantiated into the appropriate BaseDependency
    subclass with :func:`deps.auto`; this behavior depends on the
    arguments to :meth:`runcontext.RunContext.add_task`.

    A dependency of the same name will be defined multiple times in
    the normal use of AnADAMA. To make it such that many calls of a
    dependency constructor or use of the same argument to
    :func:`deps.auto` result in the same dependency instance being
    returned, a little bit of black magic involving
    :meth:`deps.BaseDependency.__new__` is required. The price for
    such magics is that subclasses of BaseDependency must define the
    ``init`` method to initialize the dependency, instead of the more
    commonly used ``__init__`` method. The ``init`` method is only
    called once per dependency, while ``__init__`` is called every
    time any BaseDependency sublcasses are
    instantiated. BaseDependency subclasses must also define a
    ``key()`` staticmethod. The ``key()`` staticmethod is used to
    lookup already existing instances of that dependency; return
    values from the ``key()`` method must be unique to that
    dependency.

    Unrelated to the quasi-singleton magics above, sublcasses of
    BaseDependency must define a ``compare()`` method. See
    :meth:`deps.BaseDependency.compare` for more documentation.

    """

    def __new__(cls, key, *args, **kwargs):
        global _singleton_idx
        real_key = cls.key(key)
        maybe_exists = _singleton_idx[cls.__name__].get(real_key, None)
        if maybe_exists:
            return maybe_exists
        else:
            _singleton_idx[cls.__name__][real_key] = dep = object.__new__(cls)
            dep._key = real_key
            dep.init(key, *args, **kwargs)
            return dep
            

    def init(self, key):
        """Initialize the dependency. Only run once for each new dependency
        object with this ``key()``.

        """
        raise NotImplementedError()
        

    def compare(self):
        """Produce the iterator that is used to determine if this dependency
        has changed. This method is called twice: once before the
        task's actions have been executed for all of a task's
        dependencies and once after all of a task's actions are
        executed for each of the task's targets.

        :returns: iterator

        """
        raise NotImplementedError()


    @staticmethod
    def key(the_key):
        """Returns the unique key for retrieving this dependency from the
        storage backend and for comparing against other dependencies
        of the same type.

        :rtype: str

        """
        raise NotImplementedError()



class StringDependency(BaseDependency):

    def init(self, s):
        """
        Initialize the dependency.
        :param s: The string to keep track of
        :type s: str or unicode
        """
        self.s = s


    def compare(self):
        yield self.s


    @staticmethod
    def key(s):
        return s

    def __str__(self):
        return self.s



class FileDependency(BaseDependency):
    """Track a small file. Small being small enough that you don't mind
    that the entire file is read to create a checksum of its contents.

    This dependency class is the safest option for tracking file
    changes. Safety comes at the cost of disk IO; all file contents
    are read to create a checksum.

    """

    def init(self, fname):
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


    @staticmethod
    def key(fname):
        return fname


    def __str__(self):
        return self.fname



class TaskDependency(BaseDependency):
    """Track another task."""

    def init(self, task):
        """Initialize the dependency
        
        :param task: the task to track
        :type task: :class:`Task`
        """
        self.task = task


    def compare(self):
        return None


    @staticmethod
    def key(task):
        return task.task_no



class ExecutableDependency(BaseDependency):
    """Track a script or binary executable."""

    def init(self, name, version_cmd=None):
        """Initialize the dependency.

        :param name: Name of a script on the shell $PATH or name of
          the file to track
        :type name: str

        :keyword version_cmd: Shell string to execute to get the
        binary or script's version

        """

        self.name = self.__class__.key(name)
        self.cmd = version_cmd


    def compare(self):
        if self.cmd:
            yield sh(self.cmd)[0]
        stat = os.stat(self.fname)
        yield stat.st_size
        yield stat.st_mtime
        yield _adler32(self.fname)


    @staticmethod
    def key(name):
        if os.path.exists(name):
            return name
        else:
            p = find_on_path(name)
            if not p:
                raise ValueError(
                    "Unable to find binary or script `{}'".format(name))
            return p


    def __str__(self):
        return self.fname



class FunctionDependency(BaseDependency):

    """Useful for things like database lookups or API calls. The function
    must return a hashable type. For a tiered comparison method like
    that seen in :class:`deps.FileDependency`, it's best to create
    your own subclass of BaseDependency and override the ``compare()``
    method.

    """

    def init(self, key, fn):
        self.fn = fn


    def compare(self):
        yield self.fn()


    @staticmethod
    def key(key):
        return key
        


_singleton_idx = dict([
    (cls.__name__, dict()) for cls in (
        BaseDependency,
        StringDependency,
        FileDependency,
        TaskDependency,
        ExecutableDependency,
        FunctionDependency)
])
