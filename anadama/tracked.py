import os
import logging
import itertools
import traceback
from glob import glob
from operator import eq, itemgetter
from collections import defaultdict

from .util import _adler32, find_on_path, sh, HasNoEqual
from .util import istask

logger = logging.getLogger(__name__)
_singleton_idx = defaultdict(dict)
first = itemgetter(0)

def auto(x):
    """Translate a string, function or task into the appropriate subclass
    of :class:`anadama.tracked.Base`. Tildes and shell
    variables are expanded using :func:`os.path.expanduser` and
    :func:`os.path.expandvars`. If that's not your game, use
    :class:`anadama.tracked.TrackedDirectory` or
    :class:`anadama.tracked.TrackedFile` as appropriate. The current
    mapping is as follows:

    - Subclasses of :class:`anadama.tracked.Base` are returned as is

    - Strings ending in '/' ``->`` :class:`anadama.tracked.TrackedDirectory`

    - Strings not ending in '/' ``->`` :class:`anadama.tracked.TrackedFile`

    - Instances of subclasses of :class:`anadama.Task` are handled
      specially by :meth:`anadama.workflow.Workflow.add_task` and
      are returned as is

    - Functions or other callables ``->`` :class:`anadama.tracked.TrackedFunction`

    :param x: The object to be translated into a dependency object

    """

    if isinstance(x, basestring):
        return _autostring(x)
    elif istask(x):
        return x
    elif isinstance(x, Base):
        return x
    elif callable(x):
        # no explicit key is given, so I have to make one up
        key = "Function ({}) <{}>".format(x.__name__, id(x))
        return TrackedFunction(key, x)
    else:
        raise ValueError(
            "Not sure how to make `{}' into a dependency".format(x))


def _autostring(s):
    s = os.path.expanduser(os.path.expandvars(s))
    if s.endswith('/'):
        return TrackedDirectory(s)
    else:
        return TrackedFile(s)


def any_different(ds, backend):
    """Determine whether any dependencies have changed since last save.

    :param ds: The dependencies in question
    :type ds: instances of any :class:`anadama.tracked.Base` subclass

    :param backend: Backend to query past results of a dependency object
    :type backend: instances of any :class:`anadama.backends.BaseBackend` subclass
    """
    isdebug = logger.isEnabledFor(logging.DEBUG)
    
    for dep in ds:
        past_dep_compare = backend.lookup(dep)
        if not past_dep_compare:
            if isdebug:
                logger.debug("Dep `%s' of type %s changed: "
                             "wasn't previously saved in backend",
                             dep._key, type(dep))
            return True

        compares = itertools.izip_longest(
            dep.compare(), past_dep_compare, fillvalue=HasNoEqual())
        the_same = itertools.starmap(eq, compares)
        try:
            is_different = not all(the_same)
        except:
            if isdebug:
                logger.debug("Dep `%s' of type %s changed: "
                             "hit an exception when running .compare()",
                             dep._key, type(dep))
            return True
        if is_different:
            if isdebug:
                logger.debug("Dep `%s' of type %s changed: "
                             ".compare() different since last save",
                             dep._key, type(dep))
            return True
    return False



class DependencyIndex(object):

    """Keeps track of what dependencies belong to what class and provides
    efficient lookups of what task produces what dependency. Use
    ``DependencyIndex[dependency_obj]`` to get the task that makes that
    dependency.

    """

    def __init__(self):
        self._taskidx = defaultdict(dict)
        self._taskidx.update((k, dict()) for k in _singleton_idx.iterkeys())

        
    def link(self, dep, task_or_none):
        """Link a dependency to a task. Used later for lookups with
        __getitem__.

        :param dep: The dependency to track
        :type dep: subclass of :class:`anadama.tracked.Base`

        :param task_or_none: The task that's supposed to create this
          dependency. Use None if the dependency isn't created by a
          task but exists prior to any tasks running.
        :type task_or_none: :class:`anadama.Task` or None

        """
        self._taskidx[dep.__class__.__name__][dep._key] = task_or_none


    def __contains__(self, dep):
        return dep._key in self._taskidx[dep.__class__.__name__]


    def __getitem__(self, dep):
        if not isinstance(dep, Base):
            raise TypeError(
                "DependencyIndex can only use subclasses of"
                " Base to perform lookups."
                " Received type `{}'".format(type(dep))
            )
        deptype = dep.__class__.__name__
        dep = _singleton_idx[deptype][dep._key]
        return self._taskidx[deptype][dep._key]



class Base(object):

    """The Dependency object is the tool for specifying task father-child
    relationships. Dependency objects can be used in either
    ``targets`` or ``depends`` arguments of
    :meth:`anadama.workflow.Workflow.add_task`. Often, these targets or
    dependencies are specified by strings or :class:`anadama.Task`
    objects and instantiated into the appropriate Base
    subclass with :func:`anadama.tracked.auto`; this behavior depends on the
    arguments to :meth:`anadama.workflow.Workflow.add_task`.

    A dependency of the same name will be defined multiple times in
    the normal use of AnADAMA. To make it such that many calls of a
    dependency constructor or use of the same argument to
    :func:`anadama.tracked.auto` result in the same dependency instance being
    returned, a little bit of black magic involving
    :meth:`anadama.tracked.Base.__new__` is required. The price for
    such magics is that subclasses of Base must define the
    ``init`` method to initialize the dependency, instead of the more
    commonly used ``__init__`` method. The ``init`` method is only
    called once per dependency, while ``__init__`` is called every
    time any Base sublcasses are
    instantiated. Base subclasses must also define a
    ``key()`` staticmethod. The ``key()`` staticmethod is used to
    lookup already existing instances of that dependency; return
    values from the ``key()`` method must be unique to that
    dependency.

    Unrelated to the quasi-singleton magics above, sublcasses of
    Base must define a ``compare()`` method. See
    :meth:`anadama.tracked.Base.compare` for more documentation.

    """

    must_preexist = True

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


    def __getnewargs__(self):
        return (self._key,)
            

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


    def __hash__(self):
        return hash(self._key)



class TrackedString(Base):

    must_preexist = False

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



KVDEPSEPARATOR = ":"
class TrackedVariable(Base):
    must_preexist = False

    def __new__(cls, namespace, key, val):
        global _singleton_idx
        real_key = cls.key(namespace, key, val)
        maybe_exists = _singleton_idx[cls.__name__].get(real_key, None)
        if maybe_exists:
            return maybe_exists
        else:
            _singleton_idx[cls.__name__][real_key] = dep = object.__new__(cls)
            dep._key = real_key
            dep.init(namespace, key, val)
            return dep


    def init(self, namespace, k, v):
        self.val = str(v)

    def compare(self):
        yield self.val

    @staticmethod
    def key(ns, k, v):
        return KVDEPSEPARATOR.join(map(str, (ns,k)))

    def __getnewargs__(self):
        ns, k = self._key.split(KVDEPSEPARATOR)
        return (ns, k, self.val)

    def __str__(self):
        return str(self.val)



class TrackedFile(Base):
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
        self.fname = self.__class__.key(fname)


    def compare(self):
        stat = os.stat(self.fname)
        yield stat.st_size
        yield stat.st_mtime
        yield _adler32(self.fname)


    @staticmethod
    def key(fname):
        return os.path.abspath(fname)


    def __str__(self):
        return self.fname



class HugeTrackedFile(TrackedFile):
    """Track a large file. Large being large enough that you don't want to
    read through the entire file to create a checksum of its contents.

    This dependency class is the fastest option for tracking file
    changes for large files. Speed comes at the cost of safety; only
    the size and the modification time are used to determine
    freshness.

    """

    def compare(self):
        stat = os.stat(self.fname)
        yield stat.st_size
        yield stat.st_mtime



class Container(object):
    """Track a collection of small strings. This is useful for rerunning
    tasks based on whether a flag has changed when running a
    command. Using :class:`anadama.tracked.TrackedString` for small
    strings can run into collisions between workflows that use the
    same backend. Consider using ``logging =
    TrackedString("debug")`` in script_a.py and ``logging =
    TrackedString("warning")`` in script_b.py. If you change
    script_a.py to ``logging = TrackedString("warning")`` after
    running script_b.py, script_a.py won't rerun tasks that depend on
    the TrackedString assigned to ``logging``.

    This class solves TrackedString collisions by prepending a
    user-provided (or auto-generated) namespace to each key-value pair
    in the collection. The auto-generated namespace is unique to the
    script or module creating the Container, so expect to have
    a bunch of tasks rerun if you rename a module or script between
    runs.

    .. code:: python

      >>> import anadama.tracked
      >>> conf = anadama.tracked.Container(alpha="5", beta=2)
      >>> conf.alpha
      <anadama.tracked.TrackedVariable object at 0x7f66445fa490> 
      >>> str(conf.alpha)
      '5'
      >>> str(conf['beta'])
      '2'
      >>> conf.beta = 7
      >>> conf.beta
      <anadama.tracked.TrackedVariable object at 0x7f66445fa4d0>
      >>> str(conf.beta)
      '7'
    
    """

    def __init__(self, namespace=None, **kwds):
        self.__dict__['_ns'] = self.__class__.key(namespace)
        logger.debug("Creating %s with namespace %s",
                     self.__class__.__name__, self._ns)
        self.__dict__['_d'] = dict()
        for k, v in kwds.iteritems():
            self._d[k] = TrackedVariable(self._ns, k, v)


    @staticmethod
    def key(namespace=None):
        if not namespace:
            anamodpath = os.path.dirname(__file__)
            for stackfile in reversed(map(first, traceback.extract_stack())):
                if not stackfile.startswith(anamodpath):
                    namespace = stackfile
                    break
        return namespace

    def items(self):
        return list(self._d.itervalues())

    def compare(self):
        for k in self._d:
            for item in self[k].compare():
                yield item


    def __getattr__(self, key):
        return self._d[key]

    __getitem__ = __getattr__

    def __setattr__(self, key, val):
        if key in self._d:
            global _singleton_idx
            dep = self._d[key]
            dep.val = val
            dep._key = TrackedVariable.key(self._ns, key, val)
            _singleton_idx[dep.__class__.__name__][dep._key] = dep
        else:
            self._d[key] = TrackedVariable(self._ns, key, val)

    __setitem__ = __setattr__

    def __hash__(self):
        return hash(tuple(self._d.iteritems()))



class TrackedDirectory(TrackedFile):

    """Track a directory. A directory is considered changed if it's
    removed, the modify time has changed, the list of files within the
    directory has changed, or if any of the files within the directory
    have changed size or modify times.

    """

    def compare(self):
        stat = os.stat(self.fname)
        yield stat.st_size
        yield stat.st_mtime
        contained = list(sorted(os.listdir(self.fname)))
        yield hash(tuple(contained))
        for item in contained:
            stat = os.stat(os.path.join(self.fname, item))
            yield stat.st_size
            yield stat.st_mtime



class TrackedFilePattern(TrackedFile):
    """Track several files according to a bash-style globbing
    pattern. Uses :func:`glob.glob` under the hood.  A Glob is
    considered changed if the names of the matched files changes, or
    any of the matched files change in size or modify time.

    """

    def compare(self):
        fs = glob(self.fname)
        fs = list(sorted(fs))
        yield hash(tuple(fs))
        for f in fs:
            stat = os.stat(f)
            yield stat.st_size
            yield stat.st_mtime



class TrackedExecutable(Base):
    """Track a script or binary executable."""

    def init(self, name, version_cmd=None):
        """Initialize the dependency.

        :param name: Name of a script on the shell $PATH or name of
          the file to track
        :type name: str
        
        :keyword version_cmd: Shell string to execute to get the
          binary or script's version
        
        """

        self.fname = self.__class__.key(name)
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
            p = name
        else:
            p = find_on_path(name)
            if not p:
                raise ValueError(
                    "Unable to find binary or script `{}'".format(name))
        return p

    def __str__(self):
        return self.fname



class TrackedFunction(Base):

    """Useful for things like database lookups or API calls. The function
    must return a hashable type. For a tiered comparison method like
    that seen in :class:`anadama.tracked.TrackedFile`, it's best to create
    your own subclass of Base and override the ``compare()``
    method.

    """

    must_preexist = False

    def init(self, key, fn):
        self.fn = fn


    def compare(self):
        yield self.fn()


    @staticmethod
    def key(key):
        return key



_cached_dep_classes = (
    Base,       TrackedString,
    TrackedFile,       HugeTrackedFile,
    TrackedExecutable, TrackedFunction,
    TrackedFilePattern,       TrackedDirectory,
    TrackedVariable
)
for cls in _cached_dep_classes:
    _singleton_idx[cls.__name__] = dict()
del cls