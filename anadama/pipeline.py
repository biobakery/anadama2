import itertools

from .runcontext import RunContext
from .util import dict_to_cmd_opts, dict_to_cmd_opts_iter


class CommandOptions(object):
    def __init__(self):
        self._opts = dict()
        self._syns = dict()
        self._doc = dict()
        self._reqd = list()
        self._default = dict()
        self._counter = itertools.count()


    def add(self, names, doc=None, default=None, required=False):
        i = next(self._counter)
        self._opts[names[0]] = i
        if doc:
            self._doc[i] = doc
        if default is not None:
            self._default[i] = default
        if required:
            self._reqd.append(i)
        for n in names[1:]:
            self._syns[n] = i


    def set(self, name, value):
        i = self._get_idx(name)
        self._default[i] = value


    def get(self, name, default=None):
        try:
            i = self._get_idx(name)
        except ValueError:
            if default is not None:
                return default
            raise
        return self._default.get(i, default)


    def _get_idx(self, name):
        if name not in self._opts:
            return self._syns[name]
        return self._opts[name]


    def _defaultcheck(self):
        for i in self._reqd:
            if i not in self._default:
                msg = "required option `{}' not set"
                missing = next(iter(k for k, idx in self._opts.iteritems()
                                    if idx == i))
                raise ValueError(msg.format(missing))


    def flags(self, **kwargs):
        """ kwargs are passed to :func:`anadama.util.dict_to_cmd_opts_iter`
        
        :returns: iterable of strings
        """
        self._defaultcheck()
        kwargs['shortsep'] = None
        d = dict([(n, self._default.get(i, None))
                  for n, i in self._opts.iteritems()])
        return itertools.chain.from_iterable(dict_to_cmd_opts_iter(d, **kwargs))


    def flags_str(self, **kwargs):
        """ kwargs are passed to :func:`anadama.util.dict_to_cmd_opts_iter`

        :returns: str
        """
        self._defaultcheck()
        d = dict([(n, self._default.get(i, None))
                  for n, i in self._opts.iteritems()])
        return dict_to_cmd_opts(d, **kwargs)


    def copy(self):
        ret = self.__class__()
        ret._opts = self._opts.copy()
        ret._syns = self._syns.copy()
        ret._doc = self._doc.copy()
        ret._reqd = self._reqd[:]
        ret._default = self._default.copy()
        ret._counter = itertools.count(len(self._opts))
        return ret


class Step(object):
    name = ""

    default_config = CommandOptions()

    def __init__(self):
        self.config = self.default_config.copy()

    def input(self, item):
        raise NotImplementedError

    @classmethod
    def task(cls, *args, **kwargs):
        step = cls()
        for k, v in kwargs.iteritems():
            step.config.set(k, v)
        return step.input(*args)
        

    

class Pipeline(object):
    """The pipeline class takes in lists of files and options to create a
    new workflow """

    def __init__(self, runcontext=None):
        if not runcontext:
            runcontext = RunContext()
        self.ctx = runcontext

    def add_route(self, from_, to):
        """Add a rule to the routes table to direct files that match ``from_``
        into the entry point ``to``.

        :param from_: The regex string or callable that, when it
          returns ``True``, routes a filename into the associated
          entry point.  
        :type from_: str or callable

        """
        pass

    @property
    def steps(self):
        pass

    def cli(self, argv):
        pass
