import re
import shlex
from collections import namedtuple

from .helpers import sh
from .deps import FileDependency, StringDependency


class Task(namedtuple("Task", ["name", "actions", "depends", "targets",
                               "task_no"])):
    """A unit of work. 
    
    :param name: The task name; must be unique to all tasks within a runcontext.
    :type name: str or unicode

    :param actions: The actions to execute; do these and the work is done.
    :type actions: list of callable

    :param depends: The list of dependencies. 
    :type depends: list of :class:`deps.BaseDependendency`

    :param targets: The list of targets. The task must produce all of
    these to be a successfully complete task.
    :type targets: list of :class:`deps.BaseDependency`

    :param task_no: The unique task number. Ordered by declaration,
    not execution.
    :type task_no: int

    """
    pass # the class definition is just for the docstring


class RunContext(object):

    def __init__(self):
        self.task_counter = itertools.count(1)


    def do(self, cmd, track_cmd=True, track_binaries=True):
        """Create and add a :class:`runcontext.Task` to the runcontext using a
        convenient, shell-like syntax. 

        To explicitly mark task targets, wrap filenames within ``cmd``
        with ``@{}``. Similarly, wrap dependencies with ``#{}``. The
        literal ``@{}`` and ``#{}`` characters will be stripped out
        prior to execution by the shell.

        Below are some examples of using ``do``:

        .. code:: python

            from anadama import RunContext

            ctx = RunContext()
            ctx.do("wget -qO- checkip.dyndns.com > @{my_ip.txt}")
            ctx.do(r"sed 's|.*Address: \(.*[0-9]\)<.*|\1|' #{my_ip.txt} > @{ip.txt}")
            ctx.do("whois $(cat #{ip.txt}) > @{whois.txt}")
            ctx.go()


        By default, changes made to ``cmd`` are tracked; any changes
        to ``cmd`` will cause this task to be rerun. Set ``track_cmd``
        to False to disable this behavior.

        Also by default, AnADAMA tries to discover pre-existing, small
        files in ``cmd`` and treat them as dependencies. This feature
        is intended to automatically track the scripts and binaries
        used in ``cmd``. Thus, this task will be re-run if any of the
        binaries or scripts change. Set ``track_binaries`` to False to
        disable this behavior.

        :param cmd: The shell command to add to the runcontext. Wrap a
          target filename in ``@{}`` and wrap a dependency filename in
          `#{}``.
        :type cmd: str

        :keyword track_cmd: Set to False to not track changes to ``cmd``.
        :type track_cmd: bool
        
        :keyword track_binaries: Set to False to not discover files
          within ``cmd`` and treat them as dependencies.
        :type track_binaries: bool

        """

        targs = _parse_wrapper(cmd, metachar="@")
        deps = _parse_wrapper(cmd, metachar="#")
        sh_cmd = re.sub(r'[@#]{([^{}]+)}', r'\1', cmd)
        if track_cmd:
            deps.append(StringDependency(sh_cmd))
        if track_binaries:
            deps.extend(discover_binaries(sh_cmd))
        return self.add_task(sh_cmd, deps, targs)


    def add_task(self, actions=None, depends=None, targets=None, name=None):
        """Create and add a :class:`runcontext.Task` to the runcontext.  This
        function can be used as a decorator to set a function as the
        sole action.
        
        :param actions: The actions to be performed to complete the
          task. Strings or lists of strings are interpreted as shell
          commands. If given just a string or just a callable, this
          method treats it as a one-item list of the string or
          callable.  
        :type actions: str or callable or list of str or
          list of callable

        :param depends: The dependencies of the task. The task must
          have these dependencies before executing the
          actions. Strings or lists of strings are interpreted as
          filenames and turned into objects of type
          :class:`deps.FileDependency`. If given just a string or just
          a :class:`deps.BaseDependency`, this method treats it as a
          one-item list of the argument provided.
        :type depends: str or :class:`deps.BaseDependency` or list of
          str or list of :class:`deps.BaseDependency`

        :param targets: The targets of the task. The task must produce
          these targets after executing the actions to be considered
          as "success". Strings or lists of strings are interpreted as
          filenames and turned into objects of type
          :class:`deps.FileDependency`. If given just a string or just
          a :class:`deps.BaseDependency`, this method treats it as a
          one-item list of the argument provided.
        :type targets: str or :class:`deps.BaseDependency` or list of
          str or list of :class:`deps.BaseDependency`

        :param name: A name for the task. Task names must be unique
          within a run context.
        :type name: str

        """

        acts = _build_actions(actions)
        deps = _build_depends(depends)
        targs = _build_targets(targets)
        task_no = next(self.task_counter)
        name = _build_name(name, task_no)
        if acts is None: # must be a decorator
            def finish_add_task(fn):
                the_task = Task(name, [fn], deps, targs, task_no)
                self._add_task(the_task)
            return finish_add_task
        else:
            the_task = Task(name, acts, deps, targs, task_no)
            self._add_task(the_task)
            return the_task


    def go(self):
        """Kick off execution of all previously configured tasks."""
        pass

    def _add_task(self, task):
        pass


def _build_actions(actions):
    actions = _sugar_list(actions)
    return [ a if callable(a) else sh(a) for a in actions ]
        

def _build_depends(depends):
    depends = _sugar_list(depends)
    return [ d if isinstance(d, basestring) else FileDependency(d)
             for d in depends ]


def _build_targets(targets):
    targets = _sugar_list(targets)
    return [ t if isinstance(t, basestring) else FileDependency(t)
             for t in depends ]


def _build_name(name, task_no):
    if not name:
        return "Step "+str(task_no)
    else:
        return name



def _sugar_list(x):
    """Turns just a single thing into a list containing that single thing.

    """

    if not hasattr(x, "__iter__") or isinstance(x, basestring):
        return [x]
    else:
        return x


def _parse_wrapper(s, metachar):
    """search through string ``s`` and find terms wrapped in curly braces
    and a metacharacter ``metachar``. Intended for use with
    :meth:`runcontext.RunContext.do`.
    """

    return re.findall(metachar+r'{[^{}]+}', s)


def discover_binaries(s):
    """Search through string ``s`` and find all existing files smaller
    than 10MB. Return those files as a list of objects of type
    :class:`deps.FileDependency`.
    """

    deps = list()
    for term in shlex.split(s):
        try:
            dep = ExecutableDependency(term)
        except ValueError:
            continue
        if os.stat(dep.name).st_size < 1<<20:
            deps.append(dep)

    return deps
