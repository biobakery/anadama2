import re
import shlex
from collections import namedtuple

import networkx as nx

from . import matcher, Task
from .helpers import sh, parse_sh

from . import deps

class RunContext(object):

    def __init__(self):
        self.task_counter = itertools.count(1)
        self.dag = nx.DiGraph()
        self.tasks = list()
        self._depidx = deps.DependencyIndex()


    def do(self, cmd, track_cmd=True, track_binaries=True):
        """Create and add a :class:`Task` to the runcontext using a
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
        return self.add_task(sh_cmd, deps, targs, parse_sh=False)


    def add_task(self, actions=None, depends=None, targets=None,
                 name=None, interpret_deps_and_targs=True):
        """Create and add a :class:`Task` to the runcontext.  This
        function can be used as a decorator to set a function as the
        sole action.
        
        :param actions: The actions to be performed to complete the
          task. Strings or lists of strings are interpreted as shell
          commands according to :func:`runcontext.parse_sh`. If given
          just a string or just a callable, this method treats it as a
          one-item list of the string or callable.
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

        :keyword interpret_deps_and_targs: Should I use
          :func:`runcontext.parse_sh` to change ``{deps[0]}`` and
          ``{targs[0]}`` into the first item in depends and the first
          item in targets? Default is True
        :type interpret_deps_and_targs: bool

        """

        deps = _build_depends(depends)
        targs = _build_targets(targets)
        acts = _build_actions(actions, deps, targs,
                              use_parse_sh=interpret_deps_and_targs)
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
        """Actually add a task to the internal dependency data structure"""
        
        self.tasks.append(task)
        self.dag.add_node(task.task_no)
        for dep in task.depends:
            try:
                parent_task = self._depidx[dep.key()]
            except KeyError:
                self._handle_nosuchdep(self, dep, task)
            else:
                self.dag.add_edge(parent_task.task_no, task.task_no)
        
                

    def _handle_nosuchdep(self, dep, task):
        self.tasks.pop()
        self.dag.remove_node(task.task_no)



def _build_actions(actions, deps, targs, use_parse_sh=True):
    actions = _sugar_list(actions)
    if use_parse_sh:
        mod = parse_sh
    else:
        mod = lambda cmd, *args, **kwargs: sh(cmd)
    return [ a if callable(a) else mod(a, deps, targs) for a in actions ]
        

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

    
