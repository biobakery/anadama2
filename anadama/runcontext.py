import os
import re
import shlex
import itertools
import multiprocessing
from operator import attrgetter, eq
from collections import deque

import networkx as nx

from . import Task
from . import deps
from . import reporters
from . import runners
from . import backends
from .helpers import sh, parse_sh
from .util import matcher, HasNoEqual
from .pickler import cloudpickle

MAX_QSIZE = 1000

class RunFailed(ValueError):
    pass


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
            self.already_exists(sh_cmd)
            deps.append(deps.StringDependency(sh_cmd))
        if track_binaries:
            for binary in discover_binaries(sh_cmd):
                self.already_exists(binary)
                deps.append(binary)
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


    def go(self, run_them_all=False, dry_run=False, quit_early=False,
           runner=None, reporter=None, storage_backend=None,
           processes=1):
        """Kick off execution of all previously configured tasks. """

        self._reporter.started()
        self.failed_tasks = failed = set()
        self.completed_tasks = done = set()

        wqueue = multiprocessing.Queue(MAX_QSIZE)
        rqueue = multiprocessing.Queue(MAX_QSIZE)
        self._reporter = reporter or reporters.default(self)
        _runner = runner or runners.default()
        _backend = storage_backend or backends.default()
        workers = [
            multiprocessing.Process(target=_runner, args=(wqueue, rqueue))
            for _ in range(processes)
        ]

        task_idxs = nx.algorithms.dag.topological_sort(self.dag)
        if not run_them_all:
            task_idxs = self._filter_skipped_tasks(task_idxs, _backend)
        task_idxs = deque(task_idxs)
        
        while len(self.tasks) < len(failed)+len(done):
            for _ in range(min(MAX_QSIZE, len(task_idxs))):
                task_idx = task_idxs.popleft()
                undone = set(self.dag.predecessors()).difference(done+failed)
                if undone:
                    task_idxs.append(task_idx)
                    break # come back again later
                try:
                    pkl = cloudpickle.dumps(self.tasks[task_idx])
                except Exception as e:
                    msg = ("Unable to serialize task `{}'. "
                           "Original error was `{}'.")
                    raise ValueError(msg.format(self.tasks[task_idx], e))
                wqueue.put(pkl)

            while True:
                try:
                    result = rqueue.get()
                    self._handle_task_result(result, _backend)
                    rqueue.task_done()
                except (SystemExit, KeyboardInterrupt, Exception):
                    for worker in workers:
                        worker.terminate()
                    raise
                except multiprocessing.queues.Empty:
                    break
        wqueue.join()
        rqueue.join()
        self._handle_finished()


    def _handle_task_result(self, result, backend):
        self._reporter.task_done(result.task_no)
        if result.error:
            self.failed_tasks.add(result.task_no)
            self._reporter.task_failed(result)
        else:
            backend.save(result.dep_keys, result.dep_compares)
            self.completed_tasks.add(result.task_no)
            self._reporter.task_completed(result)


    def _handle_finished(self):
        self._reporter.finished()
        if any(self.failed_tasks):
            raise RunFailed()


    def _filter_skipped_tasks(self, task_idxs, backend):
        ret = list()
        for idx in task_idxs:
            try:
                should_skip = self._should_skip_task(idx, backend)
            except:
                should_skip = False
            if should_skip:
                self._handle_skipped_task(idx)
            else:
                ret.append(idx)
        return ret

    
    def _should_skip_task(self, task_no, backend):
        task = self.tasks[task_no]
        if not task.targets:
            return False

        for targ in task.targets:
            past_targ_compare = backend.lookup(targ)
            if not past_targ_compare:
                return False

            compares = itertools.izip_longest(
                targ.compare(), past_targ_compare, HasNoEqual())
            the_same = itertools.starmap(eq, compares)
            if not all(the_same):
                return False
        return True


    def _handle_skipped_task(self, task_no):
        self.completed_tasks.add(task_no)
        self._reporter.task_skipped(task_no)


    def already_exists(self, *depends):
        """Declare a dependency as pre-existing. That means that no task
        creates these dependencies; they're already there before any
        tasks run.

        :param *depends: One or many dependencies to mark as pre-existing.
        :type *depends: any argument recognized by :func:`deps.auto`

        """

        ds = map(deps.auto, depends)
        for dep in ds:
            self._depidx.link(dep, None)


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
                # check to see if the dependency exists but doesn't
                # link to a task. This would happen if someone defined
                # a preexisting dependency
                if parent_task is not None:
                    self.dag.add_edge(parent_task.task_no, task.task_no)
        for targ in task.targets: 
            # add targets to the DependencyIndex after looking up
            # dependencies for the current task. Hopefully this avoids
            # circular references
            self._depidx.link(targ, task)
                

    def _handle_nosuchdep(self, dep, task):
        self.tasks.pop()
        self.dag.remove_node(task.task_no)
        alldeps = itertools.chain.from_iterable(
            [list(t.depends) + list(t.targets) for t in self.tasks]
        )
        closest = matcher.find_match(dep, alldeps, key=attrgetter("key"))
        msg = ("Unable to find dependency of type `{}'. "
               "Perhaps you meant `{}' of type `{}'?")
        msg.format(type(dep), closest, type(closest))
        raise KeyError(msg)



def _build_actions(actions, deps, targs, use_parse_sh=True):
    actions = _sugar_list(actions)
    if use_parse_sh:
        mod = parse_sh
    else:
        mod = lambda cmd, *args, **kwargs: sh(cmd)
    return [ a if callable(a) else mod(a, deps, targs) for a in actions ]
        

def _build_depends(depends):
    depends = _sugar_list(depends)
    return map(deps.auto, depends)


def _build_targets(targets):
    targets = _sugar_list(targets)
    return map(deps.auto, targets)


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
            dep = deps.ExecutableDependency(term)
        except ValueError:
            continue
        if os.stat(dep.name).st_size < 1<<20:
            deps.append(dep)

    return deps

    
