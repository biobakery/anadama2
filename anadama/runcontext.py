import os
import re
import shlex
import itertools
from operator import attrgetter
from collections import deque

import networkx as nx

from . import Task
from . import deps
from . import reporters
from . import runners
from . import backends
from .helpers import sh, parse_sh
from .util import matcher, noop, find_on_path, istask


class RunFailed(ValueError):
    pass


class RunContext(object):
    """Create a RunContext.
    
    :keyword storage_backend: Lookup and save dependency information
      from this object. If ``None`` is passed (the default), the
      default backend from :func:`anadama.backends.default` is used.
    :type storage_backend: instance of any
      :class:`anadama.backends.BaseBackend` subclass or None.

    """


    def __init__(self, storage_backend=None):
        self.task_counter = itertools.count()
        self.dag = nx.DiGraph()
        #: tasks is a list of objects of type
        #: :class:`anadama.Task`. This list is populated as new tasks
        #: are added via :meth:`anadama.runcontext.RunContext.add_task`
        #: and :meth:`anadama.runcontext.RunContext.do`
        self.tasks = list()
        #: task_results is a list of objects of type :
        #: :class:`anadama.runners.TaskResult`. This list is populated
        #: only after tasks have been run with
        #: :meth:`anadama.runcontext.RunContext.go`.
        self.task_results = list()
        self._depidx = deps.DependencyIndex()
        self.compare_cache = deps.CompareCache()
        self._pexist_task = None
        if not storage_backend:
            self._backend = storage_backend or backends.default()


    def do(self, cmd, track_cmd=True, track_binaries=True):
        """Create and add a :class:`anadama.Task` to the runcontext using a
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

        :returns: The :class:`anadama.Task` just created

        """
        targs = _parse_wrapper(cmd, metachar="@")
        ds = _parse_wrapper(cmd, metachar="#")
        sh_cmd = re.sub(r'[@#]{([^{}]+)}', r'\1', cmd)
        if track_cmd:
            d = deps.StringDependency(sh_cmd)
            self._add_do_pexist(d)
            ds.append(d)
        if track_binaries:
            for binary in discover_binaries(cmd):
                self._add_do_pexist(binary)
                ds.append(binary)

        return self.add_task(sh_cmd, depends=ds, targets=targs, name=sh_cmd,
                             interpret_deps_and_targs=False)


    def add_task(self, actions=None, depends=None, targets=None,
                 name=None, interpret_deps_and_targs=True):
        """Create and add a :class:`anadama.Task` to the runcontext.  This
        function can be used as a decorator to set a function as the
        sole action.
        
        :param actions: The actions to be performed to complete the
          task. Strings or lists of strings are interpreted as shell
          commands according to :func:`anadama.helpers.parse_sh`. If given
          just a string or just a callable, this method treats it as a
          one-item list of the string or callable.
        :type actions: str or callable or list of str or
          list of callable

        :param depends: The dependencies of the task. The task must
          have these dependencies before executing the
          actions. Strings or lists of strings are interpreted as
          filenames and turned into objects of type
          :class:`anadama.deps.FileDependency`. If given just a string or just
          a :class:`anadama.deps.BaseDependency`, this method treats it as a
          one-item list of the argument provided.
        :type depends: str or :class:`anadama.deps.BaseDependency` or list of
          str or list of :class:`anadama.deps.BaseDependency`

        :param targets: The targets of the task. The task must produce
          these targets after executing the actions to be considered
          as "success". Strings or lists of strings are interpreted as
          filenames and turned into objects of type
          :class:`anadama.deps.FileDependency`. If given just a string or just
          a :class:`anadama.deps.BaseDependency`, this method treats it as a
          one-item list of the argument provided.
        :type targets: str or :class:`anadama.deps.BaseDependency` or list of
          str or list of :class:`anadama.deps.BaseDependency`

        :param name: A name for the task. Task names must be unique
          within a run context.
        :type name: str

        :keyword interpret_deps_and_targs: Should I use
          :func:`anadama.helpers.parse_sh` to change
          ``{depends[0]}`` and ``{targets[0]}`` into the first item in
          depends and the first item in targets? Default is True 
        :type interpret_deps_and_targs: bool

        :returns: The :class:`anadama.Task` just created

        """

        deps = _build_depends(depends)
        targs = _build_targets(targets)
        task_no = next(self.task_counter)
        name = _build_name(name, task_no)
        if not actions: # must be a decorator
            def finish_add_task(fn):
                the_task = Task(name, [fn], deps, targs, task_no)
                self._add_task(the_task)
            return finish_add_task
        else:
            acts = _build_actions(actions, deps, targs,
                                  use_parse_sh=interpret_deps_and_targs)
            the_task = Task(name, acts, deps, targs, task_no)
            self._add_task(the_task)
            return the_task


    def go(self, run_them_all=False, quit_early=False, runner=None,
           reporter=None, storage_backend=None, n_parallel=1):
        """Kick off execution of all previously configured tasks. 

        :keyword run_them_all: Skip no tasks; run it all.
        :type run_them_all: bool
        
        :keyword quit_early: If any tasks fail, stop all execution
          immediately. If set to ``False`` (the default), children of
          failed tasks are *not* executed but children of successful
          or skipped tasks *are* executed: basically, keep going until
          you run out of tasks to execute.

        :keyword runner: The tasks to execute are passed to this
          object for execution.  For a list of runners that come
          bundled with anadama, see :mod:`anadama.runners`. Passing
          ``None`` (the default) uses the default runner from
          :func:`anadama.runners.default`.
        :type runner: instance of any
          :class:`anadama.runners.BaseRunner` subclass or None.

        :keyword reporter: As task execution proceeds, events are
          dispatched to this object for reporting purposes. For more
          information of the reporters bundled with anadama, see
          :mod:`anadama.reporters`. Passing ``None`` (the default)
          uses the default reporter from
          :func:`anadama.reporters.default`.
        :type reporter: instance of any
          :class:`anadama.reporters.BaseReporter` subclass or None.

        :keyword n_parallel: The number of tasks to execute in
          parallel. This option is ignored when a custom runner is
          used with the ``runner`` keyword.
        :type n_parallel: int

        """

        self.completed_tasks = set()
        self.failed_tasks = set()
        self.task_results = [None for _ in range(len(self.tasks))]
        self._reporter = reporter or reporters.default(self)
        self._reporter.started()

        _runner = runner or runners.default(self, n_parallel)
        _runner.quit_early = quit_early
        task_idxs = nx.algorithms.dag.topological_sort(self.dag, reverse=True)
        if not run_them_all:
            task_idxs = self._filter_skipped_tasks(task_idxs)
        task_idxs = deque(task_idxs)

        _runner.run_tasks(task_idxs)
        self._handle_finished()


    def _handle_task_result(self, result):
        if result.task_no is not None:
            self.task_results[result.task_no] = result
        if result.error:
            self.failed_tasks.add(result.task_no)
            self._reporter.task_failed(result)
        else:
            self._backend.save(result.dep_keys, result.dep_compares)
            self.completed_tasks.add(result.task_no)
            self._reporter.task_completed(result)


    def _handle_finished(self):
        self.compare_cache.clear()
        self._reporter.finished()
        if self.failed_tasks:
            raise RunFailed()


    def _filter_skipped_tasks(self, task_idxs):
        ret = list()
        for idx in task_idxs:
            if self._should_skip_task(idx):
                self._handle_task_skipped(idx)
            else:
                ret.append(idx)
        return ret

    
    def _should_skip_task(self, task_no):
        task = self.tasks[task_no]
        if not task.targets and not task.depends:
            return False
        if any(istask(d) for d in task.depends):
            return False
        if deps.any_different(task.depends, self._backend, self.compare_cache):
            return False
        if deps.any_different(task.targets, self._backend, self.compare_cache):
            return False
        return True


    def _handle_task_started(self, task_no):
        self._reporter.task_started(task_no)

    def _handle_task_skipped(self, task_no):
        self.completed_tasks.add(task_no)
        self._reporter.task_skipped(task_no)


    def already_exists(self, *depends):
        """Declare a dependency as pre-existing. That means that no task
        creates these dependencies; they're already there before any
        tasks run.

        .. note::

            If you have a list or other iterable containing the
            dependencies that already exist, you can declare them all
            like so ``ctx.already_exists(*my_bunch_of_deps)``.

        :param \*depends: One or many dependencies to mark as pre-existing.
        :type \*depends: any argument recognized by :func:`anadama.deps.auto`

        """

        self.add_task(noop, targets=map(deps.auto, depends),
                      name="Track pre-existing dependencies")


    def _add_task(self, task):
        """Actually add a task to the internal dependency data structure"""
        
        self.tasks.append(task)
        self.dag.add_node(task.task_no)
        for dep in task.depends:
            if istask(dep):
                self.dag.add_edge(dep.task_no, task.task_no)
                continue
            try:
                parent_task = self._depidx[dep]
            except KeyError:
                self._handle_nosuchdep(dep, task)
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
        msg = "Unable to find dependency `{}' of type `{}'. "
        msg = msg.format(str(dep), type(dep))
        alldeps = itertools.chain.from_iterable(
            [list(t.depends) + list(t.targets) for t in self.tasks]
        )
        try:
            closest = matcher.find_match(dep, alldeps, key=attrgetter("_key"))
        except:
            raise KeyError(msg)
        msg += "Perhaps you meant `{}' of type `{}'?"
        raise KeyError(msg.format(str(closest), type(closest)))


    def _add_do_pexist(self, dep):
        if not self._pexist_task:
            self._pexist_task = self.add_task(
                noop, targets=dep, name="Track pre-existing dependencies")
        else:
            d = deps.auto(dep)
            if d not in self._depidx:
                self._pexist_task.targets.append(deps.auto(dep))
                self._depidx.link(dep, self._pexist_task)



def _build_actions(actions, deps, targs, use_parse_sh=True):
    actions = filter(None, _sugar_list(actions))
    if use_parse_sh:
        mod = parse_sh
    else:
        mod = lambda cmd, *args, **kwargs: sh(cmd)
    return [ a if callable(a) else mod(a) for a in actions ]
        

def _build_depends(depends):
    depends = filter(None, _sugar_list(depends))
    return map(deps.auto, depends)


def _build_targets(targets):
    targets = filter(None, _sugar_list(targets))
    ret = list()
    for targ in targets:
        if istask(targ):
            raise ValueError("Can't make a task a target")
        ret.append(deps.auto(targ))
    return ret
    

def _build_name(name, task_no):
    if not name:
        return "Step "+str(task_no)
    else:
        return name


def _sugar_list(x):
    """Turns just a single thing into a list containing that single thing.

    """

    if istask(x) or not hasattr(x, "__iter__") or isinstance(x, basestring):
        return [x]
    else:
        return x


def _parse_wrapper(s, metachar):
    """search through string ``s`` and find terms wrapped in curly braces
    and a metacharacter ``metachar``. Intended for use with
    :meth:`anadama.runcontext.RunContext.do`.
    """

    start = len(metachar)+1
    return [ term[start:-1] for term in re.findall(metachar+r'{[^{}]+}', s) ]


def discover_binaries(s):
    """Search through string ``s`` and find all existing files smaller
    than 10MB. Return those files as a list of objects of type
    :class:`anadama.deps.FileDependency`.
    """

    ds = list()
    for term in shlex.split(s):
        if not os.path.exists(term):
            term = find_on_path(term)
        if not term:
            continue
        if not os.access(term, os.F_OK | os.X_OK):
            # doesn't exist or can't execute
            continue
        try:
            dep = deps.ExecutableDependency(term)
        except ValueError:
            continue
        if os.stat(dep.fname).st_size < 1<<20:
            ds.append(dep)

    return ds

    
