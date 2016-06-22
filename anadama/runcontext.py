import os
import re
import shlex
import logging
import itertools
from operator import attrgetter, itemgetter
from collections import deque, defaultdict

import networkx as nx
from networkx.algorithms.traversal.depth_first_search import dfs_edges

from . import Task
from . import deps
from . import reporters
from . import runners
from . import backends
from .taskcontainer import TaskContainer
from .helpers import sh, parse_sh
from .util import matcher, noop, find_on_path
from .util import istask, sugar_list, dichotomize
from .util import keepkeys

second = itemgetter(1)
logger = logging.getLogger(__name__)


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
        #: tasks is a :class:`anadama.taskcontainer.TaskContainer`
        #: filled with objects of type : :class:`anadama.Task`. This
        #: list is populated as new tasks : are added via
        #: :meth:`anadama.runcontext.RunContext.add_task` : and
        #: :meth:`anadama.runcontext.RunContext.do`
        self.tasks = TaskContainer()
        #: task_results is a list of objects of type :
        #: :class:`anadama.runners.TaskResult`. This list is populated
        #: only after tasks have been run with
        #: :meth:`anadama.runcontext.RunContext.go`.
        self.task_results = list()
        self._depidx = deps.DependencyIndex()
        self._pexist_task = None
        self._backend = storage_backend or backends.default()
        logger.debug("Instantiated run context")


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
            ns = deps.KVContainer.key(None)
            d = deps.KVDependency(ns, str(len(self.tasks)+1), sh_cmd)
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


    def go(self, run_them_all=False, quit_early=False, runner=None,
           reporter=None, storage_backend=None, n_parallel=1,
           until_task=None):
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

        :keyword until_task: Stop after running the named task. Can
          refer to the end task by task number or task name.
        :type n_parallel: int or str

        """

        self.completed_tasks = set()
        self.failed_tasks = set()
        self.task_results = [None for _ in range(len(self.tasks))]
        self._reporter = reporter or reporters.default(self)
        self._reporter.started()

        _runner = runner or runners.default(self, n_parallel)
        _runner.quit_early = quit_early
        logger.debug("Sorting task_nos by network topology")
        task_idxs = nx.algorithms.dag.topological_sort(self.dag, reverse=True)
        logger.debug("Sorting complete")
        if until_task is not None:
            parents = allparents(self.dag, self.tasks[until_task].task_no)
            task_idxs = filter(parents.__contains__, task_idxs)
        if not run_them_all:
            task_idxs = self._filter_skipped_tasks(task_idxs)
        task_idxs = deque(task_idxs)

        _runner.run_tasks(task_idxs)
        self._handle_finished()


    def run_task(self, no_or_name, **kwargs):
        kwargs['up_to_task'] = no_or_name
        return self.go(**kwargs)


    def _import(self, task_dict):
        keys_to_keep = ("actions", "depends", "targets",
                        "name", "interpret_deps_and_targs")
        return self.add_task(**keepkeys(task_dict, keys_to_keep))

    _ = _import


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
        self._reporter.finished()
        if self.failed_tasks:
            raise RunFailed()


    def _filter_skipped_tasks(self, task_idxs):
        should_run, idxs = dichotomize(task_idxs, self._always_rerun)
        should_run = set(should_run)
        for dep, idxs_set in self._aggregate_deps(idxs):
            if deps.any_different([dep], self._backend):
                for idx in idxs_set:
                    logger.debug("Can't skip task %i because of dep change",
                                 idx)
                    should_run.add(idx)
        while idxs:
            idx = idxs.pop()
            if idx in should_run:
                continue
            for parent_idx in self.dag.predecessors(idx):
                if parent_idx in should_run:
                    should_run.add(idx)
                    logger.debug("Can't skip %i because it depends "
                                 "directly on task %i, which will be rerun",
                                 parent_idx, idx)

        to_run, skipped = dichotomize(task_idxs, should_run.__contains__)
        for idx in skipped:
            self._handle_task_skipped(idx)
        return to_run


    def _always_rerun(self, task_no):
        task = self.tasks[task_no]
        if not task.targets and not task.depends:
            logger.debug("Can't skip task %i because it "
                          "has no targets or depends", task_no)
            return True
        return False


    def _aggregate_deps(self, idxs):
        grp = defaultdict(set)
        for idx in idxs:
            task = self.tasks[idx]
            for dep in itertools.chain(task.depends, task.targets):
                if istask(dep):
                    continue
                grp[dep].add(idx)
                    
        return grp.iteritems()
        

    def _handle_task_started(self, task_no):
        self._reporter.task_started(task_no)


    def _handle_task_skipped(self, task_no):
        self.completed_tasks.add(task_no)
        self._reporter.task_skipped(task_no)


    def _add_task(self, task):
        """Actually add a task to the internal dependency data structure"""
        
        self.tasks.append(task)
        self.dag.add_node(task.task_no)
        for dep in task.depends:
            if istask(dep):
                self.dag.add_edge(dep.task_no, task.task_no)
                continue
            if not _must_preexist(dep) and dep not in self._depidx:
                self._add_do_pexist(dep)
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
    actions = filter(None, sugar_list(actions))
    if use_parse_sh:
        mod = parse_sh
    else:
        mod = lambda cmd, *args, **kwargs: sh(cmd)
    return [ a if callable(a) else mod(a) for a in actions ]
        

def _build_depends(depends):
    depends = filter(None, sugar_list(depends))
    return map(deps.auto, depends)


def _build_targets(targets):
    targets = filter(None, sugar_list(targets))
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


def _must_preexist(d):
    t = (deps.StringDependency,
         deps.KVDependency,
         deps.FunctionDependency)
    return type(d) not in t


def allchildren(dag, task_no):
    return itertools.imap(second, dfs_edges(dag, task_no))


def allparents(dag, task_no):
    seen = set()
    to_check = deque([task_no])
    while to_check:
        idx = to_check.popleft()
        if idx in seen:
            continue
        seen.add(idx)
        to_check.extend(dag.predecessors(idx))
    return seen
