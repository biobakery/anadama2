from collections import namedtuple

from .helpers import sh
from .deps import FileDependency


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

        acts = self._build_actions(actions)
        deps = self._build_depends(depends)
        targs = self._build_targets(targets)
        task_no = next(self.task_counter)
        name = self._build_name(name, task_no)
        if acts is None: # must be a decorator
            def finish_add_task(fn):
                the_task = Task(name, [fn], deps, targs, task_no)
                self.add_task(the_task)
            return finish_add_task
        else:
            the_task = Task(name, acts, deps, targs, task_no)
            self.add_task(the_task)
            return the_task


    def self._build_actions(actions):
        actions = _sugar_list(actions)
        return [ a if callable(a) else sh(a) for a in actions ]
        

    def self._build_depends(depends):
        depends = _sugar_list(depends)
        return [ d if isinstance(d, basestring) else FileDependency(d)
                 for d in depends ]


    def self._build_targets(targets):
        targets = _sugar_list(targets)
        return [ t if isinstance(t, basestring) else FileDependency(t)
                 for t in depends ]


    def self._build_name(name, task_no):
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



