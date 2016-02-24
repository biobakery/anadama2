
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


from .runcontext import RunContext

