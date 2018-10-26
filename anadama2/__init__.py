# -*- coding: utf-8 -*-
import six

class Task(object):
    """A unit of work. 
    
    :param name: The task name; must be unique to all tasks within a workflow.
    :type name: str or unicode
    
    :param actions: The actions to execute; do these and the work is done.
    :type actions: list of callable
    
    :param depends: The list of dependencies. 
    :type depends: list of :class:`anadama2.tracked.Base`
    
    :param targets: The list of targets. The task must produce all of
      these to be a successfully complete task.
    :type targets: list of :class:`anadama2.tracked.Base`
    
    :param task_no: The unique task number. Ordered by declaration,
      not execution.
    :type task_no: int

    :param visible: Whether the task should appear in the console or
      not. These tasks do not generate targets (only track).
    :type visible: bool

    :param actions_raw: The raw actions from the user, unbuilt.
    :type actions: list of callable

    :param kwargs: Optional arguments used to build the actions.
    :type kwargs: dictionary

    :param use_parse_sh: Should parse actions resolving targets/dependencies.
    :type use_parse_sh: bool

    """
    
    def __init__(self, name, actions, depends, targets, task_no, visible, actions_raw, kwargs, use_parse_sh):
        # Set a default task number
        if task_no is None:
            self.task_no="NA"
        else:
            self.task_no=task_no
            
        # Set a default task name if not provided
        if name is None:
            self.name="Task"+str(task_no)
        else:
            self.name=name
            
        self.actions=actions
        self.depends=depends
        self.targets=targets
        self.visible=visible

        self.actions_raw=actions_raw
        self.kwargs=kwargs
        self.use_parse_sh=use_parse_sh
        
        # get a task description based on the actions or name
        if six.callable(actions[0]):
            # if the first action is a function, use the function name
            command=actions[0].__name__
        else:
            # if the first action is a command, use the executable name
            command=six.u(actions[0]).split(" ")[0]
            
        # if the task name is not set, then use the command name for the description
        if name is None:
            self.description=command
        else:
            self.description=six.u(name)
            
            
from .workflow import Workflow
from .document import PweaveDocument

Workflow # pyflakes
