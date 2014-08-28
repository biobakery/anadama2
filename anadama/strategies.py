"""
Execution strategies for workflows.
"""
import sys

from doit.exceptions import TaskError, TaskFailed

default_conditions = [
    lambda ret, *args, **kwargs: type(ret) in (TaskError, TaskFailed),
    lambda ret: ret is False
]

class Group(list):
    
    def __init__(self, *args, **kwargs):
        super(list, self).__init__()
        self.extend(args)

    def execute(self):
        for action in self:
            ret = action.execute()
            if any( c(ret) for c in default_conditions ):
                return ret


    def __repr__(self):
        return "strategies.Group(%s)"%(
            ", ".join([repr(item) for item in self]))


def backup(actions, 
           default_conditions = default_conditions,
           extra_conditions   = list(), 
           *args, **kwargs):
    """Try to .execute() the first action. If that action fails (as
    determined by any of the condition functions in
    `default_conditions` or `extra_conditions` returning True), try
    the next action.

    Extra *args or **kwargs are applied to the default and extra
    condition functions.  Finally, return (but not raise) a
    doit.exceptions.TaskFailed exception if all actions fall through.

    :param actions: Iterable; The collection of actions.
    :keyword default_conditions: List; The default conditions 
                                 (strategies.default_conditions)
    :keyword extra_conditions: List; Any extra conditions to determine if 
                               an action failed. Functions in this list are
                               executed in order and passed the return value
                               of an actions .execute() method. Extra arguments
                               and keywords are passed in if provided from
                               `*args` or `**kwargs`.

    """
    conditions = default_conditions+extra_conditions
    for action in actions:
        ret = action.execute()
        if not any( c(ret, *args, **kwargs) for c in conditions ):
            return ret

    return ret
