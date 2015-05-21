"""
Execution strategies for workflows.
"""
import os
import sys
from doit.exceptions import TaskError, TaskFailed

default_conditions = [
    lambda ret, *args, **kwargs: type(ret) in (TaskError, TaskFailed),
    lambda ret, *args, **kwargs: ret is False
]

class Group(list):
    
    def __init__(self, *args, **kwargs):
        super(list, self).__init__()
        self.extend(args)
        self.out = ""
        self.err = ""

    def execute(self):
        for action in self:
            ret = action.execute()
            if action.out and action.out.strip():
                self.out += action.out
            if action.err and action.err.strip():
                self.err += action.err
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

    Extra ``*args`` or ``**kwargs`` are applied to the default and
    extra condition functions.  Finally, return (but not raise) a
    doit.exceptions.TaskFailed exception if all actions fall through.

    :param actions: Iterable; The collection of actions.
    :keyword default_conditions: List; The default conditions 
                                 (strategies.default_conditions)
    :keyword extra_conditions: List; Any extra conditions to determine if 
                               an action failed. Functions in this list are
                               executed in order and passed the return value
                               of an actions .execute() method. Extra arguments
                               and keywords are passed in if provided from
                               ``*args`` or ``**kwargs``.

    """
    conditions = default_conditions+extra_conditions
    for action in actions:
        ret = action_execute(action)
        if not any( c(ret, *args, **kwargs) for c in conditions ):
            return ret

    return ret


def if_exists_run(hopefully_exists_fname, cmd, output_fname_list,
                  verbose=True):
    if issubclass(cmd, str):
        cmd = CmdAction(cmd, verbose=verbose)
    def action():
        if os.path.exists(hopefully_exists_fname) \
           and os.stat(hopefully_exists_fname).st_size > 1:
            return action_execute(cmd)
        else:
            for fname in output_fname_list:
                open(fname, 'w').close()
    return action
        

def action_execute(action):
    ret = action.execute()
    if action.out and action.out.strip():
        print action.out
    if action.err and action.err.strip():
        print >> sys.stderr, action.err
