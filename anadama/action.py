import sys
from doit.action import CmdAction as DoitCmdAction
from doit.action import PythonAction

"""
AnADAMA's CmdAction has the option of being verbose.
Just do something like this: my_action = CmdAction(..., verbose=True)
"""

class CmdAction(DoitCmdAction):
    def __init__(self, *args, **kwargs):
        self.verbose = kwargs.pop('verbose', False)
        super(CmdAction, self).__init__(*args, **kwargs)

    def execute(self, *args, **kwargs):
        # Hope calling expand_action() has no side effects!
        print >> sys.stderr, self.expand_action()
        return super(CmdAction, self).execute(*args, **kwargs)

