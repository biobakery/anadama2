
from doit.reporter import ConsoleReporter
from doit.reporter import REPORTERS as doit_REPORTERS
from doit.action import CmdAction

class VerboseConsoleReporter(ConsoleReporter):
    def execute_task(self, task, *args, **kwargs):
        super(VerboseConsoleReporter, self).execute_task(task, *args, **kwargs)
        if task.actions and (task.name[0] != '_'):
            for action in task.actions:
                if hasattr(action, 'expand_action'):
                    # I don't think calling expand_action has any side
                    # effects right now. hope it never does!
                    self.write(action.expand_action()+'\n')

REPORTERS = doit_REPORTERS
REPORTERS['verbose'] = VerboseConsoleReporter
