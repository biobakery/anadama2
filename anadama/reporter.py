import os

from doit.reporter import ConsoleReporter
from doit.reporter import REPORTERS as doit_REPORTERS
from doit.action import CmdAction


def _maybestrip(maybe_str):
    if type(maybe_str) is str:
        return maybe_str.strip()
    else:
        return maybe_str


def _alltext(task):
    return [
        (_maybestrip(action.out), _maybestrip(action.err))
        for action in task.actions
    ]

            
class VerboseConsoleReporter(ConsoleReporter):
    def execute_task(self, task, *args, **kwargs):
        super(VerboseConsoleReporter, self).execute_task(task, *args, **kwargs)
        if task.actions and (task.name[0] != '_'):
            for action in task.actions:
                if hasattr(action, 'expand_action'):
                    # I don't think calling expand_action has any side
                    # effects right now. hope it never does!
                    self.write(action.expand_action()+'\n')


    def _find_output_files(self, task):
        if not task.targets:
            return None, None
        fbase = task.targets[0]+"."
        outs, errs = zip(*_alltext(task))
        out_f, err_f = None, None
        if any(outs) and os.path.exists(fbase+"out"):
            out_f = open(fbase+"out", 'w')
        if any(errs) and os.path.exists(fbase+"err"):
            err_f = open(fbase+"err", 'w')
        return (out_f, err_f)


    def add_failure(self, task, exception):
        if task.actions and task.name[0] != '_':
            for action in task.actions:
                if type(action.err) is str:
                    action.err += ("\n" + str(exception))
                else:
                    action.err = str(exception)
        return self.add_success(task)


    def add_success(self, task):
        out_f, err_f = self._find_output_files(task)
        try:
            for action in task.actions:
                for f, attr in ((out_f, action.out), (err_f, action.err)):
                    if attr and f:
                        print >> f, attr
                    elif attr and not f:
                        print attr
        finally:
            if out_f:
                out_f.close()
            if err_f:
                err_f.close()


REPORTERS = doit_REPORTERS
REPORTERS['verbose'] = VerboseConsoleReporter
