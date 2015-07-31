import os
import time
import json
from functools import partial

from doit.reporter import ConsoleReporter
from doit.reporter import REPORTERS as doit_REPORTERS
from doit.action import CmdAction

import requests

from .util import partition

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


def is_lame_task(task):
    return (not task
            or not task.actions
            or task.name.startswith("_"))

is_not_lame_task = lambda t: not is_lame_task(t)

            
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



class WebReporter(ConsoleReporter):
    def __init__(self, outstream, options, *args, **kwargs):
        super(WebReporter, self).__init__(outstream, options, *args, **kwargs)
        self.url = options.get("reporter_url", None)
        self.send = lambda *args, **kwargs: None
        self.times = {}
        if self.url:
            self.send = lambda resource, d: requests.post(
                self.url+"/"+resource,
                headers={"Content-Type": "Application/json"},
                data=d)

    
    def write(self, text):
        pass
    

    def initialize(self, tasks):
        for chunk in partition(tasks.itervalues(), 20000):
            self.send("init", json.dumps(
                [ {"name": t.name, "task_dep": t.task_dep}
                  for t in filter(is_not_lame_task, chunk) ]
            ))


    def execute_task(self, task):
        if is_lame_task(task):
            return
        self.times[task.name] = time.time()
        self.send("execute", json.dumps({"name": task.name,
                                         "file_dep": list(task.file_dep),
                                         "targets": list(task.targets)}))

    def skip_uptodate(self, task):
        if is_lame_task(task):
            return
        self.send("skip", json.dumps({"name": task.name}))


    def add_success(self, task):
        if is_lame_task(task):
            return
        to_send = {"name": task.name}
        to_send['targets'] = [(f, os.stat(f).st_size) for f in task.targets]
        to_send['outs'], to_send['errs'] = zip(*_alltext(task))
        if task.name in self.times:
            to_send['time'] = time.time() - self.times[task.name]
            self.times.pop(task.name)
        self.send("success", json.dumps(to_send))


    def add_failure(self, task, exception):
        if is_lame_task(task):
            return
        to_send = {"name": task.name, "exc": str(exception)}
        to_send['outs'], to_send['errs'] = zip(*_alltext(task))
        if task.name in self.times:
            to_send['time'] = time.time() - self.times[task.name]
            self.times.pop(task.name)
        self.send("fail", json.dumps(to_send))


    def complete_run(self):
        self.send("finish", "{}")


                
REPORTERS = doit_REPORTERS
REPORTERS['verbose'] = VerboseConsoleReporter
REPORTERS['web'] = WebReporter
