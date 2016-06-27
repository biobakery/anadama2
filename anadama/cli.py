import sys
import json
import optparse
import itertools

from .util import istask, Bag

BANNER = """
                          _____          __  __
     /\             /\   |  __ \   /\   |  \/  |   /\    \\
====/  \===_=__====/  \==| |  | |=/  \==| \  / |==/  \====\\
===/ /\ \=| '_ \==/ /\ \=| |  | |/ /\ \=| |\/| |=/ /\ \===//
  / ____ \| | | |/ ____ \| |__| / ____ \| |  | |/ ____ \ //
 /_/    \_\_| |_/_/    \_\_____/_/    \_\_|  |_/_/    \_\

"""

options = [
    optparse.make_option("-l", '--list-tasks', action="store_true",
                         help="Print all configured tasks."),
    optparse.make_option("-d", "--dump-dependencies", action="store_true",
                         help="Print all known dependencies to standard out."),
    optparse.make_option("-f", "--forget",
                         help="""Remove dependency from storage backend. Also accepts
                         keys line-by-line to standard in"""),
    optparse.make_option("-a", '--run-them-all', action="store_true",
                         help="Skip no tasks; run it all."),
    optparse.make_option("-e", '--quit-early', action="store_true",
                         help="""If any tasks fail, stop all execution immediately. If set to
                         ``False`` (the default), children of failed tasks are *not*
                         executed but children of successful or skipped tasks *are*
                         executed: basically, keep going until you run out of tasks
                         to execute."""),
    optparse.make_option("-n", '--n-parallel', default=1, type=int,
                         help="The number of tasks to execute in parallel."),
    optparse.make_option("-u", '--until-task', default=None,
                         help="""Stop after running the named task. Can refer to
                         the end task by task number or task name."""),
]


def _depformat(d):
    if istask(d):
        return "    Task {} - {}".format(d.task_no, d.name)
    else:
        return "    {} ({})".format(d._key, type(d))


def list_tasks(ctx):
    for task in ctx.tasks:
        print "{} - {}".format(task.task_no, task.name)
        print "  Dependencies ({})".format(len(task.depends))
        for dep in task.depends:
            print _depformat(dep)
        print "  Targets ({})".format(len(task.targets))
        for dep in task.targets:
            print _depformat(dep)
        print "------------------"


def dump_dependencies(backend):
    for key in backend.keys():
        b = Bag()
        b._key = key
        print key, "\t", json.dumps(backend.lookup(b))


def forget(backend, key=None):
    keys = []
    if key:
        keys.append(key)
    if sys.stdin.isatty():
        stdin_keys = iter(line.strip() for line in sys.stdin)
        keys = itertools.chain(keys, stdin_keys)
    for key in keys:
        try:
            backend.delete(key)
        except Exception as e:
            print >> sys.stderr, "Error inserting key {}: {}".format(key, e)
