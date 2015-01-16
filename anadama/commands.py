import sys
import six
import codecs
import inspect
import pprint
from cStringIO import StringIO
from functools import partial
from operator import attrgetter

from doit.cmd_run import Run as DoitRun
from doit.cmd_run import (
    opt_always, opt_continue, opt_verbosity, 
    opt_reporter, opt_num_process, opt_single,
)
from doit.cmd_help import Help as DoitHelp
from doit.cmd_list import List as DoitList
from doit.task import Task
from doit.control import TaskControl
from doit.runner import Runner, MRunner, MThreadRunner
from doit.cmd_base import DoitCmdBase, Command
from doit.cmdparse import CmdOption
from doit.exceptions import InvalidCommand, InvalidDodoFile

from . import dag
from .runner import RUNNER_MAP
from .reporter import REPORTERS
from .provenance import find_versions
from .loader import PipelineLoader

opt_runner = dict(
    name    = "runner",
    long    = "runner",
    default = "MRunner",
    help    = ("Runner to use for executing tasks."
               " Choices: "+",".join(RUNNER_MAP.keys()) ),
    type    = str
)

opt_tmpfiles = dict(
    name    = "tmpfiledir",
    long    = "tmpfiledir",
    default = "/tmp",
    help    = "Where to save temporary files",
    type    = str
)

opt_pipeline_name = dict(
    name    = "pipeline_name",
    long    = "pipeline_name",
    default = "Custom Pipeline",
    help    = "Optional name to give to the current pipeline",
    type    = str
)

opt_reporter['help'] = \
"""Choose output reporter. Available:
'default': report output on console
'executed-only': no output for skipped (up-to-date) and group tasks
'json': output result in json format
'verbose': output actions on console as they're executed
[default: %(default)s]
"""

class AnadamaCmdBase(DoitCmdBase):
    my_base_opts = ()
    my_opts = ()

    def set_options(self):
        opt_list = (self.my_base_opts + self.my_opts + 
                    self.base_options + self._loader.cmd_options +
                    self.cmd_options)
        return [CmdOption(opt) for opt in opt_list]


class Run(AnadamaCmdBase, DoitRun):
    my_opts = (opt_runner, opt_pipeline_name)

    def _execute(self, outfile,
                 verbosity=None, always=False, continue_=False,
                 reporter='default', num_process=0, par_type='process',
                 single=False, pipeline_name="Custom Pipeline"):
        """
        @param reporter: (str) one of provided reporters or ...
                         (class) user defined reporter class 
                                 (can only be specified
                                 from DOIT_CONFIG - never from command line)
                         (reporter instance) - only used in unittests
        """
        # get tasks to be executed
        # self.control is saved on instance to be used by 'auto' command
        self.control = TaskControl(self.task_list)
        self.control.process(self.sel_tasks)

        if single:
            for task_name in self.sel_tasks:
                task = self.control.tasks[task_name]
                if task.has_subtask:
                    for task_name in task.task_dep:
                        sub_task = self.control.tasks[task_name]
                        sub_task.task_dep = []
                else:
                    task.task_dep = []

        # reporter
        if isinstance(reporter, six.string_types):
            if reporter not in REPORTERS:
                msg = ("No reporter named '%s'."
                       " Type 'doit help run' to see a list "
                       "of available reporters.")
                raise InvalidCommand(msg % reporter)
            reporter_cls = REPORTERS[reporter]
        else:
            # user defined class
            reporter_cls = reporter

        # verbosity
        if verbosity is None:
            use_verbosity = Task.DEFAULT_VERBOSITY
        else:
            use_verbosity = verbosity
        show_out = use_verbosity < 2 # show on error report

        # outstream
        if isinstance(outfile, six.string_types):
            outstream = codecs.open(outfile, 'w', encoding='utf-8')
        else: # outfile is a file-like object (like StringIO or sys.stdout)
            outstream = outfile

        # run
        try:
            # FIXME stderr will be shown twice in case of task error/failure
            if isinstance(reporter_cls, type):
                reporter_obj = reporter_cls(outstream, {'show_out':show_out,
                                                        'show_err': True})
            else: # also accepts reporter instances
                reporter_obj = reporter_cls


            run_args = [self.dep_class, self.dep_file, reporter_obj,
                        continue_, always, verbosity]
            RunnerClass = RUNNER_MAP.get(self.opt_values["runner"])
            if not RunnerClass:
                RunnerClass = self._discover_runner_class(
                    num_process, par_type)

            runner = RunnerClass(*run_args)
            runner.pipeline_name = pipeline_name
            return runner.run_all(self.control.task_dispatcher())
        finally:
            if isinstance(outfile, str):
                outstream.close()

    def _discover_runner_class(self, num_process, par_type):
        if num_process == 0:
            return Runner
        else:
            if par_type == 'process':
                RunnerClass = MRunner
                if MRunner.available():
                    return partial(MRunner, num_process=num_process)
                else:
                    RunnerClass = MThreadRunner
                    sys.stderr.write(
                        "WARNING: multiprocessing module not available, " +
                        "running in parallel using threads.")
            elif par_type == 'thread':
                    return partial(MThreadRunner, num_process=num_process)
            else:
                msg = "Invalid parallel type %s"
                raise InvalidCommand(msg % par_type)



class ListDag(Run):
    my_opts = (opt_runner, opt_tmpfiles, opt_pipeline_name)
    name = "dag"
    doc_purpose = "print execution tree"
    doc_usage = "[TASK ...]"


    def _execute(self,
                 verbosity=None, always=False, continue_=False,
                 reporter='default', num_process=0,
                 single=False, pipeline_name="Custom Pipeline"):
        self.opt_values['runner'] = 'jenkins'
        dag.TMP_FILE_DIR = self.opt_values["tmpfiledir"]
        return super(ListDag, self)._execute(outfile=sys.stdout, verbosity=verbosity,
                                             always=always, continue_=continue_,
                                             reporter=reporter,
                                             num_process=num_process,
                                             par_type="process", single=single,
                                             pipeline_name=pipeline_name)


class Help(DoitHelp):
    name = "help"

    @staticmethod
    def print_usage(cmds):
        """Print anadama usage instructions"""
        print("AnADAMA -- https://bitbucket.org/biobakery/anadama")
        print('')
        print("Commands")
        for cmd in sorted(six.itervalues(cmds), key=attrgetter('name')):
            six.print_("  anadama %s \t\t %s" % (cmd.name, cmd.doc_purpose))
        print("")
        print("  anadama help                              show help / reference")
        print("  anadama help task                         show help on task fields")
        print("  anadama help pipeline <module.Pipeline>   show module.Pipeline help")
        print("  anadama help <command>                    show command usage")
        print("  anadama help <task-name>                  show task usage")


    @staticmethod
    def print_pipeline_help(pipeline_class):
        message = StringIO()
        spec = inspect.getargspec(pipeline_class.__init__)
        args = [a for a in spec.args if a != "self"] #filter out self
        print >> message, "Arguments: "
        print >> message, pprint.pformat(args)

        print >> message, "Default options: "
        print >> message, pprint.pformat(pipeline_class.default_options)

        print >> message, "" #newline
        print >> message, pipeline_class.__doc__

        print >> message, ""
        print >> message, pipeline_class.__init__.__doc__
        
        print message.getvalue()


    def execute(self, params, args):
        """execute cmd 'help' """
        cmds = self.doit_app.sub_cmds
        if len(args) == 0 or len(args) > 2:
            self.print_usage(cmds)
        elif args[0] == 'task':
            self.print_task_help()
        elif args == ['pipeline']:
            six.print_(cmds['pipeline'].help())
        elif args[0] == 'pipeline':
            cls = PipelineLoader._import(args[1])
            self.print_pipeline_help(cls)
        elif args[0] in cmds:
            # help on command
            six.print_(cmds[args[0]].help())
        else:
            # help of specific task
            try:
                if not DoitCmdBase.execute(self, params, args):
                    self.print_usage(cmds)
            except InvalidDodoFile as e:
                self.print_usage(cmds)
                raise InvalidCommand("Unable to retrieve task help: "+e.message)
        return 0



class BinaryProvenance(Command):
    name = "binary_provenance"
    doc_purpose = "print versions for required dependencies"
    doc_usage = "<module> [<module> [<module...]]"
    
    def execute(self, opt_values, pos_args):
        """Import workflow modules as specified from positional arguments and
        determine versions of installed executables and other
        dependencies via py:ref:`provenance.find_versions`.

        For each external dependency installed, print the name of the
        dependency and the version of the dependency.

        """

        for mod_name in pos_args:
            for binary, version in find_versions(mod_name):
                print binary, "\t", version


class RunPipeline(Run):
    name = "pipeline"
    doc_purpose = "run an AnADAMA pipeline"
    doc_usage = "<module.Pipeline> [options]"

    cmd_options = (opt_always, opt_continue, opt_verbosity, 
                   opt_reporter, opt_num_process, opt_single)

    my_opts = (opt_runner, opt_tmpfiles, opt_pipeline_name)

    def __init__(self, *args, **kwargs):
        kwargs['task_loader'] = PipelineLoader()
        super(RunPipeline, self).__init__(*args, **kwargs)


    def execute(self, opt_values, pos_args, *args, **kwargs):
        if not pos_args:
            raise InvalidCommand("No pipeline specified. Try pipeline -h")
        pipeline_name = pos_args.pop()
        self._loader.pipeline_cls = pipeline_name

        return super(RunPipeline, self).execute(opt_values, pos_args,
                                                *args, **kwargs)


    def _execute(self, verbosity=None, always=False, continue_=False,
                 reporter='default', num_process=0, single=False, 
                 pipeline_name="Custom Pipeline"):
        return super(RunPipeline, self)._execute(
            outfile=sys.stdout, verbosity=verbosity,
            always=always, continue_=continue_,
            reporter=reporter,
            num_process=num_process,
            par_type="process", single=single,
            pipeline_name=pipeline_name
        )
        
class DagPipeline(ListDag, RunPipeline):
    name = "pipeline_dag"
    doc_purpose = "print dag from pipeline"
    doc_usage = "<some_module.SomePipeline> [options]"


all = (Run, ListDag, Help, BinaryProvenance, RunPipeline, DagPipeline)
