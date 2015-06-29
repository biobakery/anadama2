import sys
import codecs
from functools import partial

import six

from doit.task import Task
from doit.exceptions import InvalidCommand
from doit.control import TaskControl
from doit.runner import Runner, MRunner, MThreadRunner
from doit.cmd_run import Run as DoitRun

from .. import performance
from ..reporter import REPORTERS
from ..runner import RUNNER_MAP, GRID_RUNNER_MAP

from . import AnadamaCmdBase
from . import opt_runner, opt_pipeline_name, opt_tmpfiles

opt_grid_part = {
    "name": "partition",
    "long": "partition",
    "help": "Select what queue to submit tasks; For use with a grid runner",
    "type": str,
    "default": ""
}

opt_perf_url = {
    "name": "perf_url",
    "long": "perf_url",
    "help": "Where to save grid job performance",
    "type": str,
    "default": performance.DEFAULT_URL
}

opt_grid_args = {
    "name": "grid_args",
    "long": "gridargs",
    "help": ("Extra arguments to give to the grid"
             " runner command (srun, bsub, etc)"),
    "type": str,
    "default": ""
}


class Run(AnadamaCmdBase, DoitRun):
    my_opts = (opt_runner, opt_pipeline_name,
               opt_grid_part, opt_perf_url, opt_tmpfiles, 
               opt_grid_args)

    def _execute(self, outfile=sys.stdout,
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
            run_kwargs = {}
            RunnerClass = RUNNER_MAP.get(self.opt_values["runner"])
            if not RunnerClass:
                RunnerClass = self._discover_runner_class(
                    num_process, par_type)
            elif self.opt_values['runner'] in GRID_RUNNER_MAP:
                if not self.opt_values.get('partition', None):
                    raise InvalidCommand("--partition option is required "
                                         "when using a grid runner")
                run_args = [self.opt_values['partition'],
                            self.opt_values['perf_url'],
                            self.opt_values['tmpfiledir'],
                            self.opt_values['grid_args']]+run_args
                run_kwargs['num_process'] = num_process if num_process else 1

            runner = RunnerClass(*run_args, **run_kwargs)
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
                if MRunner.available():
                    return partial(MRunner, num_process=num_process)
                else:
                    sys.stderr.write(
                        "WARNING: multiprocessing module not available, " +
                        "running in parallel using threads.")
            elif par_type == 'thread':
                    return partial(MThreadRunner, num_process=num_process)
            else:
                msg = "Invalid parallel type %s"
                raise InvalidCommand(msg % par_type)

