import os
import sys
import logging

import six


def default(output_dir=None):
    log = "anadama.log"
    if output_dir:
        log = os.path.join(str(output_dir), log)
    return ReporterGroup([
        LoggerReporter("debug", log),
        ConsoleReporter()
    ])

class BaseReporter(object):

    """The base reporter defines functionality similar amongst all
    reporters.

    The workflow that executes the hooks defined can be accessed at
    ``self.run_context``.
    """

    def started(self, run_context):
        """Executed when a run is started, usually when
        :meth:`anadama2.workflow.Workflow.go` is executed.

        """
        self.run_context = run_context
        raise NotImplementedError()

    def task_skipped(self, task_no):
        """Executed when anadama determines that a task needn't be run.

        :param task_no: The task number of the task that is
          skipped. To get the actual :class:`anadama2.Task` object
          that's being skipped, do ``self.run_context.tasks[task_no]``.

        :type task_no: int

        """

        raise NotImplementedError()

    def task_started(self, task_no):
        """Executed when anadama is just about to execute a task.

        :param task_no: The task number of the task that is
          being started. To get the actual :class:`anadama2.Task` object
          that's being executed, do ``self.run_context.tasks[task_no]``.

        :type task_no: int

        """

        raise NotImplementedError()

    def task_failed(self, task_result):
        """Executed when a task fails.

        :param task_no: The task number of the task that failed . To
          get the actual :class:`anadama2.Task` object that failed, do
          ``self.run_context.tasks[task_no]``. To get the task result
          of the task that failed,
          do``self.run_context.task_results[task_no]``

        :type task_no: int

        """
        raise NotImplementedError()        

    def task_completed(self, task_result):
        """Executed when a task completes with no errors.

        :param task_no: The task number of the task that succeeded . To
          get the actual :class:`anadama2.Task` object that succeeded, do
          ``self.run_context.tasks[task_no]``. To get the task result
          of the task that succeeded,
          do``self.run_context.task_results[task_no]``

        :type task_no: int

        """
        raise NotImplementedError()

    def finished(self):
        """Executed when a run finishes. This method is called whether there
        are task failures or not.

        """
        raise NotImplementedError()


class ReporterGroup(BaseReporter):
    """Sometimes you want to use multiple reporters. For that, there is
    ReporterGroup. Here's an example usage:

    .. code:: python

      from anadama2.reporters import ReporterGroup
      my_grouped_reporter = ReporterGroup([custom_reporter_a, 
                                           custom_reporter_b, 
                                           custom_reporter_c])
      ...
      ctx.go(reporter=my_grouped_reporter)

    """

    def __init__(self, other_reporters):
        self.reps = other_reporters


    def started(self, ctx):
        for r in self.reps:
            r.started(ctx)


    def task_skipped(self, task_no):
        for r in self.reps:
            r.task_skipped(task_no)


    def task_started(self, task_no):
        for r in self.reps:
            r.task_started(task_no)


    def task_failed(self, task_result):
        for r in self.reps:
            r.task_failed(task_result)


    def task_completed(self, task_result):
        for r in self.reps:
            r.task_completed(task_result)


    def finished(self):
        for r in self.reps:
            r.finished()

    

class ConsoleReporter(BaseReporter):
    """Prints out run progress to stderr.
    An example readout is as follows:

    ::

      (s)[  1/  6 -  16.67%] Track pre-existing dependencies

    The readout is composed of five pieces of information:

      1. The task status. That's the part in the parentheses. 
      
        * ``( )`` means that the task is currently being executed

        * ``(+)`` means that the task finished successfully

        * ``(s)`` means that the task was skipped

        * ``(!)`` means that the task failed.

      2. The current task number. That's the first number in the
         square brackets.

      3. The total number of tasks to be run or skipped. That's the
         number after the forward slash.

      4. The percent complete of the current run. That's the number
         with a percent-sign next to it

      5. The task name. That's the text that comes after the ending
         square bracket. Remember that you can set the task name with
         the ``name`` option to
         :meth:`anadama2.workflow.Workflow.add_task`.

    """


    msg_str = six.u("({:.1})[{:3}/{:3} - {:6.2f}%] {:.57}")

    class stats:
        skip  = six.u("s")
        fail  = six.u("!")
        done  = six.u("+")
        start = six.u(" ")

    def __init__(self, *args, **kwargs):
        self.failed_results = list()
        self.n_open = 0
        self.multithread_mode = False


    def _msg(self, status, msg, c_r=False, visible=True):
        if self.n_open > 1 and not self.multithread_mode:
            self.multithread_mode = True
            if visible is True:
                sys.stderr.write(six.u('\n'))
        s = self.msg_str.format(status, self.n_complete, self.n_tasks,
                                (float(self.n_complete)/self.n_tasks)*100,
                                six.u(msg))
        if self.multithread_mode is True:
            s += six.u("\n")
        elif c_r:
            self.n_open -= 1
            s = six.u("\r") + s + six.u("\n")
        else:
            self.n_open += 1
        if visible is True:
            sys.stderr.write(s)


    def started(self, ctx):
        self.run_context = ctx
        self.reset()

    def task_started(self, task_no):
        self._msg(self.stats.start, self.run_context.tasks[task_no].name,
                  visible=self.run_context.tasks[task_no].visible)
    
    def task_skipped(self, task_no):
        self.n_complete += 1
        self._msg(self.stats.skip, self.run_context.tasks[task_no].name, True,
                  visible=self.run_context.tasks[task_no].visible)

    def task_failed(self, task_result):
        self.n_complete += 1
        if task_result.task_no is None:
            return
        name = self.run_context.tasks[task_result.task_no].name
        self.failed_results.append((name, task_result))
        self._msg(self.stats.fail, name, True,
                  visible=self.run_context.tasks[task_result.task_no].visible)

    def task_completed(self, task_result):
        self.n_complete += 1
        name = self.run_context.tasks[task_result.task_no].name
        self._msg(self.stats.done, name, True,
                  visible=self.run_context.tasks[task_result.task_no].visible)

    def finished(self):
        sys.stderr.write(six.u("Run Finished\n"))
        for name, result in self.failed_results:
            sys.stderr.write(six.u("Task {} failed\n".format(result.task_no)))
            sys.stderr.write(six.u("  Name: "+name+"\n"))
            sys.stderr.write(six.u("  Original error: \n"))
            for line in result.error.split("\n"):
                sys.stderr.write(six.u("  "+line+"\n"))
        self.reset()

    def reset(self):
        self.n_tasks = len(self.run_context.tasks)
        self.n_complete = 0
        self.failed = False



class LoggerReporter(BaseReporter):
    """A reporter that uses :mod:`logging`.

    :param loglevel_str: The logging level. Valid levels: subdebug,
      debug, info, subwarning, warning, error
    :type loglevel_str: str

    :param logfile: The file to log to. Defaults to stdout.
    :type logfile: str or file-like

    :param fmt_str: The log format. See :mod:`logging` for more
      information
    :type fmt_str: str

    """

    FORMAT = "%(asctime)s\t%(name)s\t%(funcName)s\t%(levelname)s: %(message)s"
    def __init__(self, loglevel_str="", logfile=None,
                 fmt_str=None, *args, **kwargs):
        self.logger = logging.getLogger(self.__class__.__name__)
        loglevel = getattr(logging, loglevel_str.upper(), logging.WARNING)
        logkwds = {"format": fmt_str or self.FORMAT,
                  "level":  loglevel }
        if logfile and hasattr(logfile, "write"):
            logkwds['stream'] = logfile
        elif logfile and isinstance(logfile, six.string_types):
            logkwds['filename'] = logfile
        logging.basicConfig(**logkwds)
        self.any_failed = False

    def _daginfo(self, task_no):
        children = self.run_context.dag.successors(task_no)
        parents = self.run_context.dag.predecessors(task_no)
        msg = " {} parents: {}.  {} children: {}."
        return msg.format(len(parents), parents, len(children), children)

    def started(self, ctx):
        self.run_context = ctx
        self.logger.info("Beginning AnADAMA run with %i tasks.",
                         len(self.run_context.tasks))

    def task_skipped(self, task_no):
        msg = "task %i `%s' skipped." + self._daginfo(task_no)
        self.logger.info(msg, task_no,
                         self.run_context.tasks[task_no].name)

    def task_started(self, task_no):
        msg = "task %i, `%s' started." + self._daginfo(task_no)
        self.logger.info(msg, task_no,
                         self.run_context.tasks[task_no].name)

    def task_failed(self, task_result):
        n = task_result.task_no
        if n is None:
            self.logger.error("task %s, `Unknown' failed! Error generated: %s",
                              n, task_result.error)
        else:
            self.logger.error("task %i, `%s' failed! Error generated: %s",
                              n, self.run_context.tasks[n].name, task_result.error)
        self.any_failed = True

    def task_completed(self, task_result):
        n = task_result.task_no
        self.logger.info("task %i, `%s' completed successfully.",
                         n, self.run_context.tasks[n])

    def finished(self):
        if self.any_failed:
            self.logger.error("AnADAMA run finished with errors.")
        else:
            self.logger.info("AnADAMA run finished.")

        



class WebhookReporter(BaseReporter):
    """TODO"""
    pass
