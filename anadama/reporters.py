import sys

def default(run_context):
    return ConsoleReporter(run_context)

class BaseReporter(object):

    """The base reporter defines functionality similar amongst all
    reporters.

    The runcontext that executes the hooks defined can be accessed at
    ``self.run_context``.

    """

    def __init__(self, run_context):
        self.run_context = run_context

    def started(self):
        """Executed when a run is started, usually when
        :meth:`anadama.runcontext.RunContext.go` is executed.

        """

        raise NotImplementedError()

    def task_skipped(self, task_no):
        """Executed when anadama determines that a task needn't be run.

        :param task_no: The task number of the task that is
          skipped. To get the actual :class:`anadama.Task` object
          that's being skipped, do ``self.run_context.tasks[task_no]``.

        :type task_no: int

        """

        raise NotImplementedError()

    def task_started(self, task_no):
        """Executed when anadama is just about to execute a task.

        :param task_no: The task number of the task that is
          being started. To get the actual :class:`anadama.Task` object
          that's being executed, do ``self.run_context.tasks[task_no]``.

        :type task_no: int

        """

        raise NotImplementedError()

    def task_failed(self, task_result):
        """Executed when a task fails.

        :param task_no: The task number of the task that failed . To
          get the actual :class:`anadama.Task` object that failed, do
          ``self.run_context.tasks[task_no]``. To get the task result
          of the task that failed,
          do``self.run_context.task_results[task_no]``

        :type task_no: int

        """
        raise NotImplementedError()        

    def task_completed(self, task_result):
        """Executed when a task completes with no errors.

        :param task_no: The task number of the task that succeeded . To
          get the actual :class:`anadama.Task` object that succeeded, do
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

      from anadama.reporters import ReporterGroup
      my_grouped_reporter = ReporterGroup([custom_reporter_a, 
                                           custom_reporter_b, 
                                           custom_reporter_c])
      ...
      ctx.go(reporter=my_grouped_reporter)

    """

    def __init__(self, other_reporters):
        self.reps = other_reporters


    def started(self):
        for r in self.reps:
            r.started()


    def task_skipped(self, task_no):
        for r in self.reps:
            r.task_skipped(task_no)


    def task_started(self, task_no):
        for r in self.reps:
            r.task_started(task_no)


    def task_failed(self, task_no):
        for r in self.reps:
            r.task_failed(task_no)


    def task_completed(self, task_no):
        for r in self.reps:
            r.task_completed(task_no)


    def finished(self):
        for r in self.reps:
            r.finished()

    

class ConsoleReporter(BaseReporter):
    """The default reporter. Prints out run progress to stderr.
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
         :meth:`anadama.runcontext.RunContext.add_task`.

    """


    msg_str = "({:.1})[{:3}/{:3} - {:6.2f}%] {:.57}"

    class stats:
        skip = "s"
        fail = "!"
        done = "+"
        start = " "

    def __init__(self, *args, **kwargs):
        super(ConsoleReporter, self).__init__(*args, **kwargs)
        self.failed_results = list()
        self.n_open = 0
        self.multithread_mode = False


    def _msg(self, status, msg, c_r=False):
        if self.n_open > 1 and not self.multithread_mode:
            self.multithread_mode = True
            sys.stderr.write('\n')
        s = self.msg_str.format(status, self.n_complete, self.n_tasks,
                                (float(self.n_complete)/self.n_tasks)*100, msg)
        if self.multithread_mode is True:
            s += "\n"
        elif c_r:
            self.n_open -= 1
            s = "\r" + s + "\n"
        else:
            self.n_open += 1
        sys.stderr.write(s)
        

    def started(self):
        self.reset()

    def task_started(self, task_no):
        self._msg(self.stats.start, self.run_context.tasks[task_no].name)
    
    def task_skipped(self, task_no):
        self.n_complete += 1
        self._msg(self.stats.skip, self.run_context.tasks[task_no].name, True)

    def task_failed(self, task_result):
        self.n_complete += 1
        name = self.run_context.tasks[task_result.task_no].name
        self.failed_results.append((name, task_result))
        self._msg(self.stats.fail, name, True)

    def task_completed(self, task_result):
        self.n_complete += 1
        name = self.run_context.tasks[task_result.task_no].name
        self._msg(self.stats.done, name, True)

    def finished(self):
        print >> sys.stderr, "Run Finished"
        for name, result in self.failed_results:
            print >> sys.stderr, "Task {} failed".format(result.task_no)
            print >> sys.stderr, "  Name: "+name
            print >> sys.stderr, "  Original error: "
            for line in result.error.split("\n"):
                print >> sys.stderr, "  "+line
        self.reset()

    def reset(self):
        self.n_tasks = len(self.run_context.tasks)
        self.n_complete = 0
        self.failed = False




class LoggerReporter(BaseReporter):
    """TODO"""
    pass



class WebhookReporter(BaseReporter):
    """TODO"""
    pass
