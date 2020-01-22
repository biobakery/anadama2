import os
import sys
import logging
import multiprocessing
import collections

import six
import string
import time

from .util import mkdirp
from .tracked import TrackedExecutable, s3_folder

LOG_FILE_NAME = "anadama.log"

def default(output_dir=None, log_level=None):
    log = LOG_FILE_NAME
    if not output_dir or s3_folder(output_dir):
        output_dir=os.getcwd()
    log = os.path.abspath(os.path.join(output_dir, log))
    return ReporterGroup([
        LoggerReporter(log_level, log),
        VerboseConsoleReporter()
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
        """Executed when anadama is just about to execute a task. These tasks
        are in the queue and waiting for available resources.

        :param task_no: The task number of the task that is
          being started. To get the actual :class:`anadama2.Task` object
          that's being executed, do ``self.run_context.tasks[task_no]``.

        :type task_no: int

        """

        raise NotImplementedError()

    def task_running(self, task_no):
        """Executed when anadama is ready to run a task.

        :param task_no: The task number of the task that is
          being started. To get the actual :class:`anadama2.Task` object
          that's being executed, do ``self.run_context.tasks[task_no]``.

        :type task_no: int

        """

        raise NotImplementedError()
    
    def task_command(self, task_no):
        """Executed when anadama is ready to run a task. Logs the command
            and version of any tracked executables.

        :param task: The task number of the task that is
          being started. To get the actual :class:`anadama2.Task` object
          that's being executed, do ``self.run_context.tasks[task_no]``.

        :type task_no: int

        """

        pass

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

    def task_grid_status(self, task_no, grid_id, status_message):
        """Executed when anadama has grid information for a task. These messages 
        are reported when the status for a grid task has changed.

        :param task_no: The task number of the task that is
          being started. To get the actual :class:`anadama2.Task` object
          that's being executed, do ``self.run_context.tasks[task_no]``.
        :param grid_id: The id of the grid job.
        :param status_message: The grid status message.

        :type task_no: int

        """

        raise NotImplementedError()
    
    def task_grid_status_polling(self, task_no, grid_id, status_message):
        """Executed when anadama has grid information for a task at set polling 
        intervals. Status may repeat at each interval.

        :param task_no: The task number of the task that is
          being started. To get the actual :class:`anadama2.Task` object
          that's being executed, do ``self.run_context.tasks[task_no]``.
        :param grid_id: The id of the grid job.
        :param status_message: The grid status message.

        :type task_no: int

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

    def task_running(self, task_no):
        for r in self.reps:
            r.task_running(task_no)
            
    def task_command(self, task_no):
        for r in self.reps:
            r.task_command(task_no)

    def task_failed(self, task_result):
        for r in self.reps:
            r.task_failed(task_result)


    def task_completed(self, task_result):
        for r in self.reps:
            r.task_completed(task_result)


    def finished(self):
        for r in self.reps:
            r.finished()

    def task_grid_status(self, task_no, grid_id, status_message):
        for r in self.reps:
            r.task_grid_status(task_no, grid_id, status_message)

    def task_grid_status_polling(self, task_no, grid_id, status_message):
        for r in self.reps:
            r.task_grid_status_polling(task_no, grid_id, status_message)

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

class VerboseConsoleReporter(BaseReporter):
    """Prints out verbose run progress to stdout.
    An example readout is as follows:

    ::

    DATE/TIME [ 0/18 -   0.00%] **Started  ** Task  2: kneaddata
    DATE/TIME [ 0/18 -   0.00%] **Started  ** Task  0: kneaddata
    DATE/TIME [ 1/18 -   5.56%] **Completed** Task  0: kneaddata
    DATE/TIME [ 1/18 -   5.56%] **Started  ** Task  4: metaphlan2.py
    DATE/TIME [ 2/18 -  11.11%] **Completed** Task  2: kneaddata

    The readout is composed of five pieces of information:
    
      1. The date/time for the status message.
      2. The status of all tasks. For example, [1/4 - 25%] indicates that 
          one task of the four total tasks have finished running. The
          workflow is 25% complete.
      3. The step the task has completed. Examples are "Started" and "Completed".
      4. The task number. 
      5. The task description. This is the task name if set. If the task name is
          the default then it is the first task action. This is the first command
          and it is limited to the executable name. If it is a function, it will be the 
          name of the function.
    """

    class stats:
        skip  = six.u("Skipped")
        fail  = six.u("Failed")
        done  = six.u("Completed")
        start = six.u("Started")
        ready = six.u("Ready")
        grid_run = six.u("GridJob")
        max_message_length = max(len(x) for x in [skip,fail,done,start,ready,grid_run])

    def __init__(self, *args, **kwargs):
        self.failed_results = list()
        # Use a multiprocessing value so the total tasks completed is shared
        # for the local multiprocessing tasks (is process and thread-safe)
        self.n_complete = multiprocessing.Value('i',0)
        
        # Set the max length for the total output line
        self.max_length=120

    def _msg(self, status, task_name, description, id, visible=True, grid_update=None):
        # create a date/time string
        s = time.strftime("(%b %d %H:%M:%S) ", time.localtime())
        
        # create a message string for the current status
        s += self.msg_str.format(self.n_complete.value, self.n_tasks,
                                (float(self.n_complete.value)/self.n_tasks)*100, 
                                status, id, description)
        # if command string is reduced, add ellipses
        if len(description) > self.max_command_length:
            s += " ..."
            
        if grid_update:
            s += self.grid_update_msg.format(grid_update[0], grid_update[1])
            
        s += six.u("\n")
        
        # only write if the task is visible
        if visible is True:
            sys.stdout.write(s)
            
    def _increment_complete(self, task_no):
        # update the number of completed visible tasks
        if self.run_context.tasks[task_no].visible:
            self.n_complete.value+=1

    def started(self, ctx):
        self.run_context = ctx
        self.reset()

    def task_started(self, task_no):
        # This indicates the task is ready and waiting in the queue
        self._msg(self.stats.ready, self.run_context.tasks[task_no].name,
                  self.run_context.tasks[task_no].description, task_no,
                  visible=self.run_context.tasks[task_no].visible)
        
    def task_running(self, task_no):
        self._msg(self.stats.start, self.run_context.tasks[task_no].name,
                  self.run_context.tasks[task_no].description, task_no,
                  visible=self.run_context.tasks[task_no].visible)
    
    def task_skipped(self, task_no):
        self._increment_complete(task_no)
        self._msg(self.stats.skip, self.run_context.tasks[task_no].name, 
                  self.run_context.tasks[task_no].description, task_no,
                  visible=self.run_context.tasks[task_no].visible)

    def task_failed(self, task_result):
        self._increment_complete(task_result.task_no)
        if task_result.task_no is None:
            return
        name = self.run_context.tasks[task_result.task_no].name
        self.failed_results.append((name, task_result))
        self._msg(self.stats.fail, name, self.run_context.tasks[task_result.task_no].description,
                  task_result.task_no, visible=self.run_context.tasks[task_result.task_no].visible)

    def task_completed(self, task_result):
        self._increment_complete(task_result.task_no)
        name = self.run_context.tasks[task_result.task_no].name
        self._msg(self.stats.done, name, self.run_context.tasks[task_result.task_no].description,
                  task_result.task_no, visible=self.run_context.tasks[task_result.task_no].visible)

    def task_grid_status(self, task_no, grid_id, status_message):
        self._msg(self.stats.grid_run, self.run_context.tasks[task_no].name,
                  self.run_context.tasks[task_no].description, task_no, visible=True,
                  grid_update=[grid_id, status_message])
        
    def task_grid_status_polling(self, task_no, grid_id, status_message):
        self.task_grid_status(task_no, grid_id, status_message)

    def finished(self):
        sys.stdout.write(six.u("Run Finished\n"))
        for name, result in self.failed_results:
            sys.stdout.write(six.u("Task {} failed\n".format(result.task_no)))
            sys.stdout.write(six.u("  Name: "+name+"\n"))
            sys.stdout.write(six.u("  Original error: \n"))
            for line in result.error.split("\n"):
                print_line="".join(filter(lambda x: x in string.printable, line))
                sys.stdout.write(six.u("  "+print_line+"\n"))
        self.reset()

    def reset(self):
        # count the number of visible tasks
        self.n_tasks = 0
        for task in self.run_context.tasks:
            if task.visible:
                self.n_tasks+=1
                
        # limit the full string length
        max_task_length=len(str(self.n_tasks))
        self.max_command_length = self.max_length -(20+max_task_length*3+20+self.stats.max_message_length)
        self.msg_str = six.u("[{:"+str(max_task_length)+"}/{:"+str(max_task_length)+
                             "} - {:6.2f}%] **{:"+str(self.stats.max_message_length)+
                             "}** Task {:"+str(max_task_length)+
                             "}: {:."+str(self.max_command_length)+"}")
        self.grid_update_msg = six.u(" <Grid JobId {:9}: {:.30}>")
        self.n_complete.value = 0
        self.failed = False
        
SHELL_COMMAND = "Executing with shell: "
VERSION_COMMAND = "Tracked executable version: "

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
    
    def __init__(self, loglevel_str=None, logfile=None,
                 fmt_str=None, *args, **kwargs):
        # create the log file folder if needed
        if logfile:
            mkdirp(os.path.dirname(logfile))        
        self.logger = logging.getLogger(self.__class__.__name__)
        self.loglevel_str = loglevel_str.upper() or logging.WARNING
        loglevel = getattr(logging, self.loglevel_str)
        logkwds = {"format": fmt_str or self.FORMAT,
                  "level":  loglevel }
        if logfile and hasattr(logfile, "write"):
            logkwds['stream'] = logfile
        elif logfile and isinstance(logfile, six.string_types):
            logkwds['filename'] = logfile
        logging.basicConfig(**logkwds)
        self.any_failed = False
        self.start_log_message="task %i, %s : %s "
        
    @classmethod
    def read_log(cls,file,type,remove_paths=True):
        """ Read the data from the log file """
        
        # read all of the lines from the log
        data=collections.OrderedDict()
        with open(file) as file_handle:
            lines=file_handle.readlines()
        
        # look for commands, benchmarking, or executable version information
        log_info={}
        if type == "commands":
            keyword=SHELL_COMMAND
            for line in lines:
                if keyword in line:
                    new_command=line.split(keyword)[-1].strip()
                    if remove_paths:
                        new_command=" ".join([os.path.split(i.rstrip(os.path.sep))[-1] for i in new_command.split(" ")])
                    data[new_command]=1
            log_info=list(data.keys())
        elif type == "benchmarking":
            benchmarking_info={}
            # read in the commands and also the benchmarking information
            for i in range(len(lines)):
                if "run_task_command" in lines[i]:
                    # get the id and the executable plus input item
                    id=lines[i].split()[-1].strip().replace(":","")
                    command_tokens=lines[i+1].strip().split()
                    executable=command_tokens[0]
                    try:
                        input=os.path.split(command_tokens[command_tokens.index("--input")+1])[-1]
                    except (ValueError, IndexError):
                        input="NA"
                    log_info[id]="\t".join([executable, input])
                elif "Benchmark" in lines[i]:
                    # get the job id and benchmarking info
                    id=lines[i].split()[-1].strip().replace(":","")
                    info="\t".join([lines[j].strip().split(": ")[-1] for j in [i+1,i+2,i+3]])
                    benchmarking_info[id]=info
                    
            # set the log info to include id, executable, input and benchmarking
            for id in benchmarking_info.keys():
                if id in log_info:
                    log_info[id]=log_info[id]+"\t"+benchmarking_info[id]
        elif type == "variables":
            # read in the variables set for the workflow
            # use the last variable values for workflow with more than one run
            for line in lines:
                if "started\tINFO:" in line and " = " in line:
                    data = line.rstrip().split(": ")[-1]
                    try:
                        variable, value = data.split(" = ")
                        log_info[variable]=value
                    except ValueError:
                        pass
        else:
            keyword = VERSION_COMMAND
            # remove redundant version from strings since these are already 
            # identified as version information
            format_output=lambda x: x.replace("Version:","").replace(", version","")
            for line in lines:
                if keyword in line:
                    data[format_output(line.split(keyword)[-1].strip())]=1
            log_info=list(data.keys())
                    
        if not log_info:
            log_info=["No {} found in log".format(type)]
                
        return log_info

    def _daginfo(self, task_no):
        children = self.run_context.dag.successors(task_no)
        parents = self.run_context.dag.predecessors(task_no)
        msg = " {} parents: {}.  {} children: {}."
        return msg.format(len(parents), parents, len(children), children)
    
    def log_event(self, msg, task_no, debug_msg=None):
        visible=self.run_context.tasks[task_no].visible
        description=self.run_context.tasks[task_no].description
        
        # If in debug level and debug message is provided, append to end of message
        if self.loglevel_str == "DEBUG" and debug_msg is not None:
            msg=msg+" : "+debug_msg
        
        # Write tasks that are not visible to log on debug level
        if visible:
            self.logger.info(self.start_log_message, task_no, description, msg)
        else:
            self.logger.debug(self.start_log_message, task_no, description, msg)

    def started(self, ctx):
        self.run_context = ctx
        self.logger.info("Beginning AnADAMA run with %i tasks.",
                         len(self.run_context.tasks))
        
        # if there are workflow variables set, add them to the log
        try:
            options=vars(ctx.vars.get_option_values())
        except (AttributeError, TypeError):
            options={}
           
        # write the description and version of the workflow to the log if provided
        if ctx.vars.description:
            self.logger.info("Workflow description = %s", ctx.vars.description)
        if ctx.vars.version:
            self.logger.info("Workflow version = %s", ctx.vars.version)
 
        if options:
            self.logger.info("Workflow configuration options")
        
        for name, value in options.items():
            self.logger.info("{} = {}".format(name, value))
            

    def task_skipped(self, task_no):
        self.log_event("skipped", task_no, self._daginfo(task_no))

    def task_started(self, task_no):
        self.log_event("ready and waiting for resources", task_no, self._daginfo(task_no))
        
    def task_running(self, task_no):
        self.log_event("starting to run",task_no,self._daginfo(task_no))
        
    def task_command(self, task_no):
        # if a tracked executable is found, then log the version
        for exe_depends in filter(lambda x: isinstance(x, TrackedExecutable), self.run_context.tasks[task_no].depends):
            version = exe_depends.version()
            if version:
                self.logger.info(VERSION_COMMAND+" {}".format(version.decode('utf-8')))
        
        # if a command, then log the shell command(s)
        if not list(filter(lambda x: six.callable(x), self.run_context.tasks[task_no].actions)):
            self.logger.info(SHELL_COMMAND+" "+" ".join(self.run_context.tasks[task_no].actions))

    def task_failed(self, task_result):
        self.logger.error(self.start_log_message,task_result.task_no,
                          self.run_context.tasks[task_result.task_no].description,
                          " Failed! Error message : {}".format(task_result.error))
        self.any_failed = True

    def task_grid_status(self, task_no, grid_id, status_message):
        self.log_event(" grid job id {} has status {}".format(grid_id, status_message),
            task_no, self.run_context.tasks[task_no].description)
        
    def task_grid_status_polling(self, task_no, grid_id, status_message):
        self.logger.debug(self.start_log_message,
            task_no, self.run_context.tasks[task_no].description, 
            " grid job id {} has status {}".format(grid_id, status_message))

    def task_completed(self, task_result):
        self.log_event("completed successfully",task_result.task_no)

    def finished(self):
        if self.any_failed:
            self.logger.error("AnADAMA run finished with errors.")
        else:
            self.logger.info("AnADAMA run finished.")

        



class WebhookReporter(BaseReporter):
    """TODO"""
    pass

