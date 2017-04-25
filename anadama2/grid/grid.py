# -*- coding: utf-8 -*-

import threading
import Queue

from .. import runners
from ..helpers import format_command

class GridJobRequires(object):
    """Defines the resources required for a task on the grid.

    :param time: Wall clock time in minutes.
    :type time: int

    :param mem: RAM Usage in MB (8*1024*1024 bits).
    :type mem: int

    :param cores: CPU cores.
    :type cores: int
    """
    
    def __init__(self, time, mem, cores, depends=None):
        # if time is not an int, try to format the equation
        if not str(time).isdigit():
            self.time = format_command(time, depends=depends, cores=cores)
        else:
            self.time = int(time)
        
        # if memory is not an int, try to format the equation
        if not str(mem).isdigit():
            self.mem = format_command(mem, depends=depends, cores=cores) 
        else:
            self.mem = int(mem)
            
        self.cores = int(cores) 
    
class Grid(object):
    """ Base Grid Workflow manager class """
    
    def __init__(self, name, worker, queue, partition, tmpdir, benchmark_on=None):
        self.name = name
        self.worker = worker
        self.queue = queue
        self.partition = partition
        self.tmpdir = tmpdir
        # create the folder if it does not already exist for temp directory
        if not os.path.isdir(self.tmpdir):
            os.makedirs(self.tmpdir)
        
        self.task_data = dict()

    def _get_grid_task_settings(self, kwargs, depends):
        """ Get the resources required to run this task on the grid """
        
        # check for the required keywords
        requires=[]
        for key in ["time","mem","cores"]:
            try:
                requires.append(kwargs[key])
            except KeyError:
                raise KeyError(key+" is a required keyword argument for a grid task")
        requires+=[depends]
        
        return (GridJobRequires(*requires), self.partition, self.tmpdir)
        
    def do(self, task, **kwargs):
        """Accepts the following extra arguments:
        
        :param time: The maximum time in minutes allotted to run the
          command
        :type time: int

        :param mem: The maximum memory in megabytes allocated to run
          the command
        :type mem: int

        :param cores: The number of CPU cores allocated to the job
        :type cores: int

        :param partition: The grid partition to send this job to
        :type partition: str
        """
        
        self.add_task(task, **kwargs)


    def add_task(self, task, **kwargs):
        """Accepts the following extra arguments:
        
        :keyword time: The maximum time in minutes allotted to run the
          command
        :type time: int

        :keyword mem: The maximum memory in megabytes allocated to run
          the command
        :type mem: int

        :keyword cores: The number of CPU cores allocated to the job
        :type cores: int

        :keyword partition: The grid partiton to send this job to
        :type partition: str
        """
        
        self.task_data[task.task_no] = self._get_grid_task_settings(kwargs, task.depends)


    def runner(self, workflow, jobs=1, grid_jobs=1):
        runner = runners.GridRunner(workflow)
        runner.add_worker(runners.ParallelLocalWorker,
                          name="local", rate=jobs, default=True)
        runner.add_worker(self.worker, name=self.name, rate=grid_jobs)
        runner.routes.update((
            ( task_no, (self.name, list(extra)+[self.queue, workflow._reporter]) )
            for task_no, extra in six.iteritems(self.task_data)
        ))
        return runner   


class GridWorker(threading.Thread):
    """ Base Grid Worker class """
    
    def __init__(self, work_q, result_q, lock, reporter):
        super(GridWorker, self).__init__()
        self.daemon = True
        self.logger = runners.logger
        self.work_q = work_q
        self.result_q = result_q
        self.lock = lock
        self.reporter = reporter    
    
    @staticmethod
    def appropriate_q_class(*args, **kwargs):
        return queue.Queue(*args, **kwargs)    

    @staticmethod
    def appropriate_lock():
        return threading.Lock() 
    
    def run(self):
        raise NotImplementedError
    
