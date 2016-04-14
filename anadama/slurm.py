import os
import re
import sys
import Queue
import itertools
import threading
from math import exp
from collections import namedtuple

from . import RunContext
from . import runners
from . import picklerunner

from .util import underscore
from .util import find_on_path

if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

available = bool(find_on_path("srun"))

sigmoid = lambda t: 1/(1-exp(-t))

class PerformanceData(namedtuple("PerformanceData", ["time", "mem", "cores"])):
    """Performance Data. Defines the resources or performance a task used,
    is limited to use, or is expected to use.

    :param time: Wall clock time in minutes.
    :type time: int

    :param mem: RAM Usage in MB (8*1024*1024 bits).
    :type mem: int

    :param cores: CPU cores.
    :type cores: int
    """
    pass # the class definition is just for the docstring


class SlurmContext(RunContext):
    """This class enables the RunContext class to dispatch tasks to
    SLURM. Use it like so:

    .. code:: python

      from anadama.runcontext.grid import SlurmContext

      ctx = SlurmContext(partition="general")
      ctx.do("wget "
             "ftp://public-ftp.hmpdacc.org/"
             "HMMCP/finalData/hmp1.v35.hq.otu.counts.bz2 "
             "-O @{input/hmp1.v35.hq.otu.counts.bz2}")

      # run on slurm with 200 MB of memory, 4 cores, and 60 minutes
      t1 = ctx.slurm_do("pbzip2 -d -p 4 < #{input/hmp1.v35.hq.otu.counts.bz2} "
                        "> @{input/hmp1.v35.hq.otu.counts}",
                        mem=200, cores=4, time=60)

      # run on slurm on the serial_requeue partition
      ctx.slurm_add_task("some_huge_analysis {depends[0]} {targets[0]}",
                         depends=t1.targets, targets="output.txt",
                         mem=4000, cores=1, time=300, partition="serial_requeue")


      ctx.go()


    :param partition: The name of the SLURM partition to submit tasks to
    :type partition: str

    :keyword tmpdir: A directory to store temporary files in. All
      machines in the cluster must be able to read the contents of
      this directory; uses :mod:`anadama.picklerunner` to create
      self-contained scripts to run individual tasks and calls
      ``srun`` to run the script on the cluster.
    :type tmpdir: str

    :type extra_srun_flags: list of str

    """

    def __init__(self, partition, tmpdir="/tmp", extra_srun_flags=[],
                 *args, **kwargs):
        super(SlurmContext, self).__init__(*args, **kwargs)
        self.slurm_partition = partition
        self.slurm_tmpdir = tmpdir
        self.extra_srun_flags = extra_srun_flags
        
        self.slurm_task_data = dict()


    def _kwargs_extract(self, kwargs_dict):
        time = kwargs_dict.pop("time", None)
        if time is None:
            raise TypeError("`time' is a required keyword argument")
        mem = kwargs_dict.pop("mem", None)
        if mem is None:
            raise TypeError("`mem' is a required keyword argument")
        cores = kwargs_dict.pop("cores", 1)
        partition = kwargs_dict.pop("partition", self.slurm_partition)
        extra_srun_flags = kwargs_dict.pop("extra_srun_flags",
                                           self.extra_srun_flags)
        return (PerformanceData(int(time), int(mem), int(cores)),
                partition, self.slurm_tmpdir, extra_srun_flags)


    def slurm_do(self, *args, **kwargs):
        params = self._kwargs_extract(kwargs)
        task = self.do(*args, **kwargs)
        self.slurm_task_data[task.task_no] = params
        return task

    
    def slurm_add_task(self, *args, **kwargs):
        params = self._kwargs_extract(kwargs)
        task = self.add_task(*args, **kwargs)
        self.slurm_task_data[task.task_no] = params
        return task


    def go(self, n_slurm_parallel=1, *args, **kwargs):
        kwargs.pop("runner", None) # ignore the runner keyword
        local_n_parallel = kwargs.pop("n_parallel", 1)
        runner = runners.GridRunner(self)
        runner.add_worker(runners.ParallelLocalWorker,
                          name="local", rate=local_n_parallel, default=True)
        runner.add_worker(SLURMWorker, name="slurm",
                          rate=n_slurm_parallel)
        runner.routes.update([
            ( task_idx, ("slurm", extra) )
            for task_idx, extra in self.slurm_task_data.iteritems()
        ])
        return super(SlurmContext, self).go(runner=runner, *args, **kwargs)
        


class SLURMWorker(threading.Thread):

    def __init__(self, work_q, result_q):
        super(SLURMWorker, self).__init__()
        self.logger = runners.logger
        self.work_q = work_q
        self.result_q = result_q

    @staticmethod
    def appropriate_q_class(*args, **kwargs):
        return Queue.Queue(*args, **kwargs)

    def run(self):
        return runners.worker_run_loop(self.work_q, self.result_q, 
                                       _run_task_slurm)



def _run_task_slurm(task, extra):
    (perf, partition, tmpdir, extra_srun_flags) = extra
    script_path = picklerunner.tmp(task, dir=tmpdir).path
    job_name = "task{}:{}".format(task.task_no, underscore(task.name))
    mem, time = perf.mem, perf.time
    for tries in itertools.count(1):
        rerun = False
        args = ["srun", "-v", "--export=ALL", "--partition="+partition,
                "--mem={:1g}".format(mem),
                "--time={:1g}".format(time),
                "--cpus-per-task="+str(perf.cores),
                "--job-name="+job_name]
        args += extra_srun_flags+[script_path, "-p", "-r" ]
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, 
                                stderr=subprocess.PIPE)
        out, err = proc.communicate()
        if "Exceeded job memory limit" in out+err:
            used = re.search(r'memory limit \((\d+) > \d+\)', out+err).group(1)
            mem = int(used)/1024 * 1.3
            rerun = True
        if re.search(r"due to time limit", out+err, re.IGNORECASE):
            time = time * (sigmoid(tries/10.)*2.7)
            rerun = True
        if not rerun:
            break
    extra_error = ""
    try:
        result = picklerunner.decode(out)
    except ValueError:
        extra_error += "Unable to decode task result\n"
        result = None
    if proc.returncode != 0:
        extra_error += "Srun error: "+err+"\n"
    if result is None:
        return runners.TaskResult(task.task_no, extra_error or "srun failed",
                                  None, None)
    elif extra_error: # (result is not None) is implicit here
        result = result._replace(error=result.error+extra_error)
    return result
        

