# -*- coding: utf-8 -*-
import os
import re
import sys
import logging
import itertools
import time

from math import exp

import six

from .grid import Grid
from .grid import GridWorker
from .grid import GridQueue

from .. import runners
from .. import picklerunner

from ..util import underscore
from ..util import find_on_path
from ..util import keepkeys

if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

sigmoid = lambda t: 1/(1-exp(-t))


class Slurm(Grid):
    """This class enables the Workflow class to dispatch tasks to
    SLURM. Use it like so:

    .. code:: python

      from anadama2 import Workflow
      from anadama2.slurm import Slurm

      powerup = Slurm(partition="general")
      ctx = Workflow(grid=powerup)
      ctx.do("wget "
             "ftp://public-ftp.hmpdacc.org/"
             "HMMCP/finalData/hmp1.v35.hq.otu.counts.bz2 "
             "-O @{input/hmp1.v35.hq.otu.counts.bz2}")

      # run on slurm with 200 MB of memory, 4 cores, and 60 minutes
      t1 = ctx.grid_do("pbzip2 -d -p 4 < #{input/hmp1.v35.hq.otu.counts.bz2} "
                       "> @{input/hmp1.v35.hq.otu.counts}",
                       mem=200, cores=4, time=60)

      # run on slurm on the serial_requeue partition
      ctx.grid_add_task("some_huge_analysis {depends[0]} {targets[0]}",
                        depends=t1.targets, targets="output.txt",
                        mem=4000, cores=1, time=300, partition="serial_requeue")


      ctx.go()


    :param partition: The name of the SLURM partition to submit tasks to
    :type partition: str

    :param tmpdir: A directory to store temporary files in. All
      machines in the cluster must be able to read the contents of
      this directory; uses :mod:`anadama2.picklerunner` to create
      self-contained scripts to run individual tasks and calls
      ``srun`` to run the script on the cluster.
    :type tmpdir: str
    
    :keyword benchmark_on: Option to turn on/off benchmarking
    : type benchmark_on: bool

    """

    def __init__(self, partition, tmpdir, benchmark_on=None):
        super(Slurm, self).__init__("slurm", SLURMWorker, SLURMQueue(benchmark_on), partition, tmpdir, benchmark_on)

class SLURMWorker(GridWorker):

    def __init__(self, work_q, result_q, lock, reporter):
        super(SLURMWorker, self).__init__(work_q, result_q, lock, reporter)
        
    @staticmethod
    def run_task_function(task, extra):
        (perf, partition, tmpdir, grid_queue, reporter) = extra
        # report the task has started
        reporter.task_running(task.task_no)
        script_path = picklerunner.tmp(task, dir=tmpdir).path
        job_name = "task{}:{}".format(task.task_no, underscore(task.name))
        mem, time = perf.mem, perf.time
        for tries in itertools.count(1):
            rerun = False
            args = ["srun", "-v", "--export=ALL", "--partition="+partition,
                    "--mem={}".format(int(mem)),
                    "--time={}".format(int(time)),
                    "--cpus-per-task="+str(perf.cores),
                    "--job-name="+job_name]
            args += [script_path, "-p", "-r" ]
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
            logging.error("Unable to decode task result, \nOut: %s\nErr: %s",
                          out, err)
            result = None
        if proc.returncode != 0:
            extra_error += "Srun error: "+err+"\n"
        if result is None:
            return runners.TaskResult(task.task_no, extra_error or "srun failed",
                                      None, None)
        elif extra_error: # (result is not None) is implicit here
            result = result._replace(error=str(result.error)+extra_error)
        return result


class SLURMQueue(GridQueue):
    
    def __init__(self, benchmark_on=None):
       super(SLURMQueue, self).__init__(benchmark_on) 
    
    @staticmethod
    def submit_command(grid_script):    
        return ["sbatch",grid_script]
    
    @staticmethod
    def submit_template():
        template = [
            "#SBATCH -p ${partition}",
            "#SBATCH -N 1 ",
            "#SBATCH -n ${cpus}",
            "#SBATCH -t ${time}",
            "#SBATCH --mem ${memory}",
            "#SBATCH -o ${output}",
            "#SBATCH -e ${error}"]
        return template
    
    @staticmethod
    def job_failed(status):
        # check if the job has a status that it failed
        return True if status.startswith("CANCELLED") or status in ["FAILED","TIMEOUT","MEMKILL"] else False
        
    @staticmethod
    def job_stopped(status):
        # check if the job has a status which indicates it stopped running
        # This will capture "CANCELLED by 0" and the short form "CANCELLED+"
        return True if status.startswith("CANCELLED") or status in ["COMPLETED","FAILED","TIMEOUT","MEMKILL"] else False
        
    @staticmethod
    def job_memkill(status):
        return True if status == "MEMKILL" else False
        
    @staticmethod
    def job_timeout(status):
        return True if status == "TIMEOUT" else False
    
    @staticmethod
    def get_job_status_from_stderr(error_file, grid_job_status):
        # read the error file to see if any time or memory errors were reported
        try:
            slurm_errors=subprocess.check_output(["grep","-F","slurmstepd: error:",error_file]).split("\n")
        except (EnvironmentError, subprocess.CalledProcessError):
            slurm_errors=[]
                
        if slurm_errors:
            # check for time or memory
            if list(filter(lambda x: "TIME LIMIT" in x and "CANCELLED" in x, slurm_errors)):
                logging.info("Slurm task %s cancelled due to time limit", grid_jobid)
                # This has the slurm status of "TIMEOUT" from slurm sacct
                grid_job_status="TIMEOUT"
            elif list(filter(lambda x: "exceeded memory limit" in x and "being killed" in x, slurm_errors)):
                logging.info("Slurm task %s cancelled due to memory limit", grid_jobid)
                # This has the slurm status of "CANCELLED by 0" from slurm sacct (short form is "CANCELLED+")
                grid_job_status="MEMKILL"
                
        return grid_job_status

    def refresh_queue_status(self):
        """ Get the latest status for the grid jobs using the same command for
        jobs in the queue and for completed jobs to benchmark """
        
        # Get the jobid, state, and resources for all jobs for the current user
        stdout=self.run_grid_command_resubmit(["sacct","-o","JobID,State,AllocCPUs,Elapsed,MaxRSS"])

        # remove the header information from the status lines
        # split each line and remove empty lines
        try:
            info=filter(lambda x: x, [line.rstrip().split() for line in stdout.split("\n")[2:]])
        except IndexError:
            info=[[""]]

        return list(info)
    
