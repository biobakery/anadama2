# -*- coding: utf-8 -*-
import os
import re
import sys
import logging
import itertools
import threading
import string
import tempfile
import subprocess
import datetime
import time

from math import exp
from collections import namedtuple

import six
from six.moves import queue

from . import Dummy
from .. import runners
from .. import picklerunner
from ..util import underscore
from ..util import find_on_path
from ..util import keepkeys

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


class Slurm(Dummy):
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

    :keyword tmpdir: A directory to store temporary files in. All
      machines in the cluster must be able to read the contents of
      this directory; uses :mod:`anadama2.picklerunner` to create
      self-contained scripts to run individual tasks and calls
      ``srun`` to run the script on the cluster.
    :type tmpdir: str

    :type extra_srun_flags: list of str

    """

    def __init__(self, partition, tmpdir=None, extra_srun_flags=[]):
        self.slurm_partition = partition
        self.slurm_tmpdir = tmpdir
        if self.slurm_tmpdir:
            # create the folder if it does not already exist
            if not os.path.isdir(self.slurm_tmpdir):
                os.makedirs(self.slurm_tmpdir)
            
        self.extra_srun_flags = extra_srun_flags
        
        self.slurm_task_data = dict()
        
        self.slurm_queue = SLURMQueue()

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


    def _import(self, task_dict):
        slurm_keys = ["time", "mem", "cores", "partition", "extra_srun_flags"]
        keys_to_keep = ["actions", "depends", "targets",
                        "name", "interpret_deps_and_targs"]
        if any(k in task_dict for k in slurm_keys):
            return self.slurm_add_task(
                **keepkeys(task_dict, slurm_keys+keys_to_keep)
            )
        else:
            return self.add_task(**keepkeys(task_dict, keys_to_keep))


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

        :param partition: The SLURM partiton to send this job to
        :type partition: str

        :param extra_srun_flags: Any command-line flags to augment
          ``srun`` behavior formatted like ``--begin=22:00``,
          ``--exclusive``, or ``-k``
        :type extra_srun_flags: list of str

        """
        params = self._kwargs_extract(kwargs)
        self.slurm_task_data[task.task_no] = params

    
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

        :keyword partition: The SLURM partiton to send this job to
        :type partition: str

        :keyword extra_srun_flags: Any command-line flags to augment
          ``srun`` behavior formatted like ``--begin=22:00``,
          ``--exclusive``, or ``-k``
        :type extra_srun_flags: list of str

        """
        params = self._kwargs_extract(kwargs)
        self.slurm_task_data[task.task_no] = params


    def runner(self, ctx, jobs=1, grid_jobs=1):
        runner = runners.GridRunner(ctx)
        runner.add_worker(runners.ParallelLocalWorker,
                          name="local", rate=jobs, default=True)
        runner.add_worker(SLURMWorker, name="slurm", rate=grid_jobs)
        runner.routes.update((
            ( task_idx, ("slurm", list(extra)+[self.slurm_queue, ctx._reporter]) )
            for task_idx, extra in six.iteritems(self.slurm_task_data)
        ))
        return runner

class SLURMQueue():
    
    def __init__(self):
        # this is the refresh rate for checking the queue, in seconds
        self.refresh_rate = 5*60
        # this is the last time the queue was checked
        self.last_check = time.time()
        self.sacct = None
        # create a lock for jobs in queue
        self.lock_status = threading.Lock()
        self.lock_submit = threading.Lock()
        
    def get_slurm_status(self, refresh=None):
        """ Get the queue accounting stats """
        
        # lock to prevent race conditions with status update
        self.lock_status.acquire()
        
        # check the last time the queue was captured and refresh if set
        current_time = time.time()
        if ( current_time - self.last_check > self.refresh_rate ) or refresh or self.sacct is None:
            self.last_check = current_time
            self.sacct = self._run_slurm_sacct()
            
        self.lock_status.release()
        
        return self.sacct
            
    def _run_slurm_sacct(self):
        """ Check the status of the slurm ids """
        logging.info("Running slurm sacct")
        stdout=subprocess.check_output(["sacct","-o","JobID,State,AllocCPUs,Elapsed,MaxRSS"])

        # remove the header information from the status lines
        # split each line and remove empty lines
        info=filter(lambda x: x, [line.rstrip().split() for line in stdout.split("\n")[2:]])

        return list(info)
    
    def _get_all_stats_for_jobid(self,jobid):
        """ Get all the stats for a specific job id """
        
        # use the existing stats, to get the information for the jobid
        job_stats=list(filter(lambda x: x[0].startswith(jobid),self.get_slurm_status()))
       
        # if the job stats are not found for the job, return an NA state
        if not job_stats:
            job_stats=[[jobid,"Waiting","NA","NA","NA"]] 

        return job_stats
    
    def get_status(self, jobid):
        """ Check the status of the job """
        
        info=self._get_all_stats_for_jobid(jobid)
        
        return info[0][1]

    def get_benchmark(self,jobid):
        """ Check the benchmarking stats of the slurm id """

        # if the job is not shown to have finished running then
        # wait for the next queue refresh
        status=self.get_status(jobid)
        if not (_job_stopped(status) or _job_failed(status)):
            wait_time = abs(self.refresh_rate - (time.time() - self.last_check)) + 10
            time.sleep(wait_time)

        info=self._get_all_stats_for_jobid(jobid)

        try:
            cpus=info[0][2]
        except IndexError:
            cpus="NA"
    
        try:
            elapsed=info[0][3]
        except IndexError:
            elapsed="NA"

        # get the memory max from the batch line which is the second line of output
        try:
            memory=info[1][4]
        except IndexError:
            memory="NA"
        
        if "K" in memory:    
            # if memory is in KB, convert to MB
            memory="{:.1f}".format(int(memory.replace("K",""))/1024.0)
        elif "M" in memory:
            memory=memory.replace("M","")
        elif "G" in memory:
            # if memory is in GB, convert to MB
            memory="{:.1f}".format(int(memory.replace("G",""))*1024.0)

        return elapsed, cpus, memory    

    def submit_job(self,slurm_script):
        """ Submit the slurm jobs and return the slurm job id """
        
        # lock so only one task submits jobs to the queue at a time
        self.lock_submit.acquire()

        # submit the job and get the slurm id
        logging.info("Submitting job to grid")
        stdout=subprocess.check_output(["sbatch",slurm_script])
        slurm_jobid=stdout.rstrip().split()[-1]
        
        # pause for the scheduler
        time.sleep(5)
        
        self.lock_submit.release()
    
        return slurm_jobid

class SLURMWorker(threading.Thread):

    def __init__(self, work_q, result_q):
        super(SLURMWorker, self).__init__()
        self.logger = runners.logger
        self.work_q = work_q
        self.result_q = result_q

    @staticmethod
    def appropriate_q_class(*args, **kwargs):
        return queue.Queue(*args, **kwargs)

    def run(self):
        return runners.worker_run_loop(self.work_q, self.result_q, 
                                       _run_task_slurm)
        
def _run_task_slurm(task, extra):
    # if the task is a function, then use pickle srun interface
    if six.callable(task.actions[0]):
        return _run_task_function_slurm(task, extra)
    else:
        return _run_task_command_slurm(task, extra)
        
def _create_slurm_script(partition,cpus,minutes,memory,command,taskid,dir):
    """ Create a slurm script from the template also creating temp stdout and stderr files """
    
    slurm_template = string.Template("\n".join(["#!/bin/bash ",
        "#SBATCH -p ${partition}",
        "#SBATCH -N 1 ",
        "#SBATCH -n ${cpus}",
        "#SBATCH -t ${time}",
        "#SBATCH --mem ${memory}",
        "#SBATCH -o ${output}",
        "#SBATCH -e ${error}",
        "",
        "${command}",
        "${rc_command}"]))   

    # create temp files for stdout, stderr, and return code    
    handle_out, out_file=tempfile.mkstemp(suffix=".out",prefix="task_"+str(taskid)+"_",dir=dir)
    os.close(handle_out)
    handle_err, error_file=tempfile.mkstemp(suffix=".err",prefix="task_"+str(taskid)+"_",dir=dir)
    os.close(handle_err)
    handle_rc, rc_file=tempfile.mkstemp(suffix=".rc",prefix="task_"+str(taskid)+"_",dir=dir)
    os.close(handle_rc)

    # convert the minutes to the time string "D-HH:MM:SS"
    time=str(datetime.timedelta(minutes=minutes)).replace(' day, ','-')

    slurm=slurm_template.substitute(partition=partition,cpus=cpus,time=time,
        memory=memory,command=command,output=out_file,error=error_file,rc_command="echo $? > "+rc_file)
    file_handle, slurm_file=tempfile.mkstemp(suffix=".slurm",prefix="task_"+str(taskid)+"_",dir=dir)
    os.write(file_handle,slurm)
    os.close(file_handle)
    
    return slurm_file, out_file, error_file, rc_file

def _log_slurm_output(taskid, file, file_type):
    """ Write the slurm stdout/stderr files to the log """
    
    try:
        lines=open(file).readlines()
    except EnvironmentError:
        lines=[]
        
    logging.info("Slurm %s from task id %s:\n%s",taskid, file_type, "".join(lines))

def _get_return_code(file):
    """ Read the return code from the file """

    try:
        line=open(file).readline().rstrip()
    except EnvironmentError:
        line=""

    return line

def _job_stopped(status):
    # check if the job has a status which indicates it stopped running
    if status.startswith("CANCELLED") or status in ["COMPLETED","FAILED","TIMEOUT"]:
        # This will capture "CANCELLED by 0" and the short form "CANCELLED+"
        return True
    else:
        return False

def _job_failed(status):
    # check if the job has a status that it failed
    if status.startswith("CANCELLED") or status in ["MEMKILL","FAILED","TIMEOUT"]:
        return True
    else:
        return False

def _run_task_command_slurm(task, extra):
    (perf, partition, tmpdir, extra_srun_flags, slurm_queue, reporter) = extra
    # create a slurm script and stdout/stderr files for this task
    commands="\n".join(task.actions)
    logging.info("Running commands for task id %s:\n%s", task.task_no, commands)

    resubmission = 0    
    cores, time, memory = perf.cores, perf.time, perf.mem

    slurm_jobid, out_file, error_file, rc_file = _submit_slurm_job(cores, time, memory, 
        partition, tmpdir, commands, task, slurm_queue, reporter)

    result, slurm_final_status = _monitor_slurm_job(slurm_queue, task, slurm_jobid,
        out_file, error_file, rc_file, reporter)

    # if a timeout or memory max, resubmit at most three times
    while slurm_final_status in ["TIMEOUT","MEMKILL"] and resubmission < 3:
        reporter.task_grid_status(task.task_no,slurm_jobid,"Resubmit due to "+slurm_final_status)
        resubmission+=1
        # increase the memory or the time
        if slurm_final_status == "TIMEOUT":
            time = time * 2
            logging.info("Resubmission number %s of slurm job for task id %s with 2x more time: %s minutes", 
                resubmission, task.task_no, time)
        elif slurm_final_status == "MEMKILL":
            memory = memory * 2
            logging.info("Resubmission number %s of slurm job for task id %s with 2x more memory: %s MB",
                resubmission, task.task_no, memory)
        
        slurm_jobid, out_file, error_file, rc_file = _submit_slurm_job(cores, time, memory,
            partition, tmpdir, commands, task, slurm_queue, reporter)

        result, slurm_final_status = _monitor_slurm_job(slurm_queue, task, slurm_jobid,
            out_file, error_file, rc_file, reporter)

    # get the benchmarking data
    elapsed, cpus, memory = slurm_queue.get_benchmark(slurm_jobid)
    logging.info("Benchmark information for job id %s:\nElapsed: %s minutes\nCPUs: %s\nMEMORY: %s MB",
        task.task_no, elapsed,cpus,memory)
    
    return result

def _submit_slurm_job(cores, time, memory, partition, tmpdir, commands, task, slurm_queue, reporter):
    slurm_script, out_file, error_file, rc_file = _create_slurm_script(partition,
        cores, time, memory, commands, task.task_no, tmpdir)

    logging.info("Created slurm files for task id %s: %s, %s, %s, %s",
        task.task_no, slurm_script, out_file, error_file, rc_file)

    # submit the job
    slurm_jobid = slurm_queue.submit_job(slurm_script)
    
    logging.info("Submitted job for task id %s: slurm id %s", task.task_no,
        slurm_jobid)
    
    reporter.task_grid_status(task.task_no,slurm_jobid,"Submitted")
   
    return slurm_jobid, out_file, error_file, rc_file

def _monitor_slurm_job(slurm_queue, task, slurm_jobid, out_file, error_file, rc_file, reporter): 
    # poll to check for status
    slurm_job_status=None
    for tries in itertools.count(1):
        # only check status at intervals
        time.sleep(60)
        
        # check the queue stats
        slurm_job_status = slurm_queue.get_status(slurm_jobid)
        reporter.task_grid_status(task.task_no,slurm_jobid,slurm_job_status)
        
        logging.info("Status for job id %s with slurm id %s is %s",task.task_no,
            slurm_jobid,slurm_job_status)
        
        if _job_stopped(slurm_job_status):
            logging.info("Slurm status for job id %s shows it has stopped",task.task_no)
            break
        
        # check if the return code file is written
        if os.path.getsize(rc_file) > 0:
            logging.info("Return code file for job id %s shows it has stopped",task.task_no)
            break
        
    # check if a slurm error is written to the output file
    try:
        slurm_errors=subprocess.check_output(["grep","-F","slurmstepd: error:",error_file]).split("\n")
    except (EnvironmentError, subprocess.CalledProcessError):
        slurm_errors=[]
            
    if slurm_errors:
        # check for time or memory
        if list(filter(lambda x: "TIME LIMIT" in x and "CANCELLED" in x, slurm_errors)):
            logging.info("Slurm task %s cancelled due to time limit", slurm_jobid)
            # This has the slurm status of "TIMEOUT" from slurm sacct
            slurm_job_status="TIMEOUT"
        elif list(filter(lambda x: "exceeded memory limit" in x and "being killed" in x, slurm_errors)):
            logging.info("Slurm task %s cancelled due to memory limit", slurm_jobid)
            # This has the slurm status of "CANCELLED by 0" from slurm sacct (short form is "CANCELLED+")
            slurm_job_status="MEMKILL"
    
    # write the stdout and stderr to the log
    _log_slurm_output(task.task_no, out_file, "standard output")
    _log_slurm_output(task.task_no, error_file, "standard error")
    _log_slurm_output(task.task_no, rc_file, "return code")
    
    # check the return code
    extra_error=""
    return_code=_get_return_code(rc_file)
    if return_code and not return_code == "0":
        extra_error="Return Code Error: " + return_code
      
    # check the queue status
    if _job_failed(slurm_job_status):
        extra_error+="SLURM Status Error: " + slurm_job_status
 
    # get the anadama task result
    result=runners._get_task_result(task)

    # add the extra error if found
    if extra_error:
        result = result._replace(error=str(result.error)+extra_error)
 
    return result, slurm_job_status

def _run_task_function_slurm(task, extra):
    (perf, partition, tmpdir, extra_srun_flags, slurm_queue, reporter) = extra
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

