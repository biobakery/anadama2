# -*- coding: utf-8 -*-
import os
import sys
import time
import tempfile
import pwd

import six

from .grid import Grid
from .grid import GridWorker
from .grid import GridQueue

from .. import runners
from .. import picklerunner
from ..util import underscore
from ..util import find_on_path
from ..util import keyrename, keepkeys

if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

class SGE(Grid):
    """This class enables the Workflow class to dispatch tasks to
    Sun Grid Engine and its lookalikes. Use it like so:

    .. code:: python

      from anadama2 import Workflow
      from anadama2.sge import SGE

      ctx = Workflow(grid_powerup=SGE(queue="general"))
      ctx.do("wget "
             "ftp://public-ftp.hmpdacc.org/"
             "HMMCP/finalData/hmp1.v35.hq.otu.counts.bz2 "
             "-O @{input/hmp1.v35.hq.otu.counts.bz2}")

      # run on sge with 200 MB of memory, 4 cores, and 60 minutes
      t1 = ctx.grid_do("pbzip2 -d -p 4 < #{input/hmp1.v35.hq.otu.counts.bz2} "
                       "> @{input/hmp1.v35.hq.otu.counts}",
                       mem=200, cores=4, time=60)

      # run on sge on the serial_requeue queue
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
        super(SGE, self).__init__("sge", SGEWorker, SGEQueue(benchmark_on), partition, tmpdir, benchmark_on)


class SGEWorker(GridWorker):

    def __init__(self, work_q, result_q, lock, reporter):
        super(SGEWorker, self).__init__(work_q, result_q, lock, reporter)
        
    @staticmethod
    def run_task_function(task, extra):
        (perf, partition, tmpdir, slurm_queue, reporter) = extra
        script_path = picklerunner.tmp(task, dir=tmpdir).path
        job_name = "task{}.{}".format(task.task_no, underscore(task.name))
        tmpout = tempfile.mktemp(dir=tmpdir)
        tmperr = tempfile.mktemp(dir=tmpdir)
    
        args = ["qsub", "-R", "y", "-b", "y", "-sync", "y", "-N", job_name,
                "-pe", "smp", str(perf.cores), "-cwd", "-q", partition,
                "-V", "-l", "m_mem_free={:1g}M".format(max(1, perf.mem)),
                "-o", tmpout, "-e", tmperr]
        args += [script_path, "-r", "-p" ]
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, 
                                stderr=subprocess.PIPE)
        out, err = proc.communicate()
        extra_error = ""
        try:
            taskout = _wait_on_file(tmpout)
            if proc.returncode != 0:
                extra_error += _wait_on_file(tmperr)
        except Exception as e:
            e.task_no = task.task_no
            return runners.exception_result(e)
    
        try:
            result = picklerunner.decode(taskout)
        except ValueError:
            extra_error += "Unable to decode task result\n"
            result = None
        if proc.returncode != 0:
            extra_error += "Qsub error: "+err+"\n"
        if result is None:
            return runners.TaskResult(task.task_no, extra_error or "qsub failed",
                                      None, None)
        elif extra_error: # (result is not None) is implicit here
            result = result._replace(error=result.error+extra_error)
        return result

class SGEQueue(GridQueue):
    
    def __init__(self, benchmark_on=None):
       super(SLURMQueue, self).__init__(benchmark_on) 
       self.job_code_completed="c"
       self.job_code_error="e"
       self.job_code_terminated="t"
       
       self.all_failed_codes=[self.job_code_error,self.job_code_terminated]
       self.all_stopped_codes=[self.job_code_completed]+self.all_failed_codes
    
    @staticmethod
    def submit_command(grid_script):    
        return ["qsub",grid_script]
    
    @staticmethod
    def submit_template():
        template = [
            "#$$ -q ${partition}",
            "#$$ -pe smp ${cpus}",
            "#$$ -l h_rt=${time}",
            "#$$ -l h_vmem=${memory}m",
            "#$$ -o ${output}",
            "#$$ -e ${error}"]
        return template
    
    def job_failed(self,status):
        # check if the job has a status that it failed
        return True if status in self.all_failed_codes else False
        
    def job_stopped(self,status):
        # check if the job has a status which indicates it stopped running
        return True if status in self.all_stopped_codes else False
    
    def job_memkill(self, status, jobid, memory):
        # check if the job was killed because it used too much memory
        new_status, cpus, new_time, new_memory = self.get_benchmark(jobid)
        
        try:
            exceed_allocation = True if int(new_memory) > int(memory) else False
        except ValueError:
            exceed_allocation = False
            
        return True if exceed_allocation and new_status == self.job_code_terminated else False
            
    def job_timeout(self, status, jobid, time):
        # check if the job was killed because it used too much memory
        new_status, cpus, new_time, new_memory = self.get_benchmark(jobid)
        
        try:
            exceed_allocation = True if int(new_time) > int(time) else False
        except ValueError:
            exceed_allocation = False
            
        return True if exceed_allocation and new_status == self.job_code_terminated else False
            
    def refresh_queue_status(self):
        """ Get the latest status for the grid jobs using the same command for
        jobs in the queue and for completed jobs to benchmark """
        
        # Get the jobid and state for all jobs pending/running/completed for the current user
        qacct_stdout=self.run_grid_command_resubmit(["qacct","-o",pwd.getpwuid(os.getuid())[0],"-j","'*'"])
        
        # info list should include jobid, state, cpus, time, and maxrss
        info=[]
        job_status=[]
        for line in qacct_stdout.split("\n"):
            if line.startswith("jobnumber") or line.startswith("job_number"):
                if job_status:
                    info.append(job_status)
                job_status=[line.rstrip().split()[-1],"NA","NA","NA","NA"]
            # get the states for completed jobs
            elif line.startswith("failed"):
                failed_code = line.rstrip().split()[-1]
                if failed_code != "0":
                    if failed_code == "37":
                        job_status[1]=self.job_code_terminated
                    else:
                        job_status[1]=self.job_code_error
            elif line.startswith("exit_status"):
                # only record if status has not yet been set
                if job_status[1] == "NA":
                    if line.rstrip().split()[-1] == "0":
                        job_status[1]=self.job_code_completed
                    else:
                        job_status[1]=self.job_code_error
            # get the current state for running jobs
            elif line.startswith("job_state"):
                job_status[1]=line.rstrip().split()[-1]
            elif line.startswith("slots"):
                job_status[2]=line.rstrip().split()[-1]
            elif line.startswith("ru_wallclock"):
                job_status[3]=line.rstrip().split()[-1]
            elif line.startswith("maxvmem"):
                job_status[4]=line.rstrip().split()[-1]
        
        if job_status:
            info.append(job_status)

        return info

def _wait_on_file(fname, secs=30, pollfreq=0.1, rm=True):
    for _ in range(int(secs/pollfreq)):
        if os.path.exists(fname):
            with open(fname) as f:
                ret = f.read()
            if rm is True:
                os.unlink(fname)
            return ret
        else:
            time.sleep(pollfreq)
    raise OSError("Timed out waiting for "+fname+" to appear")





