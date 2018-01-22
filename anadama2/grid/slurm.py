# -*- coding: utf-8 -*-
import os
import re
import sys
import logging
import itertools
import time

import six

from .grid import Grid
from .grid import GridWorker
from .grid import GridQueue

if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

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
    
    :keyword options: Grid specific options to apply to each job
    :type options: str

    """

    def __init__(self, partition, tmpdir, benchmark_on=None, options=None, environment=None):
        super(Slurm, self).__init__("slurm", GridWorker, SLURMQueue(partition, benchmark_on, options, environment), tmpdir, benchmark_on)


class SLURMQueue(GridQueue):
    
    def __init__(self, partition, benchmark_on=None, options=None, environment=None):
        super(SLURMQueue, self).__init__(partition, benchmark_on)
        
        self.options=options
        self.environment=environment
        
        self.job_code_completed="COMPLETED"
        self.job_code_cancelled="CANCELLED"
        self.job_code_failed="FAILED"
        self.job_code_timeout="TIMEOUT"
        self.job_code_memkill="MEMKILL"
       
        self.all_failed_codes=[self.job_code_failed,self.job_code_timeout,self.job_code_memkill,self.job_code_cancelled]
        self.all_stopped_codes=[self.job_code_completed]+self.all_failed_codes
    
    @staticmethod
    def submit_command(grid_script):    
        return ["sbatch",grid_script]
    
    def submit_template(self):
        template = [
            "#SBATCH -p ${partition}",
            "#SBATCH -N 1 ",
            "#SBATCH -n ${cpus}",
            "#SBATCH -t ${time}",
            "#SBATCH --mem ${memory}",
            "#SBATCH -o ${output}",
            "#SBATCH -e ${error}"]
        
        # add user supplied options if provided
        if self.options:
            template+=["#SBATCH "+option for option in self.options]
            
        # add user supplied environment commands if provided
        if self.environment:
            template+=self.environment
            
        return template
    
    def job_failed(self,status):
        # check if the job has a status that it failed
        # This will capture "CANCELLED by 0" and the short form "CANCELLED+"
        return True if status.startswith(self.job_code_cancelled) or status in self.all_failed_codes else False
        
    def job_stopped(self,status):
        # check if the job has a status which indicates it stopped running
        # This will capture "CANCELLED by 0" and the short form "CANCELLED+"
        return True if status.startswith(self.job_code_cancelled) or status in self.all_stopped_codes else False
        
    def job_memkill(self, status, jobid, memory):
        return True if status == self.job_code_memkill else False
        
    def job_timeout(self, status, jobid, time):
        return True if status == self.job_code_timeout else False
    
    def get_job_status_from_stderr(self, error_file, grid_job_status, grid_jobid):
        # read the error file to see if any time or memory errors were reported
        try:
            slurm_errors=subprocess.check_output(["grep","-i","slurmstepd: error\|killed",error_file]).split("\n")
        except (EnvironmentError, subprocess.CalledProcessError):
            slurm_errors=[]
                
        if slurm_errors:
            # check for time or memory
            if list(filter(lambda x: "TIME LIMIT" in x and self.job_code_cancelled in x, slurm_errors)):
                logging.info("Slurm task %s cancelled due to time limit", grid_jobid)
                # This has the slurm status of "TIMEOUT" from slurm sacct
                grid_job_status=self.job_code_timeout
            elif list(filter(lambda x: "exceeded memory limit" in x and "being killed" in x, slurm_errors)) or \
                all([i in "\n".join(slurm_errors).lower() for i in ["exceeded","memory limit","killed"]]):
                logging.info("Slurm task %s cancelled due to memory limit", grid_jobid)
                # This has the slurm status of "CANCELLED by 0" from slurm sacct (short form is "CANCELLED+")
                # It might also have the slurm status of FAILED
                grid_job_status=self.job_code_memkill
                
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
            info=[]
            
        # now merge the lines for each job so there is only a single item for each job
        # the batch line includes the final MAXRSS
        merged_info={}
        for line in info:
            if not "." in line[0]:
                merged_info[line[0]]=line
            elif ".ba" in line[0]:
                try:
                    merged_info[line[0].split(".")[0]].append(line[-1])
                except KeyError:
                    pass
        info=merged_info.values()

        return list(info)
    
