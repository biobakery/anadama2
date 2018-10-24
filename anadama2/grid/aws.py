# -*- coding: utf-8 -*-
import os
import re
import sys
import logging
import itertools
import time
import functools
import shlex
import tempfile

import six

from .grid import Grid
from .grid import GridWorker
from .grid import GridQueue

if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

class AWS(Grid):
    """This class enables the Workflow class to dispatch tasks to
    AWS Batch. Use it like so:

    .. code:: python

      from anadama2 import Workflow
      from anadama2.grid import AWS

      powerup = AWS(partition="general")
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

    :param tempdir: The temp directory for scripts and job output stderr/stdout

    """

    def __init__(self, partition, tmpdir):
        super(AWS, self).__init__("aws", GridWorker, AWSQueue(partition), tmpdir)


class AWSQueue(GridQueue):
    
    def __init__(self, partition):
        super(AWSQueue, self).__init__(partition)

        import boto3
        # get the region for this instance
        try:
            region = subprocess.check_output(["ec2-metadata","--availability-zone"]).rstrip().split(": ")[-1]
        except (subprocess.CalledProcessError, EnvironmentError):
            region = "us-east-2"

        # this is the refresh rate for checking the queue, in seconds
        self.refresh_rate = 3*60

        self.partition = partition

        self.client = boto3.client("batch",region)
        self.job_ids = []
       
        self.job_code_completed="SUCCEEDED"
        self.job_code_failed="FAILED"
       
        self.all_queued_codes=['SUBMITTED','PENDING','RUNNABLE','STARTING','RUNNING']
        self.all_stopped_codes=[self.job_code_completed,self.job_code_failed]
        self.all_codes=self.all_queued_codes+self.all_stopped_codes

    @staticmethod
    def job_submission_failed(jobid):
        """ Check if the job failed in submission and did not get an id """
        return True if "error" in jobid else False

    @staticmethod
    def get_job_id_from_submit_output(stdout):
        return stdout

    @staticmethod
    def submit_command(grid_script):   
        def submit_aws_batch(grid_script):
            response = grid_script()
            return response['jobId']

        return functools.partial(submit_aws_batch, grid_script)

    def create_grid_script(self,partition,cpus,minutes,memory,command,taskid,dir):
        """ Create a grid script from the template also creating job definition """

        # create temp files for stdout, stderr, and return code    
        job_name = "task_{}".format(taskid)
        handle_out, out_file=tempfile.mkstemp(suffix=".out",prefix=job_name+"_",dir=dir)
        os.close(handle_out)
        handle_err, error_file=tempfile.mkstemp(suffix=".err",prefix=job_name+"_",dir=dir)
        os.close(handle_err)
        handle_rc, rc_file=tempfile.mkstemp(suffix=".rc",prefix=job_name+"_",dir=dir)
        os.close(handle_rc)

        # create job definition
        response = self.client.register_job_definition(
            containerProperties={
                'image': 'amazonlinux',
                'memory': memory,
                'vcpus': cpus
                },
            jobDefinitionName=job_name,
            type='container'
            )

        submit_script = functools.partial(self.client.submit_job,
            containerOverrides={'command': shlex.split(command)},
            jobDefinition=job_name,
            jobName=job_name,
            jobQueue=self.partition)

        return submit_script, out_file, error_file, rc_file
    
    def job_failed(self,status):
        # check if the job has a status that it failed
        return status == self.job_code_failed
        
    def job_stopped(self,status):
        # check if the job has a status which indicates it stopped running
        return status in self.all_stopped_codes

    def refresh_queue_status(self):
        """ Get the latest status for all the grid jobs """
   
        def gather_job_status(response):
            job_stats = []
            for job in response['jobSummaryList']:
                job_stats.append([job['jobId'],job['status'],"NA","NA","NA"])
            return job_stats

        job_status = []
        for state in self.all_codes:
            response = self.client.list_jobs(jobQueue=self.partition,jobStatus=state)
            job_status += gather_job_status(response)
            while response.get('nextToken',None):
                response = self.client.list_jobs(jobQueue=self.partition,jobStatus=state,
                    nextToken=response['nextToken'])
                job_status += gather_job_status(response)
        
        return job_status
    
