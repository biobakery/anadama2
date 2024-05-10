# -*- coding: utf-8 -*-
import os
import re
import sys
import logging
import itertools
import time
import functools
import shlex
import copy
import threading

import six

from ..helpers import build_actions
from ..tracked import AWSHugeTrackedFile
from ..runners import _get_task_result

from .grid import Grid
from .grid import GridWorker
from .grid import GridQueue

from .aws_batch_task import parse_s3, upload_file, download_file

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
        super(AWS, self).__init__("aws", AWSGridWorker, AWSQueue(partition), tmpdir)

class AWSGridWorker(GridWorker):
    """ Base Grid Worker class """

    def __init__(self, work_q, result_q, lock, reporter):
        super(AWSGridWorker, self).__init__(work_q, result_q, lock, reporter)

    @classmethod
    def run_task_by_type(cls, task, extra):
        import cloudpickle
        import boto3

        (perf, tmpdir, grid_queue, reporter) = extra

        # check that task writes output to S3 bucket
        if not task.targets or not isinstance(task.targets[0], AWSHugeTrackedFile):
            result = _get_task_result(task)
            extra_error = "AWS Batch tasks must write targets to S3 buckets"
            return result._replace(error=extra_error)

        # move the temp tracked files to /tmp folder for docker run
        # update the action to use the new dependency locations        
        tracked_with_temp = list(filter(lambda x: x.temp_files(), task.depends+task.targets))
        task_working_directory = os.path.join("/tmp",task.targets[0].tmpdir)
        tmp_paths = [x.prepend_local_path(task_working_directory) for x in tracked_with_temp]

        # build the actions again with the new targets/depends paths
        task.actions=build_actions(task.actions_raw, task.depends, task.targets,
            task.visible, task.kwargs, task.use_parse_sh) 

        task_name="task_"+str(task.task_no)
        task_pkl_file_basename = task_name+".pkl"
        task_pkl_file=os.path.join(tmpdir,task_pkl_file_basename)
        task_result_pkl_file_basename = "result_"+task_name+".pkl"
        task_result_pkl_file=os.path.join(tmpdir,task_result_pkl_file_basename)

        # write the input pickle file
        resource = boto3.resource("s3")
        bucket, key, filename = parse_s3(task.targets[0])      
        input_s3 = "s3://"+bucket+"/"+task_pkl_file_basename
        output_s3 = "s3://"+bucket+"/"+task_result_pkl_file_basename

        with open(task_pkl_file,"wb") as file_handle:
            cloudpickle.dump(task,file_handle)
        # copy to cloud
        upload_file(resource,bucket,task_pkl_file_basename,task_pkl_file)

        # update the task to run the pickle script
        pickle_task = copy.deepcopy(task)
        pickle_task.actions = [" ".join(["anadama2_aws_batch_task",
            "--input", input_s3, "--output", output_s3,
            "--working-directory", task_working_directory])]

        # run the task as a command
        result = cls.run_task_command(pickle_task, extra)

        # download the results
        download_file(resource,bucket,task_result_pkl_file_basename,task_result_pkl_file)

        # decode the result
        extra_error = None
        try:
            with open(task_result_pkl_file,"rb") as file_handle:
                result = cloudpickle.load(file_handle)
        except (ValueError, EOFError, IOError):
            extra_error = "Unable to read task result, check the AWS batch logs"

        if extra_error:
            result = result._replace(error=str(result.error)+extra_error)

        return result

class AWSQueue(GridQueue):
    
    def __init__(self, partition):
        super(AWSQueue, self).__init__(partition)

        import boto3
        # get the region for this instance
        try:
            region = subprocess.check_output(["ec2-metadata","--availability-zone"]).rstrip().split(": ")[-1]
        except (subprocess.CalledProcessError, EnvironmentError):
            region = "us-east-2"

        # this is the number of seconds to wait after task definition submission
        self.submit_definition_sleep = 3

        # this is the refresh rate for checking the queue, in seconds
        self.refresh_rate = 2*60

        # this is a lock for task definition submission
        self.lock_submit_task_definition = threading.Lock()

        self.partition = partition

        self.client = boto3.client("batch",region)
       
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

    def create_grid_script(self,partition,cpus,minutes,memory,command,taskid,dir,docker_image):
        """ Create a grid script from the template also creating job definition """

        if not docker_image:
            docker_image='amazonlinux'

        # get the partition for the task (to allow for multiple queues and partitions in task definition)
        partition = self.get_partition(time, partition)

        # create job definition
        job_name = "task_{}".format(taskid)

        # get lock to submit job definition
        self.lock_submit_task_definition.acquire()

        response = self.client.register_job_definition(
            containerProperties={
                'image': docker_image,
                'memory': memory,
                'vcpus': cpus,
                'volumes': [
                    { 'host': { 'sourcePath': '/opt' },
                      'name': 'opt'
                    },
                    { 'host': { 'sourcePath': '/tmp' },
                      'name': 'tmp'
                    },
                ],
                'mountPoints': [
                    {
                        'containerPath': '/opt',
                        'readOnly': True,
                        'sourceVolume': 'opt'
                    },
                    {
                        'containerPath': '/tmp',
                        'readOnly': False,
                        'sourceVolume': 'tmp'
                    }
                ]
            },
            jobDefinitionName=job_name,
            type='container'
            )

        # pause after submission
        time.sleep(self.submit_definition_sleep)

        # release lock
        self.lock_submit_task_definition.release()

        submit_script = functools.partial(self.client.submit_job,
            containerOverrides={'command': shlex.split(command)},
            jobDefinition=job_name,
            jobName=job_name,
            jobQueue=partition)

        return submit_script, None, None, None
    
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
    
        def list_jobs(state, nextToken=None):
            if self.partition_long != self.partition_short:
                if nextToken:
                    response = self.client.list_jobs(jobQueue=self.partition_short,jobStatus=state,nextToken=nextToken)
                    response2 = self.client.list_jobs(jobQueue=self.partition_long,jobStatus=state,nextToken=nextToken)
                    if response2['jobSummaryList']:
                        response['jobSummaryList']+=response2['jobSummaryList']
                else:
                    response = self.client.list_jobs(jobQueue=self.partition_short,jobStatus=state)
                    response2 = self.client.list_jobs(jobQueue=self.partition_long,jobStatus=state)
                    if response2['jobSummaryList']:
                        response['jobSummaryList']+=response2['jobSummaryList']
            else:
                if nextToken:
                    response = self.client.list_jobs(jobQueue=self.partition,jobStatus=state,nextToken=nextToken)
                else:
                    response = self.client.list_jobs(jobQueue=self.partition,jobStatus=state)
            return response

        job_status = []
        for state in self.all_codes:
            response = list_jobs(state)
            job_status += gather_job_status(response)
            while response.get('nextToken',None):
                response = list_jobs(state, nextToken=response['nextToken'])
                job_status += gather_job_status(response)
        
        return job_status
    
