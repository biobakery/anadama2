#!/usr/bin/env python

""" A script to run a pickled task placed in a AWS S3 bucket """

import os
import sys
import argparse
import shutil

import six

from anadama2.runners import _run_task_locally, TaskFailed
from anadama2.tracked import try_get_local_path

def parse_arguments(args):
    """
    Parse the arguments from the user
    """
    parser = argparse.ArgumentParser(
        description= "AnADAMA2 AWS Batch task runner\n",
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        "--input",
        required=True,
        help="the pickle file of task information\n")
    parser.add_argument(
        "--output",
        required=True,
        help="the pickle file of task results\n")
    parser.add_argument(
        "--working-directory",
        default="/tmp",
        help="the directory to run the task from\n")

    return parser.parse_args()

def parse_s3(name):
    """ Parse the file into bucket, key, and filename """

    bucket = str(name).replace("s3://","").split("/")[0]
    key = "/".join(str(name).replace("s3://","").split("/")[1:])
    filename = key.split("/")[-1]

    return bucket, key, filename

def download_file(resource,bucket,key,filename):
    """ Download a file from s3 """
    import botocore

    create_directories(filename)

    try:
        resource.Bucket(bucket).download_file(key,filename)
    except botocore.exceptions.ClientError:
        print("Unable to download file: s://"+bucket+"/"+key)

def upload_file(resource,bucket,key,filename):
    """ Upload a file to s3 """

    resource.Bucket(bucket).upload_file(filename,key)

def local_path(filename,working_directory):
    return filename.replace("s3://",working_directory)

def create_directories(filename):
    """ Create directories if needed """

    directory = os.path.dirname(filename)
    if directory and not os.path.isdir(directory):
        os.makedirs(directory)

def main():
    import cloudpickle
    import boto3
    resource = boto3.resource("s3")

    # Parse arguments from the command line
    args=parse_arguments(sys.argv)

    # move to the working directory
    os.makedirs(args.working_directory)
    os.chdir(args.working_directory)

    # download the input file from S3
    in_bucket, in_key, in_filename = parse_s3(args.input)
    local_in_file = local_path(in_filename, args.working_directory)
    download_file(resource,in_bucket,in_key,local_in_file)
    
    # read in the task information
    task = cloudpickle.load(open(local_in_file,"rb"))

    # create folders for targets (if needed)
    for target in task.targets:
        target_local_path = try_get_local_path(target)
        if isinstance(target_local_path, basestring):
            create_directories(target_local_path)

    # run the task
    result = _run_task_locally(task)

    # write the pickled results to a file
    out_bucket, out_key, out_filename = parse_s3(args.output)
    local_out_file = local_path(out_filename, args.working_directory)
    cloudpickle.dump(result,open(local_out_file,"wb"))

    # upload the results to S3
    upload_file(resource,out_bucket,out_key,local_out_file)

    # remove the working directory
    shutil.rmtree(args.working_directory)

    # if task failed print error for grid
    if isinstance(result, TaskFailed):
        sys.exit(result.msg)

if __name__ == "__main__":
    main()
