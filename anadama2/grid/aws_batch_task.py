#!/usr/bin/env python

""" A script to run a pickled task placed in a AWS S3 bucket """

import os
import sys
import argparse

import six
import cloudpickle

from anadama2.runners import _run_task_locally

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

    bucket = name.replace("s3://","").split("/")[0]
    key = "/".join(name.replace("s3://","").split("/")[1:])
    filename = key.split("/")[-1]

    return bucket, key, filename

def download_file(resource,bucket,key,filename):
    """ Download a file from s3 """

    # create download folder if needed
    directory = os.path.dirname(filename)
    if directory and not os.path.isdir(directory):
        os.makedirs(directory)

    resource.Bucket(bucket).download_file(key,filename)

def upload_file(resource,bucket,key,filename):
    """ Upload a file to s3 """

    resource.Bucket(bucket).upload_file(filename,key)

def local_path(filename,working_directory):
    return filename.replace("s3://",working_directory)

def main():

    import boto3
    resource = boto3.resource("s3")

    # Parse arguments from the command line
    args=parse_arguments(sys.argv)

    # move to the working directory
    os.chdir(args.working_directory)

    # download the input file from S3
    in_bucket, in_key, in_filename = parse_s3(args.input)
    local_in_file = local_path(in_filename, args.working_directory)
    download_file(resource,in_bucket,in_key,local_in_file)
    
    # read in the task information
    task = cloudpickle.load(open(local_in_file,"rb"))

    # move all temp targets/depends to local working directory
    tracked_with_temp = list(filter(lambda x: x.temp_files(), task.depends+task.targets))
    current_local_paths = [x.local_path() for x in tracked_with_temp]
    new_local_paths = [x.prepend_local_path(args.working_directory) for x in tracked_with_temp] 
    paths = dict( (old, new) for old, new in zip(current_local_paths, new_local_paths)) 
    for i, action in enumerate(task.actions):
        if not six.callable(action):
            for old_path in sorted(paths, key=len, reverse=True):
                task.actions[i]=action.replace(old_path, paths[old_path])

    # run the task
    result = _run_task_locally(task)

    # write the pickled results to a file
    out_bucket, out_key, out_filename = parse_s3(args.output)
    local_out_file = local_path(out_filename, args.working_directory)
    cloudpickle.dump(result,open(local_out_file,"wb"))

    # upload the results to S3
    upload_file(resource,out_bucket,out_key,local_out_file)

if __name__ == "__main__":
    main()
