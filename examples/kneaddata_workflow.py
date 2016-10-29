# -*- coding: utf-8 -*-
from anadama2 import Workflow

# create a workflow instance, providing the version number and description
# the version number will appear when running this script with the "--version" option
# the description will appear when running this script with the "--help" option
workflow = Workflow(version="0.1", description="A workflow to run KneadData")

# add the custom arguments to the workflow
workflow.add_argument("kneaddata-db", desc="the kneaddata database", default="/work/code/kneaddata/db/")
workflow.add_argument("input-extension", desc="the input file extension", default="fastq")
workflow.add_argument("threads", desc="number of threads for knead_data to use", default=1)

# get the arguments from the command line
args = workflow.parse_args()

# get all input files with the input extension provided on the command line
in_files = workflow.get_input_files(extension=args.input_extension)

# get a list of output files, one for each input file, with the kneaddata tag
out_files = workflow.name_output_files(name=in_files, tag="kneaddata")

# create a task for each set of input and output files to run kneaddata
workflow.add_task_group(
    "kneaddata --input [depends[0]] --output [output_folder] --reference-db [kneaddata_db] --threads [threads]",
    depends=in_files,
    targets=out_files,
    output_folder=args.output,
    kneaddata_db=args.kneaddata_db,
    threads=args.threads)

workflow.go()
