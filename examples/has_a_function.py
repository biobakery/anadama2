
from anadama2 import Workflow

workflow = Workflow(remove_options=["input","output"])

# add a task to download the file
workflow.add_task(
    "wget ftp://public-ftp.hmpdacc.org/HMMCP/finalData/hmp1.v35.hq.otu.counts.bz2 -O [targets[0]]",
    targets="hmp1.v35.hq.otu.counts.bz2")

# add a task to decompress the file
workflow.add_task(
    "bzip2 -d < [depends[0]] > [targets[0]]",
    depends="hmp1.v35.hq.otu.counts.bz2",
    targets="hmp1.v35.hq.otu.counts")

def remove_end_tabs_function(task):
    with open(task.targets[0].name, 'w') as file_handle_out:
        for line in open(task.depends[0].name):
            file_handle_out.write(line.rstrip() + "\n")

# add a task with a function to remove the end tabs from the file
workflow.add_task(
    remove_end_tabs_function,
    depends="hmp1.v35.hq.otu.counts",
    targets="hmp1.v35.hq.otu.counts.notabs",
    name="remove_end_tabs")

workflow.go()

