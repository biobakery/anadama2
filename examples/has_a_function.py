from anadama import RunContext
import os
import sys

## Set script working directory                                                                                                                                                                
os.chdir(os.path.dirname(os.path.abspath(sys.argv[0])))

ctx = RunContext()

retrieve_data = ctx.do("wget "
                       "ftp://public-ftp.hmpdacc.org/"
                       "HMMCP/finalData/hmp1.v35.hq.otu.counts.bz2 "
                       "-O @{input/hmp1.v35.hq.otu.counts.bz2}")

unzip = ctx.do("bzip2 -d < #{input/hmp1.v35.hq.otu.counts.bz2} "
               "> @{input/hmp1.v35.hq.otu.counts}")

def remove_end_tabs_fn(task):
    myfile = open(str(task.depends[0]), 'r')
    outfile = open(str(task.targets[0]), 'w')
    for line in myfile:
        outfile.write(line.rstrip() + "\n")
    myfile.close()
    outfile.close()
remove_end_tabs = ctx.add_task(remove_end_tabs_fn, depends=unzip.targets,
                               targets=["input/hmp1.v35.hq.otu.counts.notabs"],
                               name="remove_end_tabs")

ctx.go()

