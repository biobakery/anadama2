
from anadama2 import Workflow

workflow = Workflow(remove_options=["input","output"])

downloads=["ftp://public-ftp.hmpdacc.org/HM16STR/by_sample/SRS011275.fsa.gz",
    "ftp://public-ftp.hmpdacc.org/HM16STR/by_sample/SRS011273.fsa.gz",
    "ftp://public-ftp.hmpdacc.org/HM16STR/by_sample/SRS011180.fsa.gz"]

for link in downloads:
    workflow.add_task(
        "wget -O [targets[0]] [args[0]]",
        targets=link.split("/")[-1],
        args=link)

workflow.go()

