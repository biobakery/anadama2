from anadama2 import Workflow
from anadama.util import fname
from anadama2.cli import Configuration

cfg = Configuration( version="0.0.1",
                     description="A workflow to run KneadData and MetaPhlAn2"
    ).add("kneaddata_db", desc="the kneaddata database",
          default="/work/code/kneaddata/db/"
    ).add("input_extension", desc="the input file extension",
          default="*.fastq"
    ).add("threads", desc="Number of threads for knead_data to use",
          default=1)


workflow = Workflow(vars=cfg)
in_files = workflow.vars.input.files(pattern=workflow.vars.input_extension)
for in_file in in_files:
    out_file = fname.mangle(in_file, tag="kneaddata", ext=".fastq")
    workflow.add_task(
        "kneaddata -i {depends[0]} -o {targets[0]} -db {kneaddata_db} -t {threads}",
        depends=input_file,
        target=output_file,
        kneaddata_db=workflow.vars.kneaddata_db,
        threads=workflow.vars.threads)
    
workflow.go()
