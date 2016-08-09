
from anadama import Workflow
from anadama.helpers import system, rm_r
from anadama.util import fname

base = "http://downloads.hmpdacc.org/data/Illumina/stool/"
srs_ids = ["SRS053214", "SRS052697", "SRS016944"]
    
ctx = Workflow()
for srs_id in srs_ids:
    dl_task = ctx.add_task("wget "+base+srs_id+".tar.bz2",
                           targets=[srs_id+".tar.bz2"])
    cat_task = ctx.add_task(
        [ "bunzip2 {depends[0]} | tar -xf-",
          "cat "+srs_id+"/*.fastq > {targets[0]}",
          rm_r(srs_id) ],
        targets=srs_id+".fastq",
        depends=dl_task.targets
    )
    mt_task = ctx.add_task( "metaphlan2.py --no_map {depends[0]} {targets[0]}",
                            depends=cat_task.targets,
                            targets=srs_id+".tax.txt" )

    logfile = fname.mangle("sample.log", dir=srs_id)
    output_bz = srs_id+"_humann.tar.bz2"
    ctx.add_task(
        [ system(["humann2", "--output-basename", srs_id,
                  "--remove-temp-output",
                  "--taxonomic-profile", str(mt_task.targets[0]),
                  "--input", str(cat_task.targets[0]),
                  "--output", srs_id, "--o-log", logfile]),
          system(["tar", "-cjf", output_bz, srs_id]),
          rm_r(srs_id) ],
        depends=[cat_task.targets[0], mt_task.targets[0]],
        targets=output_bz
    )

ctx.go(n_parallel=3)

