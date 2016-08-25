import os
from anadama2 import Workflow
from anadama2.helpers import system, rm
from anadama2.util import sh, fname

from sixteen import find_cutoff

input_dir = "input"
output_dir = "output"
input_bams = os.listdir(input_dir)

ctx = Workflow()
for input_bam in input_bams:
    ctx.already_exists(input_bam)

    rawfq = fname.mangle(input_bam, dir=output_dir, ext="fastq")
    r1 = fname.mangle(rawfq, tag="r1")
    r2 = fname.mangle(rawfq, tag="r2")
    ctx.add_task(
        [ system(["samtools", "bam2fq", input_bam], stdout=rawfq),
          system(["split_paired_fastq", r1, r2], stdin=rawfq),
          system(["fastq-join", r1, r2,'-o' rawfq ]),
          rm([r1, r2]) ],
        depends=input_bam,
        targets=rawfq,
        name="Convert and stitch "+input_bam
    )

    trimfq = fname.mangle(rawfq, tag="trimmed")
    @ctx.add_task(name="Trim "+rawfq, depends=rawfq, targets=trimfq)
    def trim_task(task):
        cutoff = find_cutoff(rawfq)
        sh("usearch8 -fastx_truncate "+rawfq+" -trunclen "+str(cutoff)
           " -fasta_out "+trimfq)

    ctx.add_task("pick_otus {depends[0]}, {targets[0]}",
                 depends=trimfq,
                 targets=fname.mangle(trimfq, tag="taxonomy", ext="txt"))


ctx.go(n_parallel=4)
