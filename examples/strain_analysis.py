# -*- coding: utf-8 -*-
import os
import re
import sys
import logging
from glob import glob

from bunch import Bunch

from anadama2 import Workflow
from anadama2.tracked import HugeTrackedFile as HFD
from anadama2.tracked import TrackedFilePattern
from anadama2.tracked import TrackedDirectory
from anadama2.reporters import LoggerReporter
from anadama2.slurm import SlurmPowerup
from anadama2.util import fname
from anadama2.util import sh as _sh
from anadama2.helpers import system
from anadama2.helpers import rm

threads = 10

conf = Bunch(pkl="/shares/hiibroad/deploy/metaphlan2/db_v20/mpa_v20_m200.pkl",
             strainer_src_dir="/shares/hiibroad/deploy/metaphlan2/strainer_src")
output = Bunch(db="/shares/hiibroad/data/strains/output/db_markers",
               ref="/shares/hiibroad/data/strains/output/consensus_markers",
               dir="/shares/hiibroad/data/strains/output",
               clades="/shares/hiibroad/data/strains/output/clades.txt")
bin = Bunch(strainer='/shares/hiibroad/deploy/metaphlan2/metaphlan2_strainer.py',
            sample2markers='/shares/hiibroad/deploy/metaphlan2/strainer_src/sample2markers.py',
            extractmarkers='/shares/hiibroad/deploy/metaphlan2/strainer_src/extract_markers.py')

all_markers_fasta = "/shares/hiibroad/data/strains/all_markers.fasta"

def fmt_sh(s, **kwargs):
    def _actually_fmt_sh(task):
        kwargs['depends'] = task.depends
        kwargs['targets'] = task.targets
        cmd = s.format(**kwargs)
        logging.debug("Executing shell command: %s", cmd)
        return _sh(cmd, shell=True)
    return _actually_fmt_sh


def getctx():
    ctx = Workflow(grid_powerup=SlurmPowerup("hii02", tmpdir="tmp"))
    reporter = LoggerReporter(ctx, "DEBUG", '/shares/hiibroad/data/strains/run.log')
    return ctx, reporter


def step_1():

    with open("input.txt") as f:
        input_files = map(str.strip, f)

    a, b = [iter(sorted(input_files))] * 2
    samples = zip(a, b)

    ctx, reporter = getctx()
    ctx.already_exists(conf.pkl)

    markers_files = list()
    for r1, r2 in samples:
        tax_txt = fname.mangle(r1, dir=output.dir, ext="txt", tag="taxonomy")
        sam = fname.mangle(r1, dir=output.dir, ext="sam")
        bt2_txt = fname.mangle(r1, dir=output.dir, ext="txt", tag="bowtie2")
        markers = fname.mangle(r1, dir=output.ref, ext="markers")
        markers_files.append(markers)

        r1, r2 = map(HFD, (r1, r2))
        ctx.already_exists(r1, r2)

        ctx.grid_add_task(
            [ fmt_sh("cat {depends[0]} {depends[1]} | pbzip2 -d -p{threads} | metaphlan2.py "
                     "-o {targets[0]} --input_type multifastq "
                     "--bowtie2out {bt2_txt} --samout {sam} --nproc {threads}",
                     bt2_txt=bt2_txt, sam=sam, threads=threads),
              system([bin.sample2markers, "--ifn_samples", sam,
                      "--output_dir", output.ref,
                      "--input_type", "sam"],
                     working_dir=conf.strainer_src_dir),
              rm([sam, bt2_txt]) ],
            depends=[r1, r2],
            targets=[tax_txt, markers],
            name="Get markers: "+str(r1),
            time=500, mem=6000, cores=threads
            )

    try:
        ctx.go(reporter=reporter, jobs=3, grid_jobs=50)
    except:
        pass

    markers_files = filter(os.path.exists, markers_files)

    clades_txt = fname.mangle("clades.txt", dir=output.dir)
    ctx.grid_add_task(
        [ system([bin.strainer, "--mpa_pkl", conf.pkl,
                  "--print_clades_only", "--output", output.dir,
                  "--nprocs_main", threads,
                  "--ifn_samples"] + markers_files, stdout=clades_txt) ],
        depends=markers_files,
        targets=clades_txt,
        name="Print clades",
        time=40000,
        mem=51200,
        cores=threads
        )

    ctx.go(reporter=reporter)

def parse_clades(fname):
    ret = []
    with open(fname) as f:
        for line in f:
            if not re.search(r'^[g__|s__]', line):
                continue
            ret.append(line.strip())
    return ret


def step_2():
    ctx, reporter = getctx()

    consensus_dep = TrackedFilePattern(output.ref+"/*.markers")
    consensus_marker_files = glob(str(consensus_dep))
    clades = parse_clades(output.clades)
    ctx.already_exists(all_markers_fasta)
    ctx.already_exists(consensus_dep)
    for clade in clades:
        clade_db_marker = fname.mangle(clade, dir=output.db, ext="markers.fasta")
        ctx.grid_add_task(
            system([bin.extractmarkers, "--mpa_pkl", conf.pkl, "--ifn_markers",
                    all_markers_fasta, '--clade', clade, '--ofn_markers', clade_db_marker],
                   working_dir=conf.strainer_src_dir),
            targets=clade_db_marker,
            depends=[all_markers_fasta],
            mem=2000, time=100, cores=1
            )

        clade_output_folder = fname.mangle(clade, dir=output.dir)
        clade_log_file = fname.mangle("tree_gen.log", dir=clade_output_folder)
        ctx.grid_add_task(
            system([bin.strainer, '--mpa_pkl', conf.pkl,
                    '--ifn_samples' ]+consensus_marker_files+[
                    '--ifn_markers', clade_db_marker, '--output_dir', clade_output_folder,
                    '--nprocs_main', threads, '--clades', clade], stdout_clobber=clade_log_file),
            depends=[consensus_dep, clade_db_marker],
            targets=[TrackedDirectory(clade_output_folder), clade_log_file],
            mem=51200, time=500, cores=threads
            )

    ctx.go(reporter=reporter, jobs=10, grid_jobs=50)

if __name__ == '__main__':
    if '1' == sys.argv[1]:
        step_1()
    elif '2' == sys.argv[1]:
        step_2()
    else:
        print("{} <1|2> # for step 1 or step 2".format(sys.argv[0]))
