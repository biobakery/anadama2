# -*- coding: utf-8 -*-
import os
import sys
import time
import tempfile
import threading

import six

from .grid import Grid
from .grid import GridWorker
from .slurm import PerformanceData
from .. import runners
from .. import picklerunner
from ..util import underscore
from ..util import find_on_path
from ..util import keyrename, keepkeys

if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

class SGE(Grid):
    """This class enables the Workflow class to dispatch tasks to
    Sun Grid Engine and its lookalikes. Use it like so:

    .. code:: python

      from anadama2 import Workflow
      from anadama2.sge import SGE

      ctx = Workflow(grid_powerup=SGE(queue="general"))
      ctx.do("wget "
             "ftp://public-ftp.hmpdacc.org/"
             "HMMCP/finalData/hmp1.v35.hq.otu.counts.bz2 "
             "-O @{input/hmp1.v35.hq.otu.counts.bz2}")

      # run on sge with 200 MB of memory, 4 cores, and 60 minutes
      t1 = ctx.grid_do("pbzip2 -d -p 4 < #{input/hmp1.v35.hq.otu.counts.bz2} "
                       "> @{input/hmp1.v35.hq.otu.counts}",
                       mem=200, cores=4, time=60)

      # run on sge on the serial_requeue queue
      ctx.grid_add_task("some_huge_analysis {depends[0]} {targets[0]}",
                        depends=t1.targets, targets="output.txt",
                        mem=4000, cores=1, time=300, partition="serial_requeue")


      ctx.go()


    :param partition: The name of the SLURM partition to submit tasks to
    :type partition: str

    :param tmpdir: A directory to store temporary files in. All
      machines in the cluster must be able to read the contents of
      this directory; uses :mod:`anadama2.picklerunner` to create
      self-contained scripts to run individual tasks and calls
      ``srun`` to run the script on the cluster.
    :type tmpdir: str
    
    :keyword benchmark_on: Option to turn on/off benchmarking
    : type benchmark_on: bool

    """

    def __init__(self, partition, tmpdir, benchmark_on=None):
        super(SGE, self).__init__("sge", SGEWorker, None, partition, tmpdir, benchmark_on)


class SGEWorker(GridWorker):

    def __init__(self, work_q, result_q, lock, reporter):
        super(SGEWorker, self).__init__(work_q, result_q, lock, reporter))
 
    def run(self):
        return runners.worker_run_loop(self.work_q, self.result_q, 
                                       _run_task_sge)

def _wait_on_file(fname, secs=30, pollfreq=0.1, rm=True):
    for _ in range(int(secs/pollfreq)):
        if os.path.exists(fname):
            with open(fname) as f:
                ret = f.read()
            if rm is True:
                os.unlink(fname)
            return ret
        else:
            time.sleep(pollfreq)
    raise OSError("Timed out waiting for "+fname+" to appear")


def _run_task_sge(task, extra):
    (perf, partition, tmpdir, slurm_queue, reporter) = extra
    script_path = picklerunner.tmp(task, dir=tmpdir).path
    job_name = "task{}.{}".format(task.task_no, underscore(task.name))
    tmpout = tempfile.mktemp(dir=tmpdir)
    tmperr = tempfile.mktemp(dir=tmpdir)

    args = ["qsub", "-R", "y", "-b", "y", "-sync", "y", "-N", job_name,
            "-pe", "smp", str(perf.cores), "-cwd", "-q", partition,
            "-V", "-l", "m_mem_free={:1g}M".format(max(1, perf.mem)),
            "-o", tmpout, "-e", tmperr]
    args += [script_path, "-r", "-p" ]
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, 
                            stderr=subprocess.PIPE)
    out, err = proc.communicate()
    extra_error = ""
    try:
        taskout = _wait_on_file(tmpout)
        if proc.returncode != 0:
            extra_error += _wait_on_file(tmperr)
    except Exception as e:
        e.task_no = task.task_no
        return runners.exception_result(e)

    try:
        result = picklerunner.decode(taskout)
    except ValueError:
        extra_error += "Unable to decode task result\n"
        result = None
    if proc.returncode != 0:
        extra_error += "Qsub error: "+err+"\n"
    if result is None:
        return runners.TaskResult(task.task_no, extra_error or "qsub failed",
                                  None, None)
    elif extra_error: # (result is not None) is implicit here
        result = result._replace(error=result.error+extra_error)
    return result


