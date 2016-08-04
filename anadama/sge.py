import os
import sys
import time
import Queue
import tempfile
import threading

from . import grid
from . import runners
from . import picklerunner

from .slurm import PerformanceData
from .util import underscore
from .util import find_on_path
from .util import keyrename, keepkeys

if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

available = all( bool(find_on_path(prog)) for prog in
                 ("qconf", "qsub"))


class SGEPowerup(grid.DummyPowerup):
    """This class enables the RunContext class to dispatch tasks to
    Sun Grid Engine and its lookalikes. Use it like so:

    .. code:: python

      from anadama import RunContext
      from anadama.sge import SGEPowerup

      ctx = RunContext(grid_powerup=SGEPowerup(queue="general"))
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


    :param queue: The name of the SGE queue to submit tasks to
    :type queue: str

    :keyword tmpdir: A directory to store temporary files in. All
      machines in the cluster must be able to read the contents of
      this directory; uses :mod:`anadama.picklerunner` to create
      self-contained scripts to run individual tasks and calls
      ``qsub`` to run the script on the cluster.
    :type tmpdir: str

    :type extra_qsub_flags: list of str

    """

    def __init__(self, queue, tmpdir="/tmp", extra_qsub_flags=[],
                 *args, **kwargs):
        self.sge_queue = queue
        self.sge_tmpdir = tmpdir
        self.extra_qsub_flags = extra_qsub_flags
        
        self.sge_task_data = dict()
        self._sge_pe_name = self._find_suitable_pe()


    def _import(self, task_dict):
        sge_keys = ["time", "mem", "cores", "partition", "extra_srun_flags"]
        keys_to_keep = ["actions", "depends", "targets",
                        "name", "interpret_deps_and_targs"]
        if any(k in task_dict for k in sge_keys):
            task_dict = keyrename( task_dict,
                [("partition", "queue"),
                 ("extra_srun_flags", "extra_qsub_flags")]
            )
            return self.sge_add_task(
                **keepkeys(task_dict, sge_keys+keys_to_keep)
            )
        else:
            return self.add_task(**keepkeys(task_dict, keys_to_keep))


    def do(self, task, **kwargs):
        params = self._kwargs_extract(kwargs)
        self.sge_task_data[task.task_no] = params

    
    def add_task(self, task, **kwargs):
        params = self._kwargs_extract(kwargs)
        self.sge_task_data[task.task_no] = params

    
    def runner(self, ctx, n_parallel=1, n_grid_parallel=1):
        runner = runners.GridRunner(ctx)
        runner.add_worker(runners.ParallelLocalWorker,
                          name="local", rate=n_parallel, default=True)
        runner.add_worker(SGEWorker, name="sge", rate=n_grid_parallel)
        runner.routes.update([
            ( task_idx, ("sge", extra) )
            for task_idx, extra in self.sge_task_data.iteritems()
        ])
        return runner


    def _kwargs_extract(self, kwargs_dict):
        time = kwargs_dict.pop("time", None)
        mem = kwargs_dict.pop("mem", None)
        cores = kwargs_dict.pop("cores", 1)
        partition = kwargs_dict.pop("queue", self.sge_queue)
        extra_qsub_flags = kwargs_dict.pop("extra_qsub_flags",
                                           self.extra_qsub_flags)
        return (PerformanceData(int(time), int(mem), int(cores)),
                partition, self.sge_tmpdir, self._sge_pe_name, extra_qsub_flags)


    def _find_suitable_pe(self):
        names, _ = subprocess.Popen(['qconf', '-spl'], 
                                    stdout=subprocess.PIPE).communicate()
        if not names:
            raise OSError(
                "Unable to find any SGE parallel environment names. \n"
                "Ensure that SGE tools like qconf are installed, \n"
                "ensure that this node can talk to the cluster, \n"
                "and ensure that parallel environments are enabled.")

        pe_name = None
        for name in names.strip().split():
            if pe_name:
                break
            out, _ = subprocess.Popen(["qconf", "-sp", name], 
                                   stdout=subprocess.PIPE).communicate()
            for line in out.split('\n'):
                if ["allocation_rule", "$pe_slots"] == line.split():
                    pe_name = name
        
        if not pe_name:
            raise OSError(
                "Unable to find a suitable parallel environment. "
                "Please talk with your systems administrator to enable "
                "a parallel environment that has an `allocation_rule` "
                "set to `$pe_slots`.")
        return pe_name



class SGEWorker(threading.Thread):

    def __init__(self, work_q, result_q):
        super(SGEWorker, self).__init__()
        self.logger = runners.logger
        self.work_q = work_q
        self.result_q = result_q

    @staticmethod
    def appropriate_q_class(*args, **kwargs):
        return Queue.Queue(*args, **kwargs)

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
    (perf, partition, tmpdir, pe_name, extra_qsub_flags) = extra
    script_path = picklerunner.tmp(task, dir=tmpdir).path
    job_name = "task{}.{}".format(task.task_no, underscore(task.name))
    tmpout = tempfile.mktemp(dir=tmpdir)
    tmperr = tempfile.mktemp(dir=tmpdir)

    args = ["qsub", "-R", "y", "-b", "y", "-sync", "y", "-N", job_name,
            "-pe", pe_name, str(perf.cores), "-cwd", "-q", partition,
            "-V", "-l", "m_mem_free={:1g}M".format(max(1, perf.mem)),
            "-o", tmpout, "-e", tmperr]
    args += extra_qsub_flags+[script_path, "-r", "-p" ]
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


