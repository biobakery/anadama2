import re
import operator
import subprocess
from math import exp
from collections import namedtuple

from doit.exceptions import CatchedException
from doit.runner import MThreadRunner

from .. import picklerunner
from ..util import dict_to_cmd_opts, partition
from ..performance import PerformancePredictor


sigmoid = lambda t: 1/(1-exp(-t))
first = operator.itemgetter(0)


def generate_srun(task, partition, mem, time,
                  tmpdir="/tmp", threads=1, **sbatch_kwds):
    opts = dict([
        ("mem", str(mem)),
        ("time", str(time)),
        ("export", "ALL"),
        ("partition", partition),
        ("cpus-per-task", threads),
    ]+list(sbatch_kwds.items()))

    return ( "srun -v "
             +" "+dict_to_cmd_opts(opts)
             +" "+picklerunner.tmp(task, dir=tmpdir).path+" -r" )


class SlurmRunner(MThreadRunner):
    def __init__(self, partition,
                 performance_url=None,
                 tmpdir="/tmp",
                 *args, **kwargs):
        super(SlurmRunner, self).__init__(*args, **kwargs)
        self.partition = partition
        self.tmpdir = tmpdir
        self.performance_predictor = PerformancePredictor(performance_url)
        self.id_task_map = dict()


    def execute_task(self, task):
        perf = self.performance_predictor.predict(task)
        self.reporter.execute_task(task)
        if not task.actions:
            return None

        maybe_exc, task_id = self._grid_execute(task, perf)
        if task_id:
            self.id_task_map[task_id] = task

        return maybe_exc


    def finish(self):
        for task, (max_rss_mb, cpu_hrs, clock_hrs) in self._grid_summarize():
            self.performance_predictor.update(task, max_rss_mb,
                                              cpu_hrs, clock_hrs)
        self.performance_predictor.save()
        return super(SlurmRunner, self).finish()
    

    def _grid_execute(self, task, perf_obj):
        keep_going, maybe_exc, tries = True, None, 1
        mem, time, threads = perf_obj
        task_id = None
        while keep_going:
            keep_going, maybe_exc = False, None
            srun_cmd = generate_srun(task, self.partition, mem, time,
                                     threads=threads, tmpdir=self.tmpdir)
            proc = subprocess.Popen([srun_cmd], shell=True,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            out, err = proc.communicate()
            if not task.actions[0].out:
                task.actions[0].out = str()
            if not task.actions[0].err:
                task.actions[0].err = str()
            task.actions[0].out += out
            task.actions[0].err += err
            if proc.returncode:
                packed = self._handle_grid_fail(srun_cmd, out, err,
                                                proc.returncode,
                                                tries, mem, time)
                maybe_exc, keep_going, mem, time = packed
            else:
                task_id = re.search(r'launching (\d+).(\d+) on host',
                                    err).group(1)
        return maybe_exc, task_id


    def _grid_summarize(self):
        for chunk in partition(self.id_task_map.iteritems(), 100):
            ids, tasks = zip(*sorted(filter(bool, chunk), key=first))
            stats = sorted(self._jobstats(ids), key=first)
            for task, stat in zip(tasks, stats):
                yield task, stat
    

    @staticmethod
    def _jobstats(ids):
        ids = ",".join(ids)
        def _fields():
            proc = subprocess.Popen(
                ["sacct",
                 "--format", "jobid,MaxRSS,Elapsed,ExitCode,State",
                 "-P", "-j", ids],
                stdout=subprocess.PIPE)
            for line in proc.stdout:
                fields = line.split("|")
                if any((fields[-1] != "COMPLETED", fields[4] != "0:0")):
                    continue
                yield fields[:-2]
            proc.wait()

        for id, rss, clocktime in _fields():
            rss = float(rss.replace("K", ""))/1024,
            clockparts = map(float, clocktime.split(":"))
            clocktime = clockparts[0] + clockparts[1]/60 + clockparts[2]/3600
            yield id, rss, clocktime
                    

    @staticmethod
    def _handle_grid_fail(cmd, out, err, retcode, tries, mem, time):
        exc = CatchedException("srun command failed: "+cmd
                               +"\n"+out+"\n"+err)
        keep_going = False
        outerr = out+err
        if "Exceeded job memory limit" in outerr:
            used = re.search(r'memory limit \((\d+) > \d+\)', outerr).group(1)
            mem = int(used)/1024 * (1.3**tries)
            keep_going = True
        if re.search(r"due to time limit", outerr, re.IGNORECASE):
            time = time * (sigmoid(tries/10.)*2.7)
            keep_going = True

        return exc, keep_going, int(mem), int(time)
