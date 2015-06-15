import os
import re
import tempfile
import operator
import subprocess
from math import exp

from doit.exceptions import CatchedException
from doit.runner import MThreadRunner

from .. import picklerunner
from ..util import dict_to_cmd_opts, partition
from ..performance import PerformancePredictor


sigmoid = lambda t: 1/(1-exp(-t))
first = operator.itemgetter(0)

class GridRunner(MThreadRunner):
    def __init__(self, partition,
                 performance_url=None,
                 tmpdir="/tmp",
                 extra_grid_args="",
                 *args, **kwargs):
        super(GridRunner, self).__init__(*args, **kwargs)
        self.partition = partition
        self.tmpdir = tmpdir
        self.performance_predictor = PerformancePredictor(performance_url)
        self.extra_grid_args = extra_grid_args
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
        return super(GridRunner, self).finish()
    

    # TODO: better naming of grid_execute, grid_dispatch and grid_run_task
    def _grid_execute(self, task, perf_obj):
        keep_going, maybe_exc, tries = True, None, 1
        mem, time, threads = perf_obj
        task_id = None
        while keep_going:
            keep_going, maybe_exc = False, None
            cmd, (out, err, retcode) = self._grid_run_task(
                task, self.partition, 
                mem, time, 
                threads=threads, 
                tmpdir=self.tmpdir,
                extra_grid_args=self.extra_grid_args)
            if retcode:
                packed = self._handle_grid_fail(cmd, out, err,
                                                retcode, tries, mem, time)
                maybe_exc, keep_going, mem, time = packed
            else:
                task_id = self._find_job_id(out, err)
        return maybe_exc, task_id


    @staticmethod
    def _grid_dispatch(cmd, task):
        proc = subprocess.Popen([cmd], shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        out, err = proc.communicate()
        if not task.actions[0].out:
            task.actions[0].out = str()
        if not task.actions[0].err:
            task.actions[0].err = str()
        task.actions[0].out += out
        task.actions[0].err += err
        return out, err, proc.returncode


    def _grid_summarize(self):
        for chunk in partition(self.id_task_map.iteritems(), 100):
            ids, tasks = zip(*sorted(filter(bool, chunk), key=first))
            stats = sorted(self._jobstats(ids), key=first)
            for task, stat in zip(tasks, stats):
                yield task, stat

    # You'll have to implement the below methods yourself to make your
    # own grid runner

    @staticmethod
    def _grid_run_task(task, partition, mem, time, 
                       tmpdir='/tmp', threads=1, extra_grid_args=""):
        raise NotImplementedError()


    @staticmethod
    def _find_job_id(out, err):
        raise NotImplementedError()

    @staticmethod
    def _jobstats(ids):
        raise NotImplementedError()

    @staticmethod
    def _handle_grid_fail(cmd, out, err, retcode, tries, mem, time):
        raise NotImplementedError()




class SlurmRunner(GridRunner):
    @staticmethod
    def _grid_run_task(task, partition, mem, time,
                       tmpdir="/tmp", threads=1, extra_grid_args=""):
        opts = { "mem": mem,   
                 "time": time,
                 "export": "ALL", 
                 "partition": partition,
                 "cpus-per-task": threads }

        cmd = ( "srun -v "
                +" "+dict_to_cmd_opts(opts)
                +" "+extra_grid_args+" "
                +" "+picklerunner.tmp(task, dir=tmpdir).path+" -r" )

        return cmd, SlurmRunner._grid_dispatch(cmd, task)


    @staticmethod
    def _find_job_id(out, err):
        return re.search(r'launching (\d+).(\d+) on host',
                         err).group(1)


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
                fields = line.strip().split("|")
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



class LSFRunner(GridRunner):
    fmt = ('jobid max_mem run_time exit_code'
           ' exit_reason stat delimiter="|"')
    multipliers = {
        "gbytes": lambda f: f/1024,
        "mbytes": lambda f: f,
        "kbytes": lambda f: f*1024
    }


    @staticmethod
    def _grid_run_task(task, partition, mem, time, 
                       tmpdir='/tmp', threads=1, extra_grid_args=""):
        rusage = "rusage[mem={}:duration={}]".format(mem, int(time)),
        tmpout = tempfile.mktemp(dir=tmpdir)
        opts ={ 'R': rusage, 'o': tmpout,
                'n': threads,'q': partition }
        
        cmd = ( "bsub -K -r"
                +" "+dict_to_cmd_opts(opts)
                +" "+extra_grid_args+" "
                +" "+picklerunner.tmp(task, dir=tmpdir).path+" -r" )
        out, err, retcode = LSFRunner._grid_dispatch(cmd, task)

        try:
            with open(tmpout) as f:
                task.actions[0].err += f.read()
            os.unlink(tmpout)
        except Exception as e:
            err += "Anadama error: "+str(e)

        return cmd, (out, err, retcode)


    @staticmethod
    def _find_job_id(out, err):
        return re.search(r'Job <(\d+)>', out).group(1)


    @staticmethod
    def _jobstats(ids):
        def _fields():
            proc = subprocess.Popen(['bjobs', '-noheader',
                                     '-o ', LSFRunner.fmt]+ids,
                                    stdout=subprocess.PIPE)
            for line in proc.stdout:
                fields = line.strip().split("|")
                if any((fields[-1] != "DONE", 
                        fields[-2] != "-", 
                        fields[-3] != "-")):
                    continue
                yield fields[:3]

        for id, mem, time in _fields():
            key, mem_str = mem.split()
            mem = LSFRunner.multipliers[key](float(mem_str))
            time = int(time.split()[0])
            yield id, mem, time


    @staticmethod
    def _handle_grid_fail(cmd, out, err, retcode, tries, mem, time):
        return None, False, mem, time

