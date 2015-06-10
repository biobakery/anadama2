import subprocess

from doit.exceptions import CatchedException
from doit.runner import MThreadRunner

from .. import picklerunner
from ..util import dict_to_cmd_opts
from ..performance import PerformancePredictor


DEFAULT_MEM = 10 * 1024 # 10GB, in MB
DEFAULT_time = 20 * 3600 # 20 hrs in mins

def generate_srun(task, partition, mem, time, tmpdir="/tmp", **sbatch_kwds):
    opts = dict([
        ("mem", str(mem)),
        ("time", str(time)),
        ("export", "ALL"),
        ("partition", partition),
    ]+list(sbatch_kwds.items()))

    return ( "srun "
             +" "+dict_to_cmd_opts(opts)
             +" "+picklerunner.tmp(task, dir=tmpdir).path+" -r" )


class SlurmRunner(MThreadRunner):
    def __init__(self, partition,
                 performance_url=None,
                 tmpdir="/tmp",
                 *args, **kwargs):
        super(MThreadRunner, self).__init__(*args, **kwargs)
        self.partition = partition
        self.tmpdir = tmpdir
        self.performance_predictor = PerformancePredictor(performance_url)

    def execute_task(self, task):
        perf = self.performance_predictor.predict(task)
        self.reporter.execute_task(task)
        if not task.actions:
            return None

        srun_cmd = generate_srun(task, self.partition,
                                 perf.mem/1024, perf.time,
                                 tmpdir=self.tmpdir)
        proc = subprocess.Popen([srun_cmd], shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = proc.communicate()
        if not task.actions[0].out:
            task.actions[0].out = str()
        if not task.actions[0].err:
            task.actions[0].err = str()
        task.actions[0].out += out
        task.actions[0].err += err
        if proc.returncode:
            return CatchedException("srun command failed: "+srun_cmd
                                    +"\n"+out+"\n"+err)

        return None

    
        
