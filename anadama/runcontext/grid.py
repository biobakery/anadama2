from collections import namedtuple

class PerformanceData(namedtuple("PerformanceData", ["time", "mem", "cores"])):
    """Performance Data. Defines the resources or performance a task used,
    is limited to use, or is expected to use.

    :param time: Wall clock time in minutes.
    :type time: int

    :param mem: RAM Usage in MB (8*1024*1024 bits).
    :type mem: int

    :param cores: CPU cores.
    :type cores: int
    """
    pass # the class definition is just for the docstring


class SLURMMixin(object):
    """This class enables the RunContext class to dispatch tasks to
    SLURM. Use it like so:

    .. code:: python

      from anadama import RunContext

      from anadama.runcontext.grid import SLURMMixin

      class SlurmContext(RunContext, SLURMMixin):
          pass

      ctx = SlurmContext(partition="general")
      ctx.do("wget "
             "ftp://public-ftp.hmpdacc.org/"
             "HMMCP/finalData/hmp1.v35.hq.otu.counts.bz2 "
             "-O @{input/hmp1.v35.hq.otu.counts.bz2}")

      # run on slurm with 200 MB of memory, 4 cores, and 60 minutes
      t1 = ctx.slurm_do("pbzip2 -d -p 4 < #{input/hmp1.v35.hq.otu.counts.bz2} "
                        "> @{input/hmp1.v35.hq.otu.counts}",
                        mem=200, cores=4, time=60)

      # run on slurm on the serial_requeue partition
      ctx.slurm_add_task("some_huge_analysis {depends[0]} {targets[0]}",
                         depends=t1.targets, targets="output.txt",
                         mem=4000, cores=1, time=300, partition="serial_requeue")


      ctx.go()


    :param partition: The name of the SLURM partition to submit tasks to
    :type partition: str

    :keyword tmpdir: A directory to store temporary files in. All
      machines in the cluster must be able to read the contents of
      this directory; uses :mod:`anadama.picklerunner` to create
      self-contained scripts to run individual tasks and calls
      ``srun`` to run the script on the cluster.
    :type tmpdir: str

    :type extra_srun_flags: list of str

    """

    def __init__(self, partition, tmpdir="/tmp", extra_srun_flags=[],
                 *args, **kwargs):
        super(SLURMMixin, self).__init__(*args, **kwargs)
        self.slurm_partition = partition
        self.slurm_tmpdir = tmpdir
        self.extra_srun_flags = extra_srun_flags
        
        self.slurm_task_data = dict()


    def _kwargs_extract(self, kwargs_dict):
        time = kwargs_dict.pop("time", None)
        if time is None:
            raise TypeError("`time' is a required keyword argument")
        mem = kwargs_dict.pop("mem", None)
        if mem is None:
            raise TypeError("`mem' is a required keyword argument")
        cores = kwargs_dict.pop("cores", 1)
        partition = kwargs_dict.pop("partition", self.partition)
        extra_srun_flags = kwargs_dict.pop("extra_srun_flags",
                                           self.extra_srun_flags)
        return (PerformanceData(int(time), int(mem), int(cores)),
                partition, extra_srun_flags)


    def slurm_do(self, *args, **kwargs):
        params = self._kwargs_extract(kwargs)
        task = self.do(*args, **kwargs)
        self.slurm_task_data[task.task_no] = params
        return task

    
    def slurm_add_task(self, *args, **kwargs):
        params = self._kwargs_extract(kwargs)
        task = self.add_task(*args, **kwargs)
        self.slurm_task_data[task.task_no] = params
        return task


    def go(self, n_slurm_parallel=1, *args, **kwargs):
        kwargs.pop("runner", None) # ignore the runner keyword
        local_n_parallel = kwargs.pop("n_parallel", 1)
        runner = runners.current_grid_runner()
        runner.add_worker(runners.ParallelLocalWorker,
                          name="local", rate=local_n_parallel, default=True)
        runner.add_worker(runners.SLURMWorker, name="slurm",
                          rate=n_slurm_parallel,
                          extra_kwargs={"tmpdir":self.slurm_tmpdir})
        runner.routes.update([
            ( task_idx, ("slurm", extra) )
            for task_idx, extra in self.slurm_task_data.iteritems()
        ])
        return super(SLURMMixin, self).go(runner=runner, *args, **kwargs)
        
