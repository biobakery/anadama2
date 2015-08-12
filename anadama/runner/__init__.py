from doit.runner import Runner, MRunner, MThreadRunner

from .jenkins import JenkinsRunner
from .grid import (
    SlurmRunner, 
    LSFRunner, 
    DummyGridRunner,
    SGERunner
)

RUNNER_MAP = {
    'jenkins': JenkinsRunner,
    'slurm': SlurmRunner,
    'lsf': LSFRunner,
    'sge': SGERunner,
    'dummy': DummyGridRunner,
    'mrunner': MRunner,
    'runner': Runner,
    'mthreadrunner': MThreadRunner,
}

GRID_RUNNER_MAP = {
    'slurm': SlurmRunner,
    'lsf': LSFRunner,
    'sge': SGERunner,
    'dummy': DummyGridRunner
}
