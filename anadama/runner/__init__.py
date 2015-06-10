from doit.runner import Runner, MRunner, MThreadRunner

from .jenkins import JenkinsRunner
from .slurm import SlurmRunner

RUNNER_MAP = {
    'jenkins': JenkinsRunner,
    'slurm': SlurmRunner,
    'mrunner': MRunner,
    'runner': Runner,
    'mthreadrunner': MThreadRunner,
}

GRID_RUNNER_MAP = {
    'slurm': SlurmRunner
}
