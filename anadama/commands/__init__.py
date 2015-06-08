import sys

from doit.cmd_run import opt_reporter
from doit.cmd_base import DoitCmdBase, Command
from doit.cmdparse import CmdOption

from .. import dag
from ..runner import RUNNER_MAP
from ..provenance import find_versions


opt_runner = dict(
    name    = "runner",
    long    = "runner",
    default = "MRunner",
    help    = ("Runner to use for executing tasks."
               " Choices: "+",".join(RUNNER_MAP.keys()) ),
    type    = str
)

opt_tmpfiles = dict(
    name    = "tmpfiledir",
    long    = "tmpfiledir",
    default = "/tmp",
    help    = "Where to save temporary files",
    type    = str
)

opt_pipeline_name = dict(
    name    = "pipeline_name",
    long    = "pipeline_name",
    default = "Custom Pipeline",
    help    = "Optional name to give to the current pipeline",
    type    = str
)

opt_reporter['help'] = \
"""Choose output reporter. Available:
'default': report output on console
'executed-only': no output for skipped (up-to-date) and group tasks
'json': output result in json format
'verbose': output actions on console as they're executed
[default: %(default)s]
"""

class AnadamaCmdBase(DoitCmdBase):
    my_base_opts = ()
    my_opts = ()

    def set_options(self):
        opt_list = (self.my_base_opts + self.my_opts + 
                    self.base_options + self._loader.cmd_options +
                    self.cmd_options)
        return [CmdOption(opt) for opt in opt_list]

    def help(self):
        text = super(AnadamaCmdBase, self).help()
        return text.replace("doit", "anadama")



from .run import Run
class ListDag(Run):
    my_opts = (opt_runner, opt_tmpfiles, opt_pipeline_name)
    name = "dag"
    doc_purpose = "print execution tree"
    doc_usage = "[TASK ...]"


    def _execute(self, 
                 verbosity=None, always=False, continue_=False,
                 reporter='default', num_process=0,
                 single=False, pipeline_name="Custom Pipeline",
                 **kwargs):
        # **kwargs are thrown away
        self.opt_values['runner'] = 'jenkins'
        dag.TMP_FILE_DIR = self.opt_values["tmpfiledir"]
        return super(ListDag, self)._execute(outfile=sys.stdout, verbosity=verbosity,
                                             always=always, continue_=continue_,
                                             reporter=reporter,
                                             num_process=num_process,
                                             par_type="process", single=single,
                                             pipeline_name=pipeline_name)


class BinaryProvenance(Command):
    name = "binary_provenance"
    doc_purpose = "print versions for required dependencies"
    doc_usage = "<module> [<module> [<module...]]"
    
    def execute(self, opt_values, pos_args):
        """Import workflow modules as specified from positional arguments and
        determine versions of installed executables and other
        dependencies via py:ref:`provenance.find_versions`.

        For each external dependency installed, print the name of the
        dependency and the version of the dependency.

        """

        for mod_name in pos_args:
            for binary, version in find_versions(mod_name):
                print binary, "\t", version



from .help import Help
from .pipeline import RunPipeline, DagPipeline, Skeleton

all = (Run, ListDag, Help, BinaryProvenance, RunPipeline, DagPipeline, Skeleton)
