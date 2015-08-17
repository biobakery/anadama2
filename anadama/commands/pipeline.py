import sys

from doit.exceptions import InvalidCommand
from doit.cmd_base import Command
from doit.cmd_run import (
    opt_always, opt_continue, opt_verbosity, 
    opt_reporter, opt_num_process, opt_single
)

from ..loader import PipelineLoader
from ..loader import opt_append_pipeline
from ..skeleton import make_pipeline_skeleton

from . import ListDag
from .run import Run

class RunPipeline(Run):
    name = "pipeline"
    doc_purpose = "run an AnADAMA pipeline"
    doc_usage = "<module:Pipeline> [options]"

    cmd_options = (opt_always, opt_continue, opt_verbosity, 
                   opt_reporter, opt_num_process, opt_single)

    def __init__(self, *args, **kwargs):
        kwargs['task_loader'] = PipelineLoader()
        super(RunPipeline, self).__init__(*args, **kwargs)


    def execute(self, opt_values, pos_args, *args, **kwargs):
        if not pos_args:
            raise InvalidCommand("No pipeline specified. Try pipeline -h")
        pipeline_name = pos_args.pop()
        self._loader.pipeline_cls = pipeline_name

        return super(RunPipeline, self).execute(opt_values, pos_args,
                                                *args, **kwargs)


    def _execute(self, verbosity=None, always=False, continue_=False,
                 reporter='default', num_process=0, single=False, 
                 pipeline_name="Custom Pipeline", 
                 **kwargs):
        # **kwargs are thrown away    
        return super(RunPipeline, self)._execute(
            outfile=sys.stdout, verbosity=verbosity,
            always=always, continue_=continue_,
            reporter=reporter,
            num_process=num_process,
            par_type="process", single=single,
            pipeline_name=pipeline_name
        )
        
class DagPipeline(RunPipeline, ListDag):
    name = "pipeline_dag"
    doc_purpose = "print dag from pipeline"



class Skeleton(Command):
    name = "skeleton"
    doc_purpose = ("Create directory-based working "
                   "environment for a pipeline in the current directory")
    doc_usage = "<some_module:SomePipeline>"

    my_opts = ()
    cmd_options = (opt_append_pipeline,)

    def execute(self, opt_values, pos_args, *args, **kwargs):
        if not pos_args:
            raise InvalidCommand("No pipeline specified")
        pipeline_name = pos_args.pop()
        make_pipeline_skeleton(
            pipeline_name,
            optional_pipelines=opt_values['append_pipeline']
        )


    def help(self):
        text = super(Skeleton, self).help()
        return text.replace("doit", "anadama")
