import inspect
from cStringIO import StringIO
import pprint

from doit.cmd_run import (
    opt_always, opt_continue, opt_verbosity, 
    opt_reporter, opt_num_process, opt_single
)

from ..loader import PipelineLoader

from . import Run
from . import ListDag
from . import opt_runner, opt_tmpfiles, opt_pipeline_name


def print_pipeline_help(pipeline_name):
    pipeline_class = PipelineLoader._import(pipeline_name)
    message = StringIO()
    spec = inspect.getargspec(pipeline_class.__init__)
    args = [a for a in spec.args if a != "self"] #filter out self
    print >> message, "Arguments: "
    print >> message, pprint.pformat(args)

    print >> message, "Default options: "
    print >> message, pprint.pformat(pipeline_class.default_options)

    print >> message, "" #newline
    print >> message, pipeline_class.__doc__

    print >> message, ""
    print >> message, pipeline_class.__init__.__doc__

    print message.getvalue()


class RunPipeline(Run):
    name = "pipeline"
    doc_purpose = "run an AnADAMA pipeline"
    doc_usage = "<module.Pipeline> [options]"

    cmd_options = (opt_always, opt_continue, opt_verbosity, 
                   opt_reporter, opt_num_process, opt_single)

    my_opts = (opt_runner, opt_tmpfiles, opt_pipeline_name)

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
    doc_usage = "<some_module.SomePipeline> [options]"
