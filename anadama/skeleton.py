import os
from os.path import join
import sys
from pprint import pformat

import yaml
from doit.exceptions import InvalidCommand

from .loader import PipelineLoader
from .util.help import print_pipeline_help

OPTIONS_DIR = "_options"

def logger_init(verbose=True):
    if verbose:
        return lambda msg, *args: sys.stderr.write(msg.format(*args))
    else:
        return lambda msg, *args: None


def skel_list(pipeline, the_dir):
    os.mkdir(the_dir)
    open(join(the_dir,".placeholder"), 'w').close()


skel_funcs = {
    list: skel_list,
    str: skel_list
}


YAML_HELP = \
"""#  Options are defined as key: value pairs. 
#  For example, to set the ``nproc`` option to ``16``, do
#  nproc: 16
#  
#  If a workflow expects a boolean value, just write in true or false. i.e:
#  parallel: true
#  
#  Nested mappings or dictionaries are specified with indents, like so:
#  qiime_opts: 
#      a: true
#      jobs_to_start: 6
#
#  For more information, check out:
#    - http://pyyaml.org/wiki/PyYAMLDocumentation#YAMLsyntax
#    - http://rschwager-hsph.bitbucket.org/documentation/anadama/your_own_pipeline.html#your-own-pipeline
"""


def commented(doc_str):
    return "\n".join([
        "#  "+line for line in doc_str.split("\n")
    ])
    
def write_options(options_dict, fname, workflow_func=None):
    with open(fname, 'w') as f:
        print >> f, YAML_HELP
        if all((workflow_func,
                hasattr(workflow_func, "__doc__"),
                workflow_func.__doc__)):
            print >> f, "#  Workflow Documentation:"
            print >> f, commented(workflow_func.__doc__)
        if options_dict:
            yaml.safe_dump(options_dict, stream=f,
                           default_flow_style=False)


_default_template = None
def default_template():
    global _default_template
    if not _default_template:
        here = os.path.abspath(os.path.dirname(__file__))
        with open(join(here, "default_skel_template.txt")) as f: 
           _default_template = f.read()
    return _default_template


def format_optimports(optpipe_classes):
    return "\n".join([
        "from {cls.__module__} import {cls.__name__}".format(cls=c)
        for c in optpipe_classes
    ])


def format_optappends(optpipe_classes):
    return "\n    ".join([
        "pipeline.append({cls.__name__})".format(cls=c)
        for c in optpipe_classes
    ])


def make_pipeline_skeleton(pipeline_name,
                           optional_pipelines=list(),
                           verbose=True, template=None):
    log = logger_init(verbose)
    PipelineClass = PipelineLoader._import(pipeline_name)
    optpipe_classes = map(PipelineLoader._import, optional_pipelines)

    here = os.getcwd()
    input_dir = join(here, "input")
    options_dir = join(input_dir, OPTIONS_DIR)

    def _combine(attr):
        orig = list( getattr(PipelineClass, attr).items() )
        for p in optpipe_classes:
            orig += list( getattr(p, attr).items() )
        return dict(orig)
    attrs_to_combine = ("products", "default_options", "workflows")
    allprods, allopts, allworks = map(_combine, attrs_to_combine)

    opt_import_stmts, append_stmts = str(), str()
    if optpipe_classes:
        opt_import_stmts = format_optimports(optpipe_classes)
        append_stmts = format_optappends(optpipe_classes)

    if os.path.exists(input_dir):
        raise InvalidCommand("Input directory already exists: "+input_dir)
    log("Constructing input skeleton at {}.\n", input_dir)
    os.mkdir(input_dir)
    os.mkdir(options_dir)

    if not template:
        template = default_template()
    
    product_dirs = list()
    for name, prod in allprods.iteritems():
        skel_func = skel_funcs.get(type(prod))
        if not skel_func:
            msg = "Unable to handle products of type {}"
            raise ValueError(msg.format(skel_func))

        skel_dir = join(input_dir, name)
        log("Creating input directory {} for {}...",
            skel_dir, str(type(prod)))
        skel_func(PipelineClass, skel_dir)
        log("Done.\n")
        product_dirs.append(skel_dir)

    for name, opt_dict in allopts.iteritems():
        options_fname = join(options_dir, name+".txt")
        log("Writing default options for {}.{} into {}...",
            pipeline_name, name, options_fname)
        workflow_func = allworks.get(name)
        write_options(opt_dict, options_fname, workflow_func)
        log("Done.\n")

    log("Writing dodo.py file...")
    dodo_fname = "dodo.py"
    with open(dodo_fname, 'w') as dodo_f:
        print >> dodo_f, template.format(
            pipeline_class=PipelineClass,
            known_input_directories=pformat(product_dirs),
            options_dir=repr(options_dir),
            append_imports=opt_import_stmts,
            append_statements=append_stmts
        )
    log("Done.\n")

    help_fname = "README.rst"
    log("Writing help file to {}...", help_fname)
    with open(help_fname, 'w') as help_f:
        print_pipeline_help(PipelineClass, optpipe_classes, stream=help_f)
    log("Done.\n")

    log("Complete.\n")

    return allprods, allopts, allworks
