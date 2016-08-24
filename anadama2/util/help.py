"""Utility functions for getting helpful messages out of pipelines"""

import sys
import pprint
import inspect


def _specargs(func):
    spec = inspect.getargspec(func)
    return [a for a in spec.args if a != "self"] #filter out self


def _maybe_doc(func, key):
    if not hasattr(func, "__doc__"):
        return "No {} documentation available"
    else:
        return func.__doc__


def _print_doc(cls, stream, key="Pipeline"):
    msg = key+" general documentation"
    print >> stream, "#" * len(msg)
    print >> stream, msg
    print >> stream, "#" * len(msg)
    print >> stream, "" #newline
    print >> stream, _maybe_doc(cls, "pipeline")
    print >> stream, ""

    msg = key+" argument documentation"
    print >> stream, "#" * len(msg) 
    print >> stream, msg
    print >> stream, "#" * len(msg) 
    print >> stream, ""
    print >> stream, _maybe_doc(cls.__init__, "pipeline argument")
    print >> stream, ""

    
def print_pipeline_help(pipeline_class,
                        optional_pipelines=list(),
                        stream=sys.stdout):

    args = _specargs(pipeline_class.__init__)
    for cls in optional_pipelines:
        args += _specargs(cls.__init__)

    print >> stream, "Arguments: "
    print >> stream, pprint.pformat(args)

    print >> stream, "Default options: "
    print >> stream, pprint.pformat(pipeline_class.default_options)

    _print_doc(pipeline_class, stream)
    for cls in optional_pipelines:
        key = cls.name if hasattr(cls, "name") else cls.__name__
        _print_doc(cls, stream, key)

    return stream
