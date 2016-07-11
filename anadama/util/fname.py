"""utilities for working with filenames"""

import os
import re
import traceback
from . import mkdirp

_owd = os.getcwd()
original_wd = lambda: _owd
cwd = os.getcwd

def mangle(fname, tag=None, dir=None, ext=None):
    new = fname[:]
    if tag: # don't want empty strings
        new = addtag(new, tag)
    if dir is not None:
        new = os.path.join(dir, os.path.basename(new))
    if ext is not None:
        new = addext(rmext(new, all=True), ext)
    return new_file(new)


def _new_file(*names, **opts):
    basedir = opts.get("basedir")
    for name in names:
        if basedir:
            name = os.path.abspath(
                os.path.join(
                    basedir, os.path.basename(name)
                )
            )
        
        dir = os.path.dirname(name)
        if dir and not os.path.exists(dir):
            mkdirp(dir)

        yield name


def new_file(*names, **opts):
    iterator = _new_file(*names, basedir=opts.get("basedir"))
    if len(names) == 1:
        return iterator.next()
    else:
        return list(iterator)


def addtag(name_str, tag_str):
    path, name_str = os.path.split(name_str)
    match = re.match(r'(.+)(\..*)', name_str)
    if match:
        base, ext = match.groups()
        return os.path.join(path, base + "_" + tag_str + ext)
    else:
        return os.path.join(path, name_str + "_" + tag_str)
        

def addext(name_str, tag_str):
    if not tag_str:
        return name_str
    return name_str + "." + tag_str


def rmext(name_str, all=False):
    """remove file extensions 

    :keyword all: Boolean; removes all extensions if True, else just
      the outside one

    """

    _match = lambda name_str: re.match(r'(.+)(\..*)', name_str)
    path, name_str = os.path.split(name_str)
    match = _match(name_str)
    while match:
        name_str = match.group(1)
        match = _match(name_str)
        if not all:
            break

    return os.path.join(path, name_str)


def script_wd():
    """Return the directory of the first non-anadama module in the
    stack. Most of the time, that's the directory that contains the script
    that calls ctx.go()"""

    anamodpath = os.path.dirname(__file__)
    for stack_item in reversed(traceback.extract_stack()):
        if not stack_item[0].startswith(anamodpath):
            return os.path.abspath(os.path.dirname(stack_item[0]))
    return cwd()

