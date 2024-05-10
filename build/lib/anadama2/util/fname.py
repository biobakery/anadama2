# -*- coding: utf-8 -*-
"""utilities for working with filenames"""

import os
import re
import sys
from . import mkdirp

_owd = os.getcwd()
original_wd = lambda: _owd
cwd = os.getcwd

def mangle(fname, tag=None, dir=None, ext=None):
    """Create a new filename from an old filename.

    :param fname: The old filename used as a template for the new
      filename
    
    :keyword tag: Add this tag to the new filename. See
      :func:`anadama2.util.fname.addtag`.
    
    :keyword dir: Change the path of the filename to ``dir``

    :keyword ext: Swap out the extension for ``ext``.

    :returns: The new filename
    """

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
        return next(iterator)
    else:
        return list(iterator)


def addtag(name_str, tag_str):

    """Add a tag to a filename. The tag is placed at the end of the
    filename just before the extension. If the filename has no extension,
    it's placed right on the end.

    Here's an example::

      >>> from anadama2.util.fname import addtag
      >>> addtag("myfile.txt", "big")
      'myfile_big.txt'
      >>>

    """
    path, name_str = os.path.split(name_str)
    match = re.match(r'(.+?)(\..*)', name_str)
    if match:
        base, ext = match.groups()
        return os.path.join(path, base + "_" + tag_str + ext)
    else:
        return os.path.join(path, name_str + "_" + tag_str)


def addext(name_str, ext_str):
    """Add a file extension to a filename
    
    :param name_str: The filename that will get the extension

    :param ext_str: The extension (no leading ``.`` required)

    :returns: The filename with the extension
    """

    if not ext_str:
        return name_str
    return name_str + "." + ext_str


def rmext(name_str, all=False):
    """Remove file extensions 

    :param name_str: The filename to remove extensions from
    :type name_str: str or unicode

    :keyword all: remove all extensions.
    :type all: bool

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
    """Return the directory of script being executed (sys.argv[0]). Most
    of the time, that's the directory that contains the script that
    calls ctx.go()

    """

    return os.path.abspath(os.path.dirname(sys.argv[0]))

