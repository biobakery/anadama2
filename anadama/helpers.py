"""Contains functions that help create tasks. All functions contained
herein are intended for use with
:meth:`anadama.runcontext.RunContext.add_task`. This means that the functions
in here don't immediately do what they say; they return functions
that, when called, do that they say (they're closures). Sorry if that
breaks your brain.

Using closures lets you add tasks like this:

.. code:: python

  from anadama import RunContext
  from anadama.helpers import sh

  ctx = RunContext()
  ctx.add_task(sh("my fancy shell command"),
               targets="foobaz.txt")

Instead of this:

.. code:: python


  from anadama import RunContext
  from anadama.util import sh # <--- note the different import

  ctx = RunContext()
  ctx.add_task(lambda task: sh("my fancy shell command"),
               targets="foobaz.txt")


"""

import os
import shutil

from .util import sh as _sh
from .util import sugar_list

def sh(s, **kwargs):
    """Execute a shell command. All further keywords are passed to
    :class:`subprocess.Popen`

    :param s: The command to execute. Passed directly to a shell, so
      be careful about doing things like ``sh('df -h > data; rm -rf
      /')``; both commands are executed and bad things will happen.
    :type s: str

    """

    def actually_sh(task=None):
        kwargs['shell'] = True
        return _sh(s, **kwargs)
    return actually_sh


def parse_sh(s, **kwargs):
    """Do the same thing as :func:`anadama.helpers.sh`, but do some extra
    interpreting and formatting of the shell command before handing it
    over to the shell. For those familiar with python's
    :meth:`str.format()` method, the list of dependencies and the list
    of targets are given to ``.format`` like so:
    ``.format(targets=targets, depends=depends)``. Here's a synopsis of
    common use cases:

    - ``{targets[0]}`` is formatted to the first target
    
    - ``{depends[2]}`` is formatted to the third dependency

    :param s: The command to execute. Passed directly to a shell, so
      be careful about doing things like 
      ``sh('df -h > data; rm -rf /')``; both commands are executed 
      and bad things will happen.
    :type s: str

    """

    def actually_sh(task):
        kwargs['shell'] = True
        return _sh(s.format(depends=task.depends,
                            targets=task.targets), **kwargs)
    return actually_sh


def system(args_list, **kwargs):
    """Execute a system call (no shell will be used). All further keywords
    are passed to :class:`subprocess.Popen`

    :param args_list: The argv to be passed to the system call.
    :type args_list: list

    """
    kwargs.pop("shell", None)
    def actually_system(task):
        return _sh(args_list, **kwargs)
    return actually_system


def rm(to_rm, ignore_missing=True):
    """Remove files using :func:`os.remove`.
    
    :param to_rm: The filename or filenames to remove.
    :type to_rm: str or list of str

    :keyword ignore_missing: If one of the filenames isn't a file,
      don't raise an exception
    :type ignore_missing: bool

    """

    def actually_rm(task):
        for f in sugar_list(to_rm):
            if os.path.isfile(f) or not ignore_missing:
                os.remove(f)
    return actually_rm


def rm_r(to_rm, ignore_missing=True):
    """Recursively remove files and directories using
    :func:`shutil.rmtree`.
    
    :param to_rm: The filename or filenames to remove.
    :type to_rm: str or list of str

    :keyword ignore_missing: If one of the filenames isn't a file,
      don't raise an exception
    :type ignore_missing: bool

    """
    def actually_rm_r(task):
        for f in sugar_list(to_rm):
            shutil.rmtree(f, ignore_errors=ignore_missing)
    return actually_rm_r
