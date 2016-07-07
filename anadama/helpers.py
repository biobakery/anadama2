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
import logging
import contextlib

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
        logging.getLogger(__name__).debug("Executing with shell: "+s)
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
        fmtd = s.format(depends=task.depends, targets=task.targets)
        logging.getLogger(__name__).debug("Executing with shell: "+fmtd)
        return _sh(fmtd, **kwargs)
    return actually_sh


def system(args_list, stdin=None, stdout=None, stdout_clobber=None,
           stderr=None, stderr_clobber=None, **kwargs):
    """Execute a system call (no shell will be used). All further keywords
    are passed to :class:`subprocess.Popen`

    :param args_list: The argv to be passed to the system call.
    :type args_list: list

    :keyword stdin: If provided, the name of the file to open and send
      to the subprocess' standard input. By default no data is sent to
      the process.
    :type stdin: str

    :keyword stdout: If provided, the name of the file to send output
      from the subprocess' standard output. Standard output is
      appended to the file. By default all data from the subprocess
      standard out is sent to the standard out of the executing
      process
    :type stdout: str

    :keyword stdout_clobber: If provided, the name of the file to send
      output from the subprocess' standard output. If the file already
      exists, it will be truncated before it receives writes.
    :type stdout_clobber: str

    :keyword stderr: If provided, the name of the file to send output
      from the subprocess' standard error output. Standard error
      output is appended to the file. By default all data from the
      subprocess standard error is sent to the standard error of the
      executing process.
    :type stderr: str

    :keyword stderr_clobber: If provided, the name of the file to send
      output from the subprocess' standard error output. If the file
      already exists, it will be truncated before it receives writes.
    :type stderr_clobber: str

    """
    kwargs.pop("shell", None)
    args_list = map(str, args_list)
    def actually_system(task):
        files = []
        if stdin:
            f = kwargs['stdin'] = open(stdin, 'r')
            files.append(f)
        if stdout:
            f = kwargs['stdout'] = open(stdout, 'a')
            files.append(f)
        if stdout_clobber:
            f = kwargs['stdout'] = open(stdout_clobber, 'w')
            files.append(f)
        if stderr:
            f = kwargs['stderr'] = open(stderr, 'a')
            files.append(f)
        if stderr_clobber:
            f = kwargs['stderr'] = open(stderr_clobber, 'w')
            files.append(f)
        with contextlib.nested(*files):
            logging.getLogger(__name__).debug(
                "Forking subprocess %s with args %s", args_list, kwargs)
            ret = _sh(args_list, **kwargs)
        return ret
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
                logging.getLogger(__name__).debug("Removing "+f)
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
            logging.getLogger(__name__).debug("Removing recursively: "+f)
            shutil.rmtree(f, ignore_errors=ignore_missing)
    return actually_rm_r

