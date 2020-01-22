# -*- coding: utf-8 -*-
"""Contains functions that help create tasks. All functions contained
herein are intended for use with
:meth:`anadama2.workflow.Workflow.add_task`. This means that the functions
in here don't immediately do what they say; they return functions
that, when called, do that they say (they're closures). Sorry if that
breaks your brain.

Using closures lets you add tasks like this:

.. code:: python

  from anadama2 import Workflow
  from anadama2.helpers import sh

  ctx = Workflow()
  ctx.add_task(sh("my fancy shell command"),
               targets="foobaz.txt")

Instead of this:

.. code:: python


  from anadama2 import Workflow
  from anadama2.util import sh # <--- note the different import

  ctx = Workflow()
  ctx.add_task(lambda task: sh("my fancy shell command"),
               targets="foobaz.txt")


"""

import os
import shutil
import logging
import re

import six

from .util import sh as _sh
from .util import sugar_list

from .tracked import try_get_local_path
from .tracked import s3_folder
from .tracked import AWSHugeTrackedFile
from .reporters import SHELL_COMMAND

def file_size(depends):
    """ Return the size of the file in GB """
    
    # allow for paths to files and also dependency classes
    file_name = depends
    if hasattr(depends,"name"):
        file_name = depends.name

    if s3_folder(file_name):
        size = AWSHugeTrackedFile(file_name).file_size()    
    else:
        try:
            size = os.path.getsize(file_name) / (1024.0**3)
        except (OSError, AttributeError):
            size = 0
        
    return size 


def apply_sh(actions):
    """Add the shell function to any actions that are strings"""
    return [ a if six.callable(a) else sh(a, log_command=False) for a in actions ]


def sh(s, log_command=True, **kwargs):
    """Execute a shell command. All further keywords are passed to
    :class:`subprocess.Popen`

    :param s: The command to execute. Passed directly to a shell, so
      be careful about doing things like ``sh('df -h > data; rm -rf
      /')``; both commands are executed and bad things will happen.
    :type s: str

    """
    def actually_sh(task=None):
        logger = logging.getLogger(__name__)
        if log_command:
            logger.info(SHELL_COMMAND+s)
        kwargs['shell'] = True
        ret = _sh(s, **kwargs)
        logger.info("Execution complete. Stdout: %s\nStderr: %s",
                    ret[0] or '',
                    ret[1] or '')
    return actually_sh

def build_actions(actions, deps, targs, visible, kwds, use_parse_sh=True):
    actions = filter(None, sugar_list(actions))
    if use_parse_sh:
        return [ a if six.callable(a) else format_command(a, depends=deps, targets=targs, **kwds)
                 for a in actions ]
    else:
        return [a for a in actions]


def format_command(command, **kwargs):
    """Format the shell command to allow for special variables 
    Here's a synopsis of
    common use cases:

    - ``[targets[0]]`` is formatted to the first target
    
    - ``[depends[2]]`` is formatted to the third dependency

    Extra keyword arguments are also added to the formatting keyword
    arguments. Thus, adding a keyword argument of ``threads=1`` makes
    ``[threads]`` be formatted to ``1`` in the shell command.
    :type s: str
    """

    # start with the longest keys for replacement first
    keys=sorted(kwargs.keys(), key=len)
        
    # store the original command, incase needed for error message
    original_command = command
        
    # replace instances of "[key]" with value (local path if available)
    # for values that are lists, replace "[key[0]]" with value
    for key in keys:
        replacement=kwargs[key]
        if isinstance(replacement, list) or isinstance(replacement, tuple):
            replacement=map(try_get_local_path,replacement)
            for i, item in enumerate(replacement):
                command=command.replace("["+str(key)+"["+str(i)+"]]",str(item))
            # for lists/tuples with one item, allow for index to not be included
            if len(list(replacement)) == 1:
                command=command.replace("["+str(key)+"]",str(replacement[0]))
        else:
            replacement=try_get_local_path(replacement) 
            command=command.replace("["+str(key)+"]",str(replacement))
            # also allow for the item to be referenced as the first object
            command=command.replace("["+str(key)+"[0]]",str(replacement))
            
    # check for any keywords in the command that were not replaced
    # allow for bash test constructs
    if re.search("\[[a-zA-Z]",command):
        message="Unable to replace all keys in command.  "
        message+="Original command: "+original_command+"  "
        message+="Final formatted command: "+command+"  "
        raise KeyError(message)
    
    return command 

def parse_sh(s, **kwargs):
    """Do the same thing as :func:`anadama2.helpers.sh`, but do some extra
    interpreting and formatting of the shell command before handing it
    over to the shell. For formatting information, see 
    :func:`anadama2.helpers.format_command`.
    :type s: str
    """

    def actually_sh(task):
        fmtd = format_command(s, depends=task.depends, targets=task.targets, **kwargs)
        logger = logging.getLogger(__name__)
        logger.info("Executing with shell: "+fmtd)
        ret = _sh(fmtd, shell=True)
        logger.info("Execution complete. Stdout: %s\nStderr: %s",
                    ret[0] or '',
                    ret[1] or '')
    return actually_sh


def system(args_list, stdin=None, stdout=None, stdout_clobber=None,
           stderr=None, stderr_clobber=None, working_dir=None, 
           **kwargs):
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
    args_list = list(map(str, args_list))
    __sh = _sh
    if working_dir is not None:
        def __sh(*a, **kw):
            prev = os.getcwd()
            os.chdir(working_dir)
            try:
                ret = _sh(*a, **kw)
            finally:
                os.chdir(prev)
            return ret
    def actually_system(task):
        files = []
        if stdin:
            f = kwargs['stdin'] = open(stdin, 'rb')
            files.append(f)
        if stdout:
            f = kwargs['stdout'] = open(stdout, 'ab')
            files.append(f)
        if stdout_clobber:
            f = kwargs['stdout'] = open(stdout_clobber, 'wb')
            files.append(f)
        if stderr:
            f = kwargs['stderr'] = open(stderr, 'ab')
            files.append(f)
        if stderr_clobber:
            f = kwargs['stderr'] = open(stderr_clobber, 'wb')
            files.append(f)
        try:
            logger = logging.getLogger(__name__)
            logger.info(
                "Forking subprocess %s with args %s", args_list, kwargs)
            ret = _sh(args_list, **kwargs)
            logger.info("Execution complete. Stdout: %s\nStderr: %s",
                        ret[0] or '',
                        ret[1] or '')
        finally:
            for f in files:
                f.close()
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
                logging.getLogger(__name__).info("Removing "+f)
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
            logging.getLogger(__name__).info("Removing recursively: "+f)
            shutil.rmtree(f, ignore_errors=ignore_missing)
    return actually_rm_r

