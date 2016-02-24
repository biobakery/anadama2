"""Contains functions that help create tasks. All functions contained
herein are intended for use with
:meth:`runcontext.RunContext.add_task`. This means that the functions
in here don't immediately do what they say; they return functions
that, when called, do that they say (they're closures). Sorry if that
breaks your brain.

"""

from .util import sh as _sh

def sh(s, **kwargs):
    """Execute a shell command. All further keywords are passed to
    ``subprocess.Popen``

    :param s: The command to execute. Passed directly to a shell, so
      be careful about doing things like ``sh('df -h > data; rm -rf
      /')``; both commands are executed and bad things will happen.
    :type s: str

    """

    def actually_sh(ctx=None):
        kwargs['shell'] = True
        return _sh(s, **kwargs)
    return actually_sh


def parse_sh(s, **kwargs):
    """Do the same thing as :func:`helpers.sh`, but do some extra
    interpreting and formatting of the shell command before handing it
    over to the shell. For those familiar with python's
    ``string.format()`` method, the list of dependencies and the list
    of targets are given to ``.format`` like so:
    ``.format(targs=targets, deps=depends)``. Here's a synopsis of
    common use cases:

    - ``{targs[0]}`` is formatted to the first target
    
    - ``{deps[2]}`` is formatted to the third dependency

    :param s: The command to execute. Passed directly to a shell, so
      be careful about doing things like 
      ``sh('df -h > data; rm -rf /')``; both commands are executed 
      and bad things will happen.
    :type s: str

    """

    def actually_sh(ctx):
        kwargs['shell'] = True
        t = ctx.current_task()
        return _sh(s.format(deps=t.depends, targs=t.targets), **kwargs)
