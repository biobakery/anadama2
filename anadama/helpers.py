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
    be careful about doing things like 
    ``sh('df -h > data; rm -rf /')``; both commands are executed 
    and bad things will happen.
    :type s:

    """

    def actually_sh(ctx=None):
        return _sh(s, **kwargs)
    return actually_sh
