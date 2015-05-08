import sys

import six
from six import StringIO

from doit.action import CmdAction as DoitCmdAction
from doit.action import PythonAction as DoitPythonAction
from doit.action import Writer
from doit.exceptions import TaskFailed, TaskError


class CmdAction(DoitCmdAction):
    """
    AnADAMA's CmdAction has the option of being verbose.
    Just do something like this:: 
     
        my_action = CmdAction(..., verbose=True)

    """

    def __init__(self, *args, **kwargs):
        self.verbose = kwargs.pop('verbose', False)
        super(CmdAction, self).__init__(*args, **kwargs)

    def execute(self, *args, **kwargs):
        # Hope calling expand_action() has no side effects!
        if self.verbose:
            print >> sys.stderr, self.expand_action()
        return super(CmdAction, self).execute(*args, **kwargs)


class PythonAction(DoitPythonAction):

    def execute(self, out=None, err=None):
        """Execute command action
        both stdout and stderr from the command are captured and saved
        on self.out/err. Real time output is controlled by parameters

        :param out: None - no real time output a file like object (has
                    write method)
        
        :param err: idem
        
        :return failure: see CmdAction.execute

        """
        # set std stream
        old_stdout = sys.stdout
        output = StringIO()
        out_writer = Writer()
        # capture output but preserve isatty() from original stream
        out_writer.add_writer(output, old_stdout.isatty())
        if out:
            out_writer.add_writer(out)
        sys.stdout = out_writer

        old_stderr = sys.stderr
        errput = StringIO()
        err_writer = Writer()
        err_writer.add_writer(errput, old_stderr.isatty())
        if err:
            err_writer.add_writer(err)
        sys.stderr = err_writer

        kwargs = self._prepare_kwargs()

        # execute action / callable
        try:
            returned_value = self.py_callable(*self.args, **kwargs)
        except Exception as exception:
            return TaskError("PythonAction Error", exception)
        finally:
            # restore std streams /log captured streams
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            self.out = output.getvalue()
            self.err = errput.getvalue()

        # if callable returns false. Task failed
        if returned_value is False:
            return TaskFailed("Python Task failed: '%s' returned %s" %
                              (self.py_callable, returned_value))
        elif returned_value is True or returned_value is None:
            pass
        elif isinstance(returned_value, six.string_types):
            self.result = returned_value
        elif isinstance(returned_value, dict):
            self.values = returned_value
            self.result = returned_value
        elif isinstance(returned_value, TaskFailed) \
             or isinstance(returned_value, TaskError):
            return returned_value
        else:
            return TaskError("Python Task error: '%s'. It must return:\n"
                             "False for failed task.\n"
                             "True, None, string or dict for successful task\n"
                             "returned %s (%s)" %
                             (self.py_callable, returned_value,
                              type(returned_value)))
