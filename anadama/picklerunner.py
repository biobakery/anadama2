"""Save a task to a script for running by other programs"""

import os
import sys
from tempfile import NamedTemporaryFile

from .pickler import cloudpickle

template = \
"""#!{python_bin}

import os
import sys
import cPickle as pickle

the_pickle = {pickle}

task = pickle.loads(the_pickle)
task.__init__(task.name, task.some_actions)

def remove_myself():
    myself = os.path.abspath(__file__)
    os.remove(myself)

def be_verbose():
    for action in task.actions:
        if hasattr(action, 'expand_action'):
            print action.expand_action()

def main():
    return task.execute(out=sys.stdout, err=sys.stderr)

if __name__ == '__main__':
    if "-v" in sys.argv or "--verbose" in sys.argv:
        be_verbose()

    ret = main()

    if "-r" in sys.argv or "--remove" in sys.argv:
        remove_myself()

    sys.exit(ret)

"""

class PickleScript(object):
    def __init__(self, task):
        self.task = task
        self._path = None

        self.task.some_actions = self.task._actions

    @property
    def path(self):
        if self._path is None:
            raise AttributeError("A PickleScript must be saved"
                                 " before it has a path.")
        else:
            return self._path

    def save(self, path=None, to_fp=None):
        if bool(path) == bool(to_fp): # logical not xor
            raise ValueError("Need either path or to_fp")
        if path:
            self._path = path
            with open(path, 'rb') as out_file:
                self.render(to_fp=out_file)
        elif to_fp:
            self._path = to_fp.name
            self.render(to_fp=to_fp)

            
    def render(self, python_bin=None, to_fp=None):
        if not python_bin:
            python_bin = os.path.join(sys.prefix, "bin", "python")
        rendered = template.format(
            python_bin = python_bin,
            pickle     = repr(cloudpickle.dumps(self.task))
        )
        if not to_fp:
            return rendered
        else:
            to_fp.write(rendered)
    
    def __repr__(self):
        return self.render()


def tmp(task, chmod=0o755, *args, **kwargs):
    kwargs.pop('delete', None) # don't delete it
    suffix = kwargs.pop('suffix', '') + "_picklerunner.py"
    with NamedTemporaryFile(delete=False, suffix=suffix, 
                            *args, **kwargs) as tmp_file:
        script = PickleScript(task)
        script.save(to_fp=tmp_file)
    os.chmod(script.path, chmod)
    return script

        
