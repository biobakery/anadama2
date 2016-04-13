"""Save a task to a script for running by other programs"""

import os
import sys
import cPickle as pickle
from base64 import b64decode
from tempfile import NamedTemporaryFile

from .pickler import cloudpickle

PICKLE_KEY = "results_as_pickle"

template = \
"""#!{python_bin}

import os
import sys
import cPickle as pickle
from base64 import b64encode

from anadama.runners import _run_task_locally


the_pickle = {pickle}

task = pickle.loads(the_pickle)


def remove_myself():
    myself = os.path.abspath(__file__)
    os.remove(myself)


def main(do_pickle=False):
    result = _run_task_locally(task)
    if do_pickle:
        print "{pickle_key}: "+b64encode(pickle.dumps(result))
    else:
        print result
    if result.error:
        return 1


if __name__ == '__main__':
    do_pickle = '-p' in sys.argv or '--pickle' in sys.argv

    ret = main(do_pickle)

    if "-r" in sys.argv or "--remove" in sys.argv:
        remove_myself()

    sys.exit(ret)

"""

class PickleScript(object):
    def __init__(self, task):
        self.task = task
        self._path = None

        
    @property
    def path(self):
        if self._path is None:
            raise AttributeError("A PickleScript must be saved"
                                 " before it has a path.")
        else:
            return self._path

    def save(self, path=None, to_fp=None):
        if bool(path) == bool(to_fp): # logical xor
            raise ValueError("Need either path or to_fp")
        if path:
            self._path = path
            with open(path, 'rb') as out_file:
                self.render(to_fp=out_file)
        elif to_fp:
            self._path = to_fp.name
            self.render(to_fp=to_fp)

            
    def render(self, python_bin=None, to_fp=None, pickle_key=PICKLE_KEY):
        if not python_bin:
            python_bin = os.path.join(sys.prefix, "bin", "python")
        rendered = template.format(
            python_bin = python_bin,
            pickle     = repr(cloudpickle.dumps(self.task)),
            pickle_key = pickle_key
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

        
def decode(script_output, pickle_key=PICKLE_KEY):
    match = re.search(pickle_key+r': (\S+)$', script_output)
    if not match:
        msg = "Unable to find pickle key `{}' in script output"
        raise ValueError(msg.format(pickle_key))
    return pickle.loads(b64decode(match.group(1)))

    
