# -*- coding: utf-8 -*-
"""Save a task to a script for running by other programs"""

import os
import sys
import tempfile
import copy

import cloudpickle

template = \
r"""

import cloudpickle

from anadama2.runners import _run_task_locally


# set the variables
in_file = "{in_file}"
out_file = "{out_file}"

# load the task
task = cloudpickle.load(open(in_file,"rb"))

# run the task
result = _run_task_locally(task)

# write the pickled results to a file
cloudpickle.dump(result,open(out_file,"wb"))

"""

class PickleScript(object):
    def __init__(self, task, tmpdir, suffix):
        # create temp files for the script, input and the output file
        output_handle, output_file = tempfile.mkstemp(dir=tmpdir, suffix=suffix+"_output.pkl")
        os.close(output_handle)
        input_handle, input_file = tempfile.mkstemp(dir=tmpdir, suffix=suffix+"_input.pkl")
        os.close(input_handle)
        script_handle, script_file = tempfile.mkstemp(dir=tmpdir, suffix=suffix+"_picklerunner.py")
        os.close(script_handle)
        
        self.task = task
        self.output_file = output_file
        self.input_file = input_file
        self.script_file = script_file
        
    def create_task(self):
        # write the input pickle file
        with open(self.input_file,"wb") as file_handle:
            cloudpickle.dump(self.task,file_handle)
        
        # write the script
        custom_script = template.format(in_file=self.input_file, out_file=self.output_file)
        with open(self.script_file,"wb") as file_handle:
                file_handle.write(custom_script.encode("utf-8"))
        
        # update the task to run the pickle script
        pickle_task = copy.deepcopy(self.task)
        pickle_task.actions = [self.run_command()]
        
        return pickle_task
        
    def run_command(self):
        return sys.executable+" "+self.script_file

    def result(self, result):
        # try to get the result from running the pickled function
        extra_error = None
        try:
            with open(self.output_file,"rb") as file_handle:
                result = cloudpickle.load(file_handle)
        except (ValueError, EOFError):
            extra_error = "Unable to decode pickle task result"
        
        if extra_error:    
            result = result._replace(error=str(result.error)+extra_error)
            
        return result

