import os
import sys
import inspect
import json
import signal

import networkx
from doit.cmd_run import Run as DoitRun
from doit.exceptions import InvalidCommand

from .. import dag
from ..tm import tm_daemon
from ..tm import globals as tm_globals
from ..tm.cli import peekForHash
from ..tm import tm_daemon

from . import opt_runner, opt_tmpfiles, opt_pipeline_name
from . import AnadamaCmdBase


HELP=""" 

%prog - TM (Task Manager) parses the tasks contained in the given
json encoded directed acyclic graph (DAG) file.  This command is
designed to read the json encoded DAG from stdin.  However, a filename
may be specified for input (--tasks_json filename.json) if that method
is preferred.  type currently can be one of three values: local,
slurm, or lsf.

User supplied 'hooks' are called for the given events:
    post_pipeline_failure.sh  
    post_pipeline_success.sh  
    post_task_failure.sh  
    post_task_success.sh  
    pre_pipeline.sh  
    pre_pipeline_restart.sh

"""

RUN_LOCAL = 'local'
PAR_TYPES = {'local': RUN_LOCAL, 
             'process': RUN_LOCAL,
             'thread': RUN_LOCAL,
             'slurm': 'slurm', 
             'lsf': 'lsf'}

opt_flowdb_path = {
    "name": "flowdb_path",
    "long": "flowdb_path",
    "default": "./anadama_flows",
    "help": ("Specify directory where all tasks and logfiles are stored."
             "  Default is the current working directory."),
    "type": str,
}

opt_tasks_json = {
    "name": "tasks_json",
    "long": "tasks_json",
    "default": "",
    "help": "Specify json encrypted dag file",
    "type": str,
}

opt_webgui_port = {
    "name": "webgui_port",
    "long": "webgui_port",
    "default": 8888,
    "help": "Specify the port of the webserver",
    "type": int,
}

opt_hooks_path = {
    "name": "hooks_path",
    "long": "hooks_path",
    "default": "./anadama_hooks",
    "help": "Specify the directory of the user supplied hooks",
    "type": str,
}

# This option needs to go away
opt_source_path = {
    "name": "source_path",
    "long": "source_path",
    "default": "/home/vagrant/hmp2/bin/activate",
    "help": ("Where to source the current environment"),
    "type": int,
}

class RunTaskManager(AnadamaCmdBase, DoitRun):
    name = "task_manager"
    doc_purpose = "run tasks with web GUI"
    doc_usage = "[TASK ...]"

    my_opts = (opt_pipeline_name, opt_flowdb_path, opt_tasks_json,
               opt_webgui_port, opt_hooks_path, opt_source_path)

    def __init__(self, *args, **kwargs):
        self.hashdirectory = ""
        self.rundirectory = ""
        super(RunTaskManager, self).__init__(*args, **kwargs)


    def generate_dag(self, pipeline_name, key="dag"):
        task_dict = self.control.task_dispatcher().tasks
        the_dag, _ = dag.assemble(
            task_dict.itervalues(),
            root_attrs={"pipeline_name": pipeline_name}
        )
        ordered_nodes = [ 
            {"node": node, "parents": the_dag.successors(node)}
            for node in networkx.algorithms.dag.topological_sort(the_dag)
        ]
        return { key: ordered_nodes }


    def execute(self, params, *args, **kwargs):
        if params['tasks_json']:
            # short circuit task loading from dodo file if
            # deserializing from json
            params['pos_args'] = args # hack
            params['continue_'] = params.get('continue') # hack
            args_name = inspect.getargspec(self._execute)[0]
            exec_params = dict((n, params[n]) for n in args_name if n != 'self')
            return self._execute(**exec_params)
        else:
            return super(RunTaskManager, self).execute(params, *args, **kwargs)


    def _execute(self, 
                 verbosity=None, always=False, continue_=False,
                 reporter='default', num_process=0, par_type="local",
                 single=False, pipeline_name="Custom Pipeline",
                 **kwargs):
        # **kwargs are thrown away
        the_dag = self._find_dag()
        self._handle_options(par_type)
        self._handle_files(the_dag)
        tm_globals.config.update(self.opt_values.iteritems())

        # create json to send to server
        sigtermSetup()
        daemon = tm_daemon.Tm_daemon()
        daemon.setupTm({ 'dag': the_dag,
                         'type': self.opt_values['par_type'], 
                         'rundirectory': self.rundirectory,
                         'location': self.opt_values['flowdb_path'],
                         'hooks': self.opt_values['hooks_path'],
                         'governor': self.opt_values['GOVERNOR'],
                         'hashdirectory': self.hashdirectory })
        daemon.run(self.opt_values['webgui_port'])


    def _find_dag(self):
        tasks_json = self.opt_values['tasks_json']
        if tasks_json == '-': 
            the_dag = json.load(sys.stdin)
        elif tasks_json:
             with open(tasks_json) as f:
                 the_dag = json.load(f)
        else:
            dag.TMP_FILE_DIR = self.opt_values["tmpfiledir"]
            the_dag = self.generate_dag(self.pipeline_name)

        return the_dag


    def _handle_options(self, par_type):
        self.opt_values['par_type'] = par_type = PAR_TYPES.get(par_type)
        if not par_type:
            msg = "provided par_type %s is not one of %s" %(
                par_type, ", ".join(PAR_TYPES.keys()))
            raise InvalidCommand(msg)

        flowdb_path = os.path.dirname(self.opt_values['flowdb_path'])
        if not os.access(flowdb_path, os.W_OK):
            raise InvalidCommand(
                "directory %s is not writable." % (flowdb_path))

        if self.opt_values['num_process'] == 0:
            self.opt_values['num_process'] += 1

        # make names match up between anadama and task_manager
        mapping = [ ("CLUSTER", "par_type"), ("SOURCE_PATH", "source_path"),
                    ("PORT", "webgui_port"), ("GOVERNOR", "num_process"),
                    ("HOOKS", "hooks_path"), ("TEMP_PATH", "flowdb_path") ]
        for a, b in mapping:
            self.opt_values[a] = self.opt_values[b]
        

    def _handle_files(self, task_dag):
        hash_str, product = peekForHash(task_dag)
        self.hashdirectory = self.opt_values['TEMP_PATH'] + '/' + hash_str
        
        run = "/run1"
        if not os.path.exists(self.hashdirectory):
            os.makedirs(self.hashdirectory)
            self.rundirectory = self.hashdirectory + run
        else:
            # get the last run and increment it by one
            runnum = 1
            for run in os.walk(self.hashdirectory).next()[1]:
                #print "run: " + run
                curnum = int(run[3:])
                if runnum < curnum:
                    runnum = curnum
            runnum += 1;
            #print "runnum: " + str(runnum)
            self.rundirectory = self.hashdirectory + "/run" + str(runnum)

        os.makedirs(self.rundirectory)

        # create link to directory if it doesn't exist
        symlinkdir = self.opt_values['TEMP_PATH'] + "/by_task_name"
        if not os.path.exists(symlinkdir):
            os.makedirs(symlinkdir)

        symlink = symlinkdir + "/" + product
        #print "symlink: " + symlink
        if not os.path.exists(symlink):
            print "symlink: " + self.hashdirectory + "  " + symlink
            os.symlink(self.hashdirectory, symlink)



    def help(self):
        text = super(RunTaskManager, self).help()
        return text + HELP.replace("%prog", RunTaskManager.name)


def sigtermSetup():
    signal.signal(signal.SIGTERM, sigtermHandler)
    signal.signal(signal.SIGINT, sigtermHandler)


def sigtermHandler(signum, frame):
    print "caught signal " + str(signum)
    print "cleaning up..."
    tm_daemon.Tm_daemon.cleanup()
    print "shutting down webserver..."
    sys.exit(0)

