import os

import networkx
from doit.exceptions import InvalidCommand

from .. import dag
from ..tm import tm_daemon
from ..tm import globals as tm_globals
from ..tm.cli import peekForHash
from ..tm import tm_daemon

from . import opt_runner, opt_tmpfiles, opt_pipeline_name
from . import AnadamaCmdBase


HELP="""%prog -t type [-i <json encoded inputfile>] [-l location] [-D] [-g governor] [-p port][-k hooks dir]

%prog - TM (Task Manager) parses the tasks contained in the given 
json encoded directed acyclic graph (DAG) file.  This command
is designed to read the json encoded DAG from stdin.  However, a filename 
may be specified for input (-i filename.json) if that method is preferred.
type currently can be one of three values: local, slurm, or lsf.

User supplied 'hooks' are called for the given events:
    post_pipeline_failure.sh  
    post_pipeline_success.sh  
    post_task_failure.sh  
    post_task_success.sh  
    pre_pipeline.sh  
    pre_pipeline_restart.sh
    

NOTE:
-D indicates that the task_manager daemon should be started and will 
process all DAGs sent via a websocket interface to the daemon listening 
on the given '-p' port.  If no daemon is listening on that port, a new
daemon will be started.
"""

RUN_LOCAL = 'local'
PAR_TYPES = {'local': RUN_LOCAL, 
             'process': RUN_LOCAL,
             'thread': RUN_LOCAL,
             'slurm': 'slurm', 
             'lsf': 'lsf'}

opt_flowdb_path = {
    "name": "flows_path",
    "long": "flows_path",
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

opt_governor = {
    "name": "governor",
    "long": "governor",
    "default": 2,
    "help": ("Rate limit the number of concurrent tasks."
             "  Useful for limited resource on local desktops / laptops"),
    "type": int,
}

# This option needs to go away
opt_source_path = {
    "name": "source_path",
    "long": "source_path",
    "default": "/home/vagrant/hmp2/bin/activate",
    "help": ("Where to source the current environment"),
    "type": int,
}

class RunTaskManager(AnadamaCmdBase):
    name = "task_manager"
    doc_purpose = "run tasks with web GUI"
    doc_usage = "[TASK ...]"

    my_opts = (opt_pipeline_name, opt_flowdb_path, opt_tasks_json,
               opt_webgui_port, opt_hooks_path, opt_governor)

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


    def _execute(self, 
                 verbosity=None, always=False, continue_=False,
                 reporter='default', num_process=0, par_type="local",
                 single=False, pipeline_name="Custom Pipeline",
                 **kwargs):
        # **kwargs are thrown away
        if self.opt_values['tasks_json']:
            the_dag = json.loads(self.opt_values['tasks_json'])
        else:
            dag.TMP_FILE_DIR = self.opt_values["tmpfiledir"]
            the_dag = self.generate_dag(self.pipeline_name)
        
        self._handle_options(par_type)
        self._handle_files(the_dag)
        tm_globals.config.update(self.opt_values.iteritems())

        # create json to send to server
        sigtermSetup()
        daemon = tm_daemon.Tm_daemon()
        daemon.setupTm({ 'dag': data,
                         'type': self.opt_values['par_type'], 
                         'rundirectory': self.rundirectory,
                         'location': self.opt_values['flowdb_path'],
                         'hooks': self.opt_values['hooks_path'],
                         'governor': self.opt_values['governor'],
                         'hashdirectory': self.hashdirectory })
        daemon.run()



    def _handle_options(self, par_type):
        self.opt_values['par_type'] = par_type = PAR_TYPES.get(par_type)
        if not par_type:
            msg = "provided par_type %s is not one of %s" %(
                par_type, ", ".join(PAR_TYPES.keys()))
            raise InvalidCommand(msg)

        if not os.access(os.path.dirname(self.opt_values['flowdb_path']), os.W_OK):
            raise InvalidCommand("directory %s is not writable." % (
                self.opt_values['flowdb_path']))

        # make names match up between anadama and task_manager
        mapping = [ ("CLUSTER", "par_type"), ("SOURCE_PATH", "source_path"),
                    ("PORT", "webgui_port"), ("GOVERNOR", "governor"),
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
        symlinkdir = opts.location + "/by_task_name"
        if not os.path.exists(symlinkdir):
            os.makedirs(symlinkdir)

        symlink = symlinkdir + "/" + product
        #print "symlink: " + symlink
        if not os.path.exists(symlink):
            print "symlink: " + self.hashdirectory + "  " + symlink
            os.symlink(self.hashdirectory, symlink)



    @staticmethod
    def help():
        return HELP.replace("%prog", RunTaskManager.name)


def sigtermSetup():
    signal.signal(signal.SIGTERM, sigtermHandler)
    signal.signal(signal.SIGINT, sigtermHandler)


def sigtermHandler(signum, frame):
    print "caught signal " + str(signum)
    print "cleaning up..."
    tm_daemon.Tm_daemon.cleanup()
    print "shutting down webserver..."
    sys.exit(0)

