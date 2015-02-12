#!/usr/bin/env python
import optparse, sys, json, pprint
import tasks, parser
import tm as TM
import tm_daemon
import os
import signal
import tempfile
from pprint import pprint
import websocket
import re
import hashlib
import globals


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

opts_list = [
    optparse.make_option('-v', '--verbose', action="store_true",
                         dest="verbose", default=False,
                         help="Turn on verbose output (to stderr)"),
    optparse.make_option('-i', '--input', action="store", type="string",
                         dest="dagfile", help="Specify json encrypted dag file"),
    optparse.make_option('-l', '--location', action="store", type="string", default="",
                         dest="location", help="Specify directory where all tasks and logfiles are stored.  Default is the current working directory."),
    optparse.make_option('-t', '--type', action="store",
                         dest="type", type="string", 
                         help="The type of task to be run (local, slurm, or lsf)"),
    optparse.make_option('-g', '--governor', action="store", type="int", 
                         dest="governor", help="Rate limit the number of concurrent tasks.  Useful for limited resource on local desktops / laptops."),
    optparse.make_option('-D', '--daemon', action="store_true", 
                         dest="daemon", help="Specify task manager to run as a daemon.  Will process multiple dags."),
    optparse.make_option('-p', '--port', action="store", type="int", 
                         dest="port", help="Specify the port of the webserver - defaults to 8888."),
    optparse.make_option('-k', '--hooks', action="store", type="string", 
                         dest="hooks", help="Specify the directory of the user supplied hooks.")
]


global opts, p, data, wslisteners
types = ('local', 'slurm', 'lsf')
wslisteners = []

def fileHandling():
   
    '''create directory structure to store metadata from tasks'''

    global hashdirectory, rundirectory

    hash, product = peekForHash(data)

    hashdirectory = opts.location + "/" + hash

    # create task directory if it doesn't exist
    run = "/run1"
    if not os.path.exists(hashdirectory):
        os.makedirs(hashdirectory)
        rundirectory = hashdirectory + run
    else:
        # get the last run and increment it by one
        runnum = 1
        for run in os.walk(hashdirectory).next()[1]:
            #print "run: " + run
            curnum = int(run[3:])
            if runnum < curnum:
                runnum = curnum
        runnum += 1;
        #print "runnum: " + str(runnum)
        rundirectory = hashdirectory + "/run" + str(runnum)

    os.makedirs(rundirectory)

    # create link to directory if it doesn't exist
    symlinkdir = opts.location + "/by_task_name"
    if not os.path.exists(symlinkdir):
        os.makedirs(symlinkdir)
        
    symlink = symlinkdir + "/" + product
    #print "symlink: " + symlink
    if not os.path.exists(symlink):
        print "symlink: " + hashdirectory + "  " + symlink
        os.symlink(hashdirectory, symlink)

def optionHandling():
    global opts, data

    if opts.dagfile:
        if not os.path.isfile(opts.dagfile):
            print >> sys.stderr, "opts.dagfile not found."
            argParser.print_usage()
            sys.exit(1)
        else:
            input = opts.dagfile
            if opts.verbose:
                print >> sys.stderr, "Input is: ", opts.dagfile
    else:
        input = "-" 
        if opts.verbose:
            print >> sys.stderr, "Input is stdin."

    if opts.type not in types:
        if globals.config.get("CLUSTER") is None:
            print >> sys.stderr, "type not one of 'local', 'slurm', or 'lsf'."
            argParser.print_usage()
            sys.exit(1)
        else:
            opts.type = globals.config["CLUSTER"].lower()

    if opts.location is "":
        if globals.config.get("TEMP_PATH") is not None:
            opts.location = globals.config["TEMP_PATH"] + "/anadama_flows"
        else:
            opts.location = os.getcwd() + "/anadama_flows"

    if not os.access(os.path.dirname(opts.location), os.W_OK):
        print >> sys.stderr, "directory " + opts.location + " is not writable."
        argParser.print_usage()
        sys.exit(1)

    if opts.port is None or "":
        if globals.config.get("PORT") is None:
            opts.port = 8888
        else:
            opts.port = globals.config["PORT"]

    if opts.governor is None or "":
        if globals.config.get("GOVERNOR") is None:
            opts.governor = 999
        else:
            opts.governor = globals.config["GOVERNOR"]

    if opts.hooks is None or "":
        if globals.config.get("HOOKS") is None:
            opts.hooks = os.path.dirname(os.path.realpath(__file__)) + "/hooks"
        else:
            opts.hooks = globals.config["HOOKS"]
            
    if input is "-":
        jsonString = sys.stdin.read()
        data = json.loads(jsonString);
    else:
        jsonfile = open(opts.dagfile)
        jsondata = jsonfile.read()
        data = json.loads(jsondata);
        jsonfile.close()

def peekForHash(data):
    nodes = data['dag']
    for entry in nodes:
        node = entry['node']
        if 'produces' in node and len(node['produces']) > 0:
            md5 = hashlib.md5()
            md5.update(node['produces'][0])
            name = node['name'].split(':')[0]
            return (str(md5.hexdigest())[:5], name)

                
def main():
    global opts, p, argParser, tm, config

    def sigtermSetup():
        signal.signal(signal.SIGTERM, sigtermHandler)
        signal.signal(signal.SIGINT, sigtermHandler)

    def sigtermHandler(signum, frame):
        print "caught signal " + str(signum)
        print "cleaning up..."
        tm_daemon.Tm_daemon.cleanup()
        print "shutting down webserver..."
        sys.exit(0)

    # load configuration parameters (if they exist)
    globals.init(os.getcwd())

    # read in options
    argParser = optparse.OptionParser(option_list=opts_list,
                                   usage=HELP)
    (opts, args) = argParser.parse_args()
    optionHandling()

    # setup output directories
    fileHandling()
   
    # get our installed location
    currentFile = os.path.realpath(__file__)
    path = os.path.dirname(currentFile)
    web_install_path = os.path.join(path, "anadama_flows")

    # create json to send to server
    d = {'tm': 
            {'dag': data, 
                'type': opts.type, 
                'location': opts.location,
                'rundirectory': rundirectory,
                'hashdirectory': hashdirectory,
                'governor': opts.governor,
                'hooks': opts.hooks}}
    msg = json.dumps(d)
    #print >> sys.stderr, msg

    # if opts.daemon is false; call daemon directly to process single dag in foreground.
    if not opts.daemon:

        # first setup up signals
        sigtermSetup()

        daemon = tm_daemon.Tm_daemon()
        daemon.setupTm(d['tm'])
        daemon.run(opts.port)
        # daemon doesn't return

    # connect to daemon (or start it)
    else:
        done = False
        while not done:
            try:
                ws = websocket.create_connection("ws://localhost:" + str(opts.port) + "/websocket/")
                down = True
            except:
                print >> sys.stderr, "starting daemon..."
                subprocess.popen('supervisord', '-c', 'configuration_supervisord.conf')
                sleep(2)

        ws.send(msg)
        ws.close()

if __name__ == '__main__':
    main()
