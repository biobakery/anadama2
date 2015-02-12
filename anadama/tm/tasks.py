#!/usr/bin/env python
import os, sys
import logging
import subprocess, tempfile
import tornado
import signal
import hashlib
import time
import globals
import re
import shutil
import psutil

class Enum(set):
    """ duplicates basic java enum functionality """
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError
    def __setattr__(self, name, value):
        print "__setattr__"
    def __delattr__(self, name):
        print "__delattr__"

Status = Enum(['WAITING', 'QUEUED', 'RUNNING', 'FINISHED'])
Type = Enum(['LOCAL', 'SLURM', 'LSF', 'EC2'])
Result = Enum(['NA', 'SUCCESS', 'RUN_PREVIOUSLY', 'FAILURE'])

class Task(object):
    """ Parent class for all tasks """

    def __init__(self, jsondata, taskList, fifo):
        self.taskList = taskList
        self.fifo = fifo
        self.directory = "NA"
        self.completed = False
        self.json_node = jsondata['node']
        self.json_parents = jsondata['parents']
        self.setStatus(Status.WAITING)
        self.taskType = Type.EC2
        self.result = Result.NA
        self.return_code = None
        self.pid = None
        self.parents = []
        self.tm = None
        self.runTime = 0
        self.retryMemoryIndex = 0
        self.retryQueueIndex = 0
        self.pipeline = self.json_node.get('pipeline_name')
        self.logfileno = None
        self.logscriptno = None
        # before any tasks are run, find out if this task already ran
        if self.doAllProductsExist():
            self.setCompleted(True)
            self.setStatus(Status.FINISHED)
            self.result = Result.RUN_PREVIOUSLY

    def markIncomplete(self):
        self.setCompleted(False)
        self.setStatus(Status.WAITING)
        self.result = Result.NA

    def getTaskNum(self):
        try:
            return self.num
        except AttributeError:
            return "00"

    def setTaskNum(self, num):
        self.num = num

    def getPipelineName(self):
        return self.pipeline

    def getTaskId(self):
        return str(self.getTaskNum()) + "-" + self.getSimpleName()

    def getTaskList(self):
        return self.taskList
 
    def setScriptfile(self, path):
        self.setOutputDirectory(path)
        self.scriptfile = os.path.join(path, str(self.getTaskNum()) + "-" + self.getSimpleName() + ".sh")

    def getScriptfile(self):
        return self.scriptfile

    def getLogfile(self):
        filename, ext = os.path.splitext(self.getScriptfile())
        return filename + ".log"

    def setCompleted(self, flag):
        self.completed = flag

    def isComplete(self):
        return self.completed

    def setReturnCode(self, code):
        self.return_code = code

    def getReturnCode(self):
        return self.return_code

    def getRuntime(self):
        if self.runTime == 0:
            return 0
        else:
            rtime = int(time.time()) - self.runTime
            return rtime

    def getRetryMemoryIndex(self):
        return self.retryMemoryIndex

    def setRetryMemoryIndex(self, counter):
        self.retryMemoryIndex+= counter

    def getRetryQueueIndex(self):
        return self.retryQueueIndex

    def setRetryQueueIndex(self, counter):
        self.retryQueueIndex+= counter
    
    def run(self, callback):
        #print "in parent Task class setting status to RUNNING"
        self.setStatus(Status.RUNNING)
        self.tm = callback
        self.runtime = int(time.time())

    def canRun(self):
        for parentId in self.getParentIds():
            if not self.taskList[parentId].isComplete():
                return False
        for dependency in self.json_node['depends']:
            if (not os.path.isfile(dependency)):
                print "dependency not found: " + dependency
                return False
        return True

    def doAllProductsExist(self):
        """ Method should only be used prior to running any tasks! """
        for product in self.json_node['produces']:
            if (not os.path.isfile(product)):
                if (not os.path.isdir(product)):
                    print >> sys.stderr, self.getSimpleName() + " " + product + " NOT FOUND."
                    return False
        return True

    def hasFailed(self):
        if self.getStatus() == Status.FINISHED:
            if self.getResult() == Result.FAILURE:
                return True
            if self.getResult() == Result.NA:
                print "Warning: task " + self.getName() + " is complete but result is NA"
        # next check if our parents have failed
        for parentId in self.getParentIds():
            if self.taskList[parentId].hasFailed():
                self.setStatus(Status.FINISHED)
                self.setResult(Result.FAILURE)
                self.setCompleted(True)
                return True
        return False

    def getParentIds(self):
        idList = []
        for parent in self.json_parents:
            idList.append(parent['id'])
        return idList

    def getStatus(self):
        if not self.isComplete() and self.return_code is not None:
            if self.return_code == 0:
                print >> sys.stderr, "FINISHED TASK: " + self.getTaskId()
                self.setStatus(Status.FINISHED)
                self.result = Result.SUCCESS
            else:
                print >> sys.stderr, "FINISHED (FAILED) TASK: " + self.getName() + " " + str(self.getReturnCode())
                print >> sys.stderr, " logfile: " + self.getLogfile()
                self.setStatus(Status.FINISHED)
                self.result = Result.FAILURE
            self.setCompleted(True)
        if self.status == Status.FINISHED and self.result == Result.NA:
            print "Warning: task " + self.getName() + " status is " + self.status + " but result is " + self.result +" and completed is " + str(self.isComplete())
        return self.status

    def setStatus(self, givenStatus):
        if (givenStatus in Status):
            self.status = givenStatus
            if self.fifo is not None:
                print >> self.fifo, "Task " + self.getTaskId() + " status is now " + givenStatus
        else: 
            logging.error("Setting task status to unknown status: " + givenStatus)

    def setResult(self, givenResult):
        if givenResult in Result:
            self.result = givenResult
        else:
            logging.error("Setting task result to unknown result: " + givenResult)

    def getResult(self):
        return self.result

    def getClientStatus(self):
        if self.getStatus() == Status.FINISHED:
            return self.getResult()
        else: 
            return self.getStatus()

    def getProducts(self):
        return self.json_node['produces']

    def getType(self):
        return self.taskType

    def getId(self):
        return self.json_node['id']

    def getPickleScript(self):
        return self.json_node['command']

    def getName(self):
        return self.json_node['name']

    def getSimpleName(self):
        return self.getName().split(':')[0]

    def getJson(self):
        return self.json_node

    def setOutputDirectory(self, givenDir):
        self.directory = givenDir

    def callback(self, exit_code):
        if self.logfileno is not None:
            if self.logscriptno is not None:
                self.logscriptno.seek(0)
                for line in self.logscriptno:
                    self.logfileno.write(line)
                    self.logscriptno.flush()
                self.logscriptno.close()
            self.logfileno.flush()
            self.logfileno.close()

        if self.pid is not None:
            self.publishLogfile()
            self.setReturnCode(exit_code)
            self.tm.runQueue()

    def cleanupProducts(self):
        for product in self.getProducts():
            if os.path.exists(product):
                print "removing " + product
                if os.path.isfile(product):
                    os.unlink(product)
                elif os.path.isdir(product):
                    # safety test
                   dirs = [self.getOutputDirectory(), product]
                   if os.path.commonprefix(dirs) == self.getOutputDirectory():
                       shutil.rmtree(product)
                   else:
                       print >> sys.stderr, "Warning: attempting to remove directory " + product

    def cleanup(self):
        # remove scripts
        # print "cleaning up " + self.getName()
        if os.path.exists(self.getPickleScript()):
            os.unlink(self.getPickleScript())
        if os.path.exists(self.getScriptfile()):
            os.unlink(self.getScriptfile())

    def killRun(self):
        try:
            p = psutil.Process(self.pid.pid)
            for child in p.children(True):
                os.kill(child.pid, signal.SIGTERM)
            os.kill(self.pid.pid, signal.SIGTERM)
        except Exception as e:
            print >> sys.stderr, self.getName() + " job removed."
        # set our pid to None as a flag for any job callbacks to ignore the results
        self.pid = None

    def cleanupFailure(self):
        ''' Cleanup method is called if the task has failed or the
            task manager is killed for some reason.  This removes
            the potentially unfinished products from the file system
            so the task is recognised as needing to be executed for
            the next run.  It also removes the pickle script'''

        self.killRun()
        self.cleanupProducts()
        self.cleanup()

    def callHook(self):
        ''' Method spawns a child process that easily allows user defined things
            to run after the task completes.  This hook calls hooks/post_task_success.sh
            or hooks/post_task_failure.sh based on the task result. '''
        self.setupEnvironment()
        #path = os.path.dirname(os.path.realpath(__file__))
        hooks = self.tm.getHooks()
        hook = str()
        if self.getResult() == Result.SUCCESS:
            hook = os.path.join(hooks, "post_task_success.sh")
        elif self.getResult() == Result.FAILURE:
            hook = os.path.join(hooks, "post_task_failure.sh")
        if os.path.exists(hook):
            print >> sys.stderr, "hook: " + hook
            subprocess.call(hook, shell=True)
        else:
            print >> sys.stderr, "hook: " + hook + " not found."


    def setupEnvironment(self):
        os.environ["TaskName"] = self.getName()
        os.environ["TaskResult"] = self.getResult()
        os.environ["TaskReturnCode"] = str(self.getReturnCode())
        os.environ["TaskScriptfile"] = self.getScriptfile()
        os.environ["TaskLogfile"] = self.getLogfile()
        productfiles = str()
        for file in self.getProducts():
            productfiles += file + " "
        os.environ["TaskProducts"] = productfiles
        if productfiles:
            os.environ["TaskOutputDirectory"] = productfiles.split()[0].split('mibc_products')[0] + 'mibc_products/'

    def ancestorIncomplete(self):
        if self.getName() == "root":
            return False
        for parentId in self.getParentIds(): 
            if not self.taskList[parentId].isComplete():
                return True
            if self.taskList[parentId].ancestorIncomplete():
                return True
        return False 

    def isAncestor(self, targetTask):
        """ method returns true if the targetTask is an ancestor; false otherwise
        """

        if self == targetTask:
            return True
        else:
            for parentId in self.getParentIds():
                if self.taskList[parentId].isAncestor(targetTask):
                    return True
        return False

    def publishLogfile(self):
        """ Copy the logfile to sit beside each task product with the log's name as the
            product with the extension of '.log.html'
        """
        for product in self.getProducts():
            if os.path.exists(product):
                shutil.copy2(self.getLogfile(), product + ".log.html")

    def __str__(self):
        return "Task: " + self.json_node['name']



class LocalTask(Task):
    """ Tasks run on local workstation"""

    def __init__(self, jsondata, taskList, fifo):
        super(LocalTask, self).__init__(jsondata, taskList, fifo)
        self.taskType = Type.LOCAL

    def run(self, callback):
        super(LocalTask, self).run(callback)
        # create subprocess script
        script = self.getScriptfile()
        sub = """#!/bin/sh
source {SOURCE_PATH}
cat - <<EOF
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.4.11/d3.min.js"></script>
<script src='https://cpettitt.github.io/project/dagre-d3/v0.2.9/dagre-d3.min.js'></script>
<script src='https://ajax.googleapis.com/ajax/libs/jquery/1.11.0/jquery.min.js' ></script>

<div align=center>
<H5> <B>Run Status: </B></H5>
<table border='1' cellspacing=5 cellpadding=5>
<TR><Td>date<Td> `date`
<TR><td>taskname<td> {taskname}
</table> <P> </div>

<style>
#dag svg {{
    border: 1px solid #999;
}}

#dag text {{
    font-weight: 300;
        font-family: "Helvetica Neue", Helvetica, Arial, sans-serf;
            font-size: 14px;
}}

#dag rect {{
    fill: #fff;
}}

#dag .node rect {{
    stroke-width: 2px;
        stroke: #333;
            fill: none;
}}

#dag .node:hover rect {{
    stroke: #828b9e;
        fill:   #eaeaea;
}}

#dag .edge rect {{
    fill: #fff;
}}

#dag .edge path {{
    fill: none;
        stroke: #333;
            stroke-width: 1.5px;
}}

#dag .edgePath path {{
    fill: none;
        stroke: #333;
            stroke-width: 1.5px;
}}

</style>

<p>DAG: <span id='dag-name'></span></p>
<div id='dag'><svg height='80'><g transform='translate(20, 20)'/></svg></div>

<script> 

var graph = {graph}
                                                                                                                        
var nodes = graph.nodes;
var links = graph.links;
var graphElem = jQuery('#dag > svg').children('g').get(0);
var svg = d3.select(graphElem);
var renderer = new dagreD3.Renderer();
var layout = dagreD3.layout().rankDir('LR');
renderer.layout(layout).run(dagreD3.json.decode(nodes, links), svg.append('g'));

// Adjust SVG height to content
var main = jQuery('#dag > svg').find('g > g');
var h = main.get(0).getBoundingClientRect().height;
var w = main.get(0).getBoundingClientRect().width;
var newHeight = h + 40;
var newWidth = w + 40;
newHeight = newHeight < 80 ? 80 : newHeight;
newWidth = newWidth < 768 ? 768 : newWidth;
jQuery('#dag > svg').height(newHeight);
jQuery('#dag > svg').width(newWidth);
</script>

<UL> dependencies: 
EOF
for dep in {deps}; do
  echo "<LI> $dep </LI>"
done
echo "</UL>"

echo "<UL> products: "
for prod in {products}; do
  echo "<LI> $prod </LI>"
done
echo "</UL>"

echo "<P>"
echo "-- task cmd output --<br>" 
echo "<PRE>" 
/usr/bin/env time -p {pickle} -v 
cmd_exit=`echo $?`
echo "</PRE>" 
echo "<br>-- end task cmd output --<br>" 
exit $cmd_exit
            """.format(taskname=self.getTaskId(), 
                       deps=self.json_node['depends'],
                       products=self.json_node['produces'],
                       SOURCE_PATH=globals.config['SOURCE_PATH'],
                       pickle=self.getPickleScript(),
                       graph=self.tm.getJsonTaskGraph(self))
        with open(script, 'w+') as f:
          f.write(sub)

        os.chmod(script, 0755)
        scriptpath = os.path.abspath(script)
        self.logfileno = open(self.getLogfile(), "w")
        self.pid = tornado.process.Subprocess([scriptpath], stdout=self.logfileno, stderr=self.logfileno)
        self.pid.set_exit_callback(self.callback)

    def __str__(self):
        str = """Task: {name}
                 Result: {result}
                 Status: {status}""".format(name=self.getName(), result = self.result, status=self.status)
        return str


class LSFTask(Task):
    """ Tasks run on LSF queue """

    def __init__(self, jsondata, taskList, fifo):
        super(LSFTask, self).__init__(jsondata, taskList, fifo)
        self.taskType = Type.LSF

    def run(self, callback):
        super(LSFTask, self).run(callback)
        # create subprocess script
        #print >> sys.stderr, "task_id: " + self.getTaskId()
        cluster_script = os.path.join(globals.config['TEMP_PATH'], self.getTaskId() + ".sh")
        sub = """#!/bin/sh
#BSUB {CLUSTER_QUEUE}
source {SOURCE_PATH}
{picklescript} -v
"""     .format(CLUSTER_QUEUE=globals.config['CLUSTER_QUEUE'],
                       CLUSTER_JOB=globals.config['CLUSTER_JOB'],
                       SOURCE_PATH=globals.config['SOURCE_PATH'],
                       picklescript=self.getPickleScript())
        with open(cluster_script, 'w+') as f:
          f.write(sub)
        os.chmod(cluster_script, 0755)

        monitor_script =  os.path.join(globals.config['TEMP_PATH'], self.getTaskId() + "-monitor.sh")
        sub = """#!/bin/sh
# kickoff and monitor LSF cluster job
source {SOURCE_PATH}
cat - <<EOF
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.4.11/d3.min.js"></script>
<script src='https://cpettitt.github.io/project/dagre-d3/v0.2.9/dagre-d3.min.js'></script>
<script src='https://ajax.googleapis.com/ajax/libs/jquery/1.11.0/jquery.min.js' ></script>

<div align=center>
<H5> <B>Run Status: </B></H5>
<table border='1' cellspacing=5 cellpadding=5>
<TR><Td>date<Td> `date`
<TR><td>taskname<td> {taskname}
</table> <P> </div>

<style>
#dag svg {{
    border: 1px solid #999;
}}

#dag text {{
    font-weight: 300;
            font-family: "Helvetica Neue", Helvetica, Arial, sans-serf;
                        font-size: 14px;
}}

#dag rect {{
    fill: #fff;
}}

#dag .node rect {{
    stroke-width: 2px;
            stroke: #333;
                        fill: none;
}}

#dag .node:hover rect {{
    stroke: #828b9e;
            fill:   #eaeaea;
}}

#dag .edge rect {{
    fill: #fff;
}}

#dag .edge path {{
    fill: none;
            stroke: #333;
                        stroke-width: 1.5px;
}}

#dag .edge path {{
    fill: none;
            stroke: #333;
                        stroke-width: 1.5px;
}}

#dag .edgePath path {{
    fill: none;
            stroke: #333;
                        stroke-width: 1.5px;
}}

</style>

<p>DAG: <span id='dag-name'></span></p>
<div id='dag'><svg height='80'><g transform='translate(20, 20)'/></svg></div>
<script> 

var graph = {graph}

var nodes = graph.nodes;
var links = graph.links;
var graphElem = jQuery('#dag > svg').children('g').get(0);
var svg = d3.select(graphElem);
var renderer = new dagreD3.Renderer();
var layout = dagreD3.layout().rankDir('LR');
renderer.layout(layout).run(dagreD3.json.decode(nodes, links), svg.append('g'));

// Adjust SVG height to content
var main = jQuery('#dag > svg').find('g > g');
var h = main.get(0).getBoundingClientRect().height;
var w = main.get(0).getBoundingClientRect().width;
var newHeight = h + 40;
var newWidth = w + 40;
newHeight = newHeight < 80 ? 80 : newHeight;
newWidth = newWidth < 768 ? 768 : newWidth;
jQuery('#dag > svg').height(newHeight);
jQuery('#dag > svg').width(newWidth);
</script>
<UL> dependencies: 
EOF
for dep in {deps}; do
  echo "<LI> $dep </LI>"
done
echo "</UL>"

echo "<UL> products: "
for prod in {products}; do
  echo "<LI> $prod </LI>"
done
echo "</UL>"
echo "<P>"

export DK_ROOT="/broad/software/dotkit";
. /broad/software/dotkit/ksh/.dk_init
use LSF 

which bsub 
if [ $? != 0 ];then
  echo "can't find use" 
  reuse LSF 
fi

# launch job

jobid_str=`eval {CLUSTER_JOB} {CLUSTER_PROJECT} {CLUSTER_MEMORY} {CLUSTER_QUEUE} {CLUSTER_JOBNAME} {taskname} {CLUSTER_OUTPUT_PARAM} {BATCHLOGFILE} < "{cluster_script}"`
job_id=`echo ${{jobid_str}} | awk -v i={CLUSTER_JOBID_POS} '{{printf $i}}' | sed -e 's/^.//' -e 's/.$//'`
echo "<BR>job_id: +${{job_id}}+"
done="no"
last_output=""
while [ "${{done}}" != "yes" ]; do

  sleep 60

  raw_output=`{CLUSTER_QUERY} ${{job_id}}`
  regex="${{job_id}}"
  output=`{CLUSTER_QUERY} ${{job_id}} | grep "$regex" | awk -v i={CLUSTER_STATUS_POS} '{{printf $i}}'`
  if [ "${{last_output}}" = "${{output}}" ]; then
    echo -n "."
  else
    echo "<BR>batch run: ${{output}}" 
    last_output=${{output}}
  fi

  case ${{output}} in

    PEND)      ;;
    RUN)       ;;
    DONE)      STATUS="OK" && done="yes";;
    COMPLETED) STATUS="OK" && done="yes";;
    EXIT)      STATUS="FAILED" && done="yes";;
    FAILED)    STATUS="FAILED" && done="yes";;
    CANCELLED|CANCELLED+) STATUS="CANCEL" && done="yes";;
    TIMEOUT)   export STATUS="TIMEOUT" && done="yes";;
    NODE_FAIL) export STATUS="RETRY" && done="yes";;
    *)         ;;

  esac

done

echo "<BR>"
echo "<PRE>"
# append job log to our monitor log
if [ -f {BATCHLOGFILE} ]; then
  cat {BATCHLOGFILE} >> {LOGFILE}
  rm {BATCHLOGFILE}
fi
echo "</PRE>"
if [ "$STATUS" != "OK" ]; then
    exit -1
else 
    exit 0
fi
EOF
"""     .format(cluster_script=cluster_script,
                   CLUSTER_JOBID_POS=globals.config["CLUSTER_JOBID_POS"],
                   CLUSTER_QUERY=globals.config["CLUSTER_QUERY"],
                   CLUSTER_STATUS_POS=globals.config["CLUSTER_STATUS_POS"],
                   CLUSTER_JOB=globals.config["CLUSTER_JOB"],
                   CLUSTER_PROJECT=globals.config["CLUSTER_PROJECT"],
                   CLUSTER_MEMORY=globals.config["CLUSTER_MEMORY"][self.getRetryMemoryIndex()],
                   CLUSTER_QUEUE=globals.config["CLUSTER_QUEUE"][self.getRetryQueueIndex()],
                   CLUSTER_JOBNAME=globals.config["CLUSTER_JOBNAME"],
                   CLUSTER_OUTPUT_PARAM=globals.config["CLUSTER_OUTPUT_PARAM"],
                   BATCHLOGFILE=self.getLogfile() + ".batch",
                   LOGFILE=self.getLogfile(),
                   SOURCE_PATH=globals.config['SOURCE_PATH'],
                   graph=self.tm.getJsonTaskGraph(self),
                   products=self.json_node['produces'],
                   deps=self.json_node['depends'],
                   taskname=self.getTaskId())

        with open(monitor_script, 'w+') as f:
            f.write(sub)

        os.chmod(monitor_script, 0755)
        scriptpath = os.path.abspath(monitor_script)
        self.logfileno = open(self.getLogfile(), "w")
        self.pid = tornado.process.Subprocess([scriptpath], stdout=self.logfileno, stderr=self.logfileno)
        self.pid.set_exit_callback(self.callback)

    def cleanup(self):
        jobid = None
        super(LSFTask, self).cleanup()
        # attempt to get our jobid from the custer
        if os.path.exists(self.getLogfile()):
            with open(self.getLogfile()) as f:
                jobid = re.search(r'job_id: \+(\d+)\+', f.read())
        if jobid is not None:
            # kill the LSF job on the cluster
            sub='''#!/bin/sh
eval export DK_ROOT="/broad/software/dotkit";                                                                        
. /broad/software/dotkit/ksh/.dk_init 
use LSF 
bkill {jobid}
        '''.format(jobid=jobid.group(1))
            subprocess.call(sub, shell=True)


    def __str__(self):
        str = """Task: {name}
                 Result: {result}
                 Status: {status}""".format(name=self.getName(), result = self.result, status=self.status)
        return str


class SlurmTask(Task):
    """ Tasks run on Slurm queue """

    def __init__(self, jsondata, taskList, fifo):
        super(SlurmTask, self).__init__(jsondata, taskList, fifo)
        self.taskType = Type.SLURM

    def run(self, callback):
        super(SlurmTask, self).run(callback)
        # create subprocess script
        #print >> sys.stderr, "task_id: " + self.getTaskId()
        cluster_script = os.path.join(globals.config['TEMP_PATH'], self.getTaskId() + ".sh")
        sub = """#!/bin/sh
source {SOURCE_PATH}
{picklescript} -v
"""     .format(CLUSTER_QUEUE=globals.config['CLUSTER_QUEUE'],
                       CLUSTER_JOB=globals.config['CLUSTER_JOB'],
                       SOURCE_PATH=globals.config['SOURCE_PATH'],
                       OUTPUT=self.getLogfile(),
                       picklescript=self.getPickleScript())
        with open(cluster_script, 'w+') as f:
          f.write(sub)
        os.chmod(cluster_script, 0755)

        monitor_script =  os.path.join(globals.config['TEMP_PATH'], self.getTaskId() + "-monitor.sh")
        sub = """#!/bin/sh
# kickoff and monitor SLURM cluster job
source {SOURCE_PATH}
cat - <<EOF
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/3.4.11/d3.min.js"></script>
<script src='https://cpettitt.github.io/project/dagre-d3/v0.2.9/dagre-d3.min.js'></script>
<script src='https://ajax.googleapis.com/ajax/libs/jquery/1.11.0/jquery.min.js' ></script>

<div align=center>
<H5> <B>Run Status: </B></H5>
<table border='1' cellspacing=5 cellpadding=5>
<TR><Td>date<Td> `date`
<TR><td>taskname<td> {taskname}
</table> <P> </div>

<style>
#dag svg {{
    border: 1px solid #999;
}}

#dag text {{
    font-weight: 300;
            font-family: "Helvetica Neue", Helvetica, Arial, sans-serf;
                        font-size: 14px;
}}

#dag rect {{
    fill: #fff;
}}

#dag .node rect {{
    stroke-width: 2px;
            stroke: #333;
                        fill: none;
}}

#dag .node:hover rect {{
    stroke: #828b9e;
            fill:   #eaeaea;
}}

#dag .edge rect {{
    fill: #fff;
}}

#dag .edge path {{
    fill: none;
            stroke: #333;
                        stroke-width: 1.5px;
}}

#dag .edge path {{
    fill: none;
            stroke: #333;
                        stroke-width: 1.5px;
}}

#dag .edgePath path {{
    fill: none;
            stroke: #333;
                        stroke-width: 1.5px;
}}

</style>

<p>DAG: <span id='dag-name'></span></p>
<div id='dag'><svg height='80'><g transform='translate(20, 20)'/></svg></div>
<script> 

var graph = {graph}

var nodes = graph.nodes;
var links = graph.links;
var graphElem = jQuery('#dag > svg').children('g').get(0);
var svg = d3.select(graphElem);
var renderer = new dagreD3.Renderer();
var layout = dagreD3.layout().rankDir('LR');
renderer.layout(layout).run(dagreD3.json.decode(nodes, links), svg.append('g'));

// Adjust SVG height to content
var main = jQuery('#dag > svg').find('g > g');
var h = main.get(0).getBoundingClientRect().height;
var w = main.get(0).getBoundingClientRect().width;
var newHeight = h + 40;
var newWidth = w + 40;
newHeight = newHeight < 80 ? 80 : newHeight;
newWidth = newWidth < 768 ? 768 : newWidth;
jQuery('#dag > svg').height(newHeight);
jQuery('#dag > svg').width(newWidth);
</script>
<UL> dependencies: 
EOF
for dep in {deps}; do
  echo "<LI> $dep </LI>"
done
echo "</UL>"

echo "<UL> products: "
for prod in {products}; do
  echo "<LI> $prod </LI>"
done
echo "</UL>"
echo "<P>"

. {SOURCE_PATH}

which sbatch
if [ $? != 0 ];then
  echo "can't find sbatch" 
fi

# launch job

jobid_str=`eval {CLUSTER_JOB} {CLUSTER_PROJECT} {CLUSTER_MEMORY} {CLUSTER_QUEUE} {CLUSTER_JOBNAME} {taskname} --open-mode=append {CLUSTER_OUTPUT_PARAM} {BATCHLOGFILE} < "{cluster_script}"`
job_id=`echo ${{jobid_str}} | awk -v i={CLUSTER_JOBID_POS} '{{printf $i}}'`
echo "<BR>job_id: +${{job_id}}+"
done="no"
last_output=""
while [ "${{done}}" != "yes" ]; do

  sleep 60

  raw_output=`{CLUSTER_QUERY} ${{job_id}}`
  regex="${{job_id}}[\t ]"
  output=`{CLUSTER_QUERY} ${{job_id}} | grep "$regex" | awk -v i={CLUSTER_STATUS_POS} '{{printf $i}}'`
  if [ "${{last_output}}" = "${{output}}" ]; then
    echo -n "."
  else
    echo "<BR>batch run: ${{output}}" 
    last_output=${{output}}
  fi

  case ${{output}} in

    PEND)      ;;
    RUN)       ;;
    DONE)      STATUS="OK" && done="yes";;
    COMPLETED) STATUS="OK" && done="yes";;
    EXIT)      STATUS="FAILED" && done="yes";;
    FAILED)    STATUS="FAILED" && done="yes";;
    CANCELLED|CANCELLED+) STATUS="CANCEL" && done="yes";;
    TIMEOUT)   export STATUS="TIMEOUT" && done="yes";;
    NODE_FAIL) export STATUS="RETRY" && done="yes";;
    *)         ;;

  esac

done

echo "<BR>"
echo "<PRE>"
# append job log to our monitor log
if [ -f {BATCHLOGFILE} ]; then
  cat {BATCHLOGFILE} >> {LOGFILE}
  rm {BATCHLOGFILE}
fi

echo "</PRE>"
{CLUSTER_STATS} "${{job_id}}"
if [ "$STATUS" != "OK" ]; then
    exit -1
else 
    exit 0
fi
EOF
"""     .format(cluster_script=cluster_script,
                   CLUSTER_JOBID_POS=globals.config["CLUSTER_JOBID_POS"],
                   CLUSTER_QUERY=globals.config["CLUSTER_QUERY"],
                   CLUSTER_STATUS_POS=globals.config["CLUSTER_STATUS_POS"],
                   CLUSTER_JOB=globals.config["CLUSTER_JOB"],
                   CLUSTER_PROJECT=globals.config["CLUSTER_PROJECT"],
                   CLUSTER_MEMORY=globals.config["CLUSTER_MEMORY"][self.getRetryMemoryIndex()],
                   CLUSTER_QUEUE=globals.config["CLUSTER_QUEUE"][self.getRetryQueueIndex()],
                   CLUSTER_JOBNAME=globals.config["CLUSTER_JOBNAME"],
                   CLUSTER_OUTPUT_PARAM=globals.config["CLUSTER_OUTPUT_PARAM"],
                   CLUSTER_STATS=globals.config["CLUSTER_STATS"],
                   BATCHLOGFILE=self.getLogfile() + ".batch",
                   LOGFILE=self.getLogfile(),
                   SOURCE_PATH=globals.config['SOURCE_PATH'],
                   graph=self.tm.getJsonTaskGraph(self),
                   products=self.json_node['produces'],
                   deps=self.json_node['depends'],
                   taskname=self.getTaskId())

        with open(monitor_script, 'w+') as f:
            f.write(sub)

        os.chmod(monitor_script, 0755)
        scriptpath = os.path.abspath(monitor_script)
        self.logfileno = open(self.getLogfile(), "a+")
        self.pid = tornado.process.Subprocess([scriptpath], stdout=self.logfileno, stderr=self.logfileno)

        self.pid.set_exit_callback(self.callback)

    def cleanup(self):
        jobid = None
        super(SlurmTask, self).cleanup()
        # attempt to get our jobid from the custer
        if os.path.exists(self.getLogfile()):
            with open(self.getLogfile()) as f:
                jobid = re.search(r'job_id: \+(\d+)\+', f.read())
        if jobid is not None:
            # kill the SLURM job on the cluster
            sub='''#!/bin/sh
skill {jobid}
        '''.format(jobid=jobid.group(1))
            subprocess.call(sub, shell=True)


    def __str__(self):
        str = """Task: {name}
                 Result: {result}
                 Status: {status}""".format(name=self.getName(), result = self.result, status=self.status)
        return str

