#!/usr/bin/env python
import os.path
import json
import tasks
import time
import sys
import subprocess
import traceback

QueueStatus = tasks.Enum(['RUNNING', 'PAUSED', 'STOPPED'])

def nextNum(localDir):
    ''' Returns the next sequencial whole number.  Used for unique task ids. '''
    if True:
        # read counter from disk...
        counterFile=os.path.join(localDir + "/counter.txt")
        if os.path.isfile(counterFile):
            with open(counterFile) as f:
                counter = int(f.readline())
                counter += 1
        else:
            counter = 1 
        with open(counterFile, 'w+') as f:
            f.write(str(counter))
        return counter

class TaskManager(object):
    """ Parse the json dag passed in via cmdline args """

    def __init__(self, taskList, wslisteners, hooks, governor=99):
        self.taskList = taskList
        self.completedTasks = []
        self.waitingTasks = []
        self.queuedTasks = []
        self.governor = governor
        self.saved_governor = governor
        self.hooks = hooks
        self.wslisteners = wslisteners
        self.queueStatus = QueueStatus.RUNNING
        self.root = None
        self.pipeline_output_directory = None
        self.run = 1 # this should be read from filespace...

    def getTasks(self):
        return self.taskList

    def setupQueue(self):
        """ method starts the flow 
            at this point, no tasks have been run and the filesystem is quiet
            assume all tasks that have existing products on the filesystem 
            are complete.  If some products do not exist, assume that none
            exist, removed the other products, and schedule the task.
        """
        firsttime = True
        for key,task in self.taskList.iteritems():
            if task.getName() == 'root':
                self.root = task
                self.completedTasks.append(task)
                task.setCompleted(True)
                self.notify(task)
                task.cleanup()
            elif task.isComplete():
                firsttime = False;
                self.completedTasks.append(task)
                self.notify(task)
                #task.cleanup() do not call cleanup now; further processing
                # may require re-running this task
            else:
                """ task not complete.  Clean up any existing products belonging to
                    the task.  Then, requeue any tasks which rely on this task.
                """
                task.cleanupProducts()

        for key,task in self.taskList.iteritems():
            if task.isComplete() and task.ancestorIncomplete():
                task.markIncomplete()
                self.completedTasks.remove(task)
                task.cleanupProducts()

        for key,task in self.taskList.iteritems():
            if task not in self.completedTasks:
                if task.canRun():
                    task.setStatus(tasks.Status.QUEUED)
                    self.queuedTasks.append(task)
                    self.notify(task)
                else:
                    task.setStatus(tasks.Status.WAITING)
                    self.waitingTasks.append(task)
                    self.notify(task)
            # prevent cleanup until tm is brought down
            #else:
                #task.cleanup()

            # this is a bit of a hack to find our output directory
            # at some point this field should come from the root DAG
            if not self.getOutputDirectory():
                if task.getProducts():
                    self.setOutputDirectory(task.getProducts()[0].split('mibc_products')[0] + 'mibc_products/')
        if firsttime:
            self.callHook("pre")

    def runQueue(self):

        """ attempts to spawn subprocesses in order to 
            launch all tasks in queuedTasks
            For now, this method will run until all jobs
            are finished...
        """
        updates_made = True
        while updates_made:

            updates_made = False
            still_waitingTasks = [];

            # loop thru waiting tasks
            if self.queueStatus != QueueStatus.STOPPED:
                for task in self.waitingTasks:
                    if task.hasFailed():
                        self.completedTasks.append(task)
                        self.notify(task)
                        updates_made = True
                        # We didn't run and we failed so do not invoke hook
                    elif task.canRun():
                        task.setStatus(tasks.Status.QUEUED)
                        self.queuedTasks.append(task)
                        self.notify(task)
                        updates_made = True
                    else:
                        still_waitingTasks.append(task)

                    self.waitingTasks = still_waitingTasks

            # loop thru queued tasks
            still_queuedTasks = []

            if self.queueStatus != QueueStatus.STOPPED:
                for task in self.queuedTasks:
                    if task.getStatus() == tasks.Status.QUEUED:
                        if self.governor > 0:
                            print >> sys.stderr, "governor: " + str(self.governor) + "->" + str(self.governor - 1)
                            self.governor -= 1
                            task.run(self)
                            self.notify(task)
                        still_queuedTasks.append(task)

                    elif task.getStatus() == tasks.Status.FINISHED:
                        print >> sys.stderr, "governor: " + str(self.governor) + "->" + str(self.governor + 1)
                        self.governor += 1
                        # Did the task fail? 
                        if task.getResult() == tasks.Result.FAILURE:
                            # Can we mitigate it?
                            #if task.canRedo():
                            #    pass
                            pass

                        self.completedTasks.append(task)
                        self.notify(task)
                        updates_made = True
                        # We did run this job - call hook based on status
                        task.callHook()
                    elif task.getStatus() == tasks.Status.RUNNING:
                        still_queuedTasks.append(task)

                self.queuedTasks = still_queuedTasks

            elif self.queueStatus == QueueStatus.STOPPED:
                for task in self.queuedTasks:
                    if task.getStatus() == tasks.Status.FINISHED:
                        task.setReturnCode(None)
                        task.setResult(tasks.Result.NA)
                        task.setStatus(tasks.Status.QUEUED)
                        task.setCompleted(False)
                        self.notify(task)
                        # queue has stopped; task will be rolled back; don't call hook

        if self.isQueueEmpty():
            all_success = True
            for completedTask in self.completedTasks:
                if completedTask.getResult() is tasks.Result.FAILURE:
                    all_success = False
            if all_success:
                self.callHook("post_success")
            else:
                self.callHook("post_failure")


    def cleanup(self):
        for task in self.waitingTasks:
            task.cleanup()
        for task in self.completedTasks:
            if task.getResult() == tasks.Result.FAILURE:
                task.cleanupFailure()
            else:
                task.cleanup()
        for task in self.queuedTasks:
            task.cleanupFailure()


    def notify(self, task):
        for listener in self.wslisteners:
            listener.taskUpdate(task)

    def status(self):
        print >> sys.stderr, "=========================="
        #failed = [x for x in self.completedTasks if x.hasFailed()]
        #for task in self.completedTasks:
        #    print "  " + task.getName()
        #print >> sys.stderr, "completed tasks: (" + str(len(self.completedTasks)) + " " + str(len(failed)) + " failed.)"
        #for task in self.completedTasks:
        #    print "  " + task.getName()
        #print >> sys.stderr, "waiting tasks: " + str(len(self.waitingTasks))
        #for task in self.waitingTasks:
        #    print "  " + task.getName()
        #running = [task for task in self.queuedTasks if task.getStatus() == tasks.Status.RUNNING]
        #print >> sys.stderr, "queued tasks: " + str(len(self.queuedTasks)) + " (" + str(len(running)) + " running.)"

        print >> sys.stderr, "waiting tasks: " + str(len(self.waitingTasks))
        print >> sys.stderr, "queued tasks: " + str(len(self.queuedTasks))
        print >> sys.stderr, "finished tasks: " + str(len(self.completedTasks))
        print >> sys.stderr, "governor: " + str(self.governor)

        print >> sys.stderr, "=========================="

    def isQueueEmpty(self):
        if len(self.waitingTasks) > 0:
            return False
        if len(self.queuedTasks) > 0:
            return False
        return True

    def getFifo(self):
        if fifo is not None:
            return fifo
        else:
            print "Warning: fifo is null"
            return None

    def setTaskOutputs(self, rundirectory):
        print "RUNDIRECTORY: " + rundirectory
        for k, task in self.getTasks().iteritems():
            task.setTaskNum(nextNum(rundirectory))
            task.setScriptfile(rundirectory)

    def getStatus(self):
        return self.status

    def getOutputDirectory(self):
        return self.pipeline_output_directory

    def setOutputDirectory(self, dir):
        self.pipeline_output_directory = dir

    def pauseQueue(self):
        print >> sys.stderr, "pauseQueue"
        self.queueStatus = QueueStatus.PAUSED
        self.governor = -99

    def stopQueue(self):
        print >> sys.stderr, "stopQueue"
        self.queueStatus = QueueStatus.STOPPED
        self.governor = -99
        for task in self.queuedTasks:
            if task in self.waitingTasks:
                print >> sys.stderr, "ERROR! " + task.getTaskId() + " is already in waiting list!"
            else:
                self.waitingTasks.insert(0, task)
            if task.getStatus() == tasks.Status.RUNNING:
                print >> sys.stderr, "Task is RUNNING - Killing it now:" + task.getSimpleName() 
                task.killRun()
                task.cleanupProducts()
                task.setCompleted(False)
                task.setReturnCode(None)
                task.setResult(tasks.Result.NA)
            task.setStatus(tasks.Status.WAITING)
            self.notify(task)
        self.queuedTasks = []

    def startQueue(self):
        print >> sys.stderr, "startQueue"
        if self.queueStatus is not QueueStatus.RUNNING:
            self.queueStatus = QueueStatus.RUNNING
            self.governor = self.saved_governor
            self.runQueue()
        print >> sys.stderr, "governor: " + str(self.governor)

    def getQueueStatus(self):
        return self.queueStatus

    def increaseGovernor(self):
        self.governor+=1
        self.saved_governor+=1
        self.runQueue()

    def decreaseGovernor(self):
        self.governor-=1
        self.saved_governor-=1

    def redoTask(self, givenTaskString):
        """ Method retrieves a handle to the real task identified by the given task
            string.  It then forces this task into the taskWaiting Queue.  It then
            traverses the task list looking for downstream tasks to also 'redo'.
            If downstream tasks are running, they are killed and their products are removed.
            Finally, it kicks off the queue to find if anything new can be processed.
        """
        if self.queueStatus != QueueStatus.STOPPED:
            return

        print >> sys.stderr, "redoTask: " + givenTaskString

        givenTask = [task for k,task in self.getTasks().iteritems() if task.getName() == givenTaskString]
        redoTasks = [child for k,child in self.getTasks().iteritems() if child.isAncestor(givenTask[0])]
        for task in redoTasks:
            # always skip root task
            if task.getName() == "root":
                continue
            print >> sys.stderr, "putting " + task.getSimpleName() + " back into waiting queue"
            self.waitingTasks.append(task)
            task.cleanupProducts()
            task.setCompleted(False)
            if task.getStatus() == tasks.Status.WAITING:
                self.notify(task)
                continue
            if task.getStatus() == tasks.Status.QUEUED:
                if task in self.queuedTasks:
                    self.queuedTasks.remove(task)
                task.setStatus(tasks.Status.WAITING)
                self.notify(task)
                continue
            if task.getStatus() == tasks.Status.RUNNING:
                self.queuedTasks.remove(task)
                task.killRun()
                task.cleanupProducts()
                task.setReturnCode(None)
                task.setResult(tasks.Result.NA)
                task.setStatus(tasks.Status.WAITING)
                task.setCompleted(False)
                self.notify(task)
                # reset queue governor
                self.governor += 1
                continue
            if task.getStatus() == tasks.Status.FINISHED:
                self.completedTasks.remove(task)
                task.cleanupProducts()
                task.setReturnCode(None)
                task.setResult(tasks.Result.NA)
                task.setStatus(tasks.Status.WAITING)
                task.setCompleted(False)
                self.notify(task)

        self.runQueue()

    def getHooks(self):
        return self.hooks

    def callHook(self, pipeline):
        ''' Method spawns a child process that easily allows user defined things
            to run after the task completes.  This hook calls hooks/post_task_success.sh
            or hooks/post_task_failure.sh based on the task result. '''
        self.setupEnvironment()
        hook = str()
        if pipeline == "pre":
            hook = os.path.join(self.hooks, "pre_pipeline.sh")
        elif pipeline == "post_success":
            hook = os.path.join(self.hooks, "post_pipeline_success.sh")
        elif pipeline == "post_failure":
            hook = os.path.join(self.hooks, "post_pipeline_failure.sh")

        if os.path.exists(hook):
            print >> sys.stderr, "hook: " + hook
            subprocess.call(hook, shell=True)
        else:
            print >> sys.stderr, "hook: " + hook + " not found."


    def setupEnvironment(self):
        os.environ["TaskName"] = self.root.getName()
        os.environ["TaskResult"] = self.root.getResult()
        os.environ["TaskReturnCode"] = str(self.root.getReturnCode())
        os.environ["TaskScriptfile"] = self.root.getScriptfile()
        os.environ["TaskLogfile"] = self.root.getLogfile()
        os.environ["TaskOutputDirectory"] = self.getOutputDirectory()
        productfiles = str()
        for file in self.root.getProducts():
            productfiles += file + " "
        os.environ["TaskProducts"] = productfiles
        if self.root.getPipelineName() is not None: 
            os.environ["PipelineName"] = self.root.getPipelineName()

    def getJsonTaskGraph(self, task):
        """ Generate graph structure for a single task flowchart
            encompassing all tasks up to the given task.
        """
        graph = {}
        graph['name'] = "graph1"
        graph['nodes'] = []
        graph['links'] = []

        # nodes
        for k, targetTask in self.getTasks().iteritems():
            nodes = graph['nodes']
            links = graph['links']
            if task == targetTask or task.isAncestor(targetTask):
                nodes.append({'id': targetTask.getName(), 'value': { 'label': targetTask.getSimpleName()}})
                for parentId in targetTask.getParentIds():
                    parent = self.getTasks()[parentId]
                    links.append(
                        {'u': parent.getName(), 'v': targetTask.getName(),
                         'value': { 'label': ''}})

        json_encoded = json.dumps(graph, sort_keys=True, indent=4, separators=(',', ": "))
        #print "graph: " + repr(graph)
        #print "json : " + json_encoded
        return json_encoded

