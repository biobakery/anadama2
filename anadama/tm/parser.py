#!/usr/bin/env python
import os.path
import json
import tasks
import time
import sys
import hashlib
import datetime


class Parser(object):
    """ Parse the json dag passed in via cmdline args """

    def __init__(self, data, taskType, fifo):
        self.taskList = {}
        nodes = data['dag']

        for node in nodes:
            if taskType == tasks.Type.LOCAL:
                task = tasks.LocalTask(node, self.taskList, fifo)
                if task.getId() in self.taskList.keys():
                    print "Error: duplicate task Identifier: " + task.getId()
                    sys.exit(-1)
                self.taskList[task.getId()] = task;
            elif taskType == tasks.Type.LSF:
                task = tasks.LSFTask(node, self.taskList, fifo)
                if task.getId() in self.taskList.keys():
                    print "Error: duplicate task Identifier: " + task.getId()
                    sys.exit(-1)
                self.taskList[task.getId()] = task;
            elif taskType == tasks.Type.SLURM:
                task = tasks.SlurmTask(node, self.taskList, fifo)
                if task.getId() in self.taskList.keys():
                    print "Error: duplicate task Identifier: " + task.getId()
                    sys.exit(-1)
                self.taskList[task.getId()] = task;
            else:
                print "Task type: " + taskType + " not supported (yet)"
                sys.exit(0);

        # set the parent indexes for each task
        #for k, task in self.taskList.iteritems():
        #    for parentId            


    def getTasks(self):
        return self.taskList


    def getTask(self, idx):
        if idx in taskList:
            return taskList[idx]
        print "Error: " + idx + " not in taskList."
        return None

    def getHashTuple(self):
        for k, task in self.getTasks().iteritems():
            if len(task.getProducts()) > 0:
                md5 = hashlib.md5()
                #print "product for hash: " + task.getProducts()[0]
                md5.update(task.getProducts()[0])
                #print "hash: " + md5.hexdigest()
                name = task.getSimpleName() + "." + str(datetime.date.today())
                return (str(md5.hexdigest())[:5], name)

    #def setTaskDir(self, directory):
    #    for k, task in self.getTasks().iteritems():
    #        task.setDirectory(directory)

    def getJsonGraph(self):
        graph = {}
        graph['name'] = "graph1"
        graph['nodes'] = []
        graph['links'] = []

        # nodes
        for k, task in self.getTasks().iteritems():
            nodes = graph['nodes'] 
            nodes.append({'id': task.getName(), 'value': { 'label': task.getSimpleName()}})
        # links
        for k, task in self.getTasks().iteritems():
            links = graph['links'] 
            for parentId in task.getParentIds():
                parent = self.getTasks()[parentId]
                links.append(
                    {'u': parent.getName(), 'v': task.getName(), 
                    'value': { 'label': ''}})

        json_encoded = json.dumps(graph, sort_keys=True, indent=4, separators=(',', ": "))
        #print "graph: " + repr(graph)
        #print "json : " + json_encoded

        return json_encoded
