# -*- coding: utf-8 -*-

import os
import random
import shutil
import unittest
from collections import defaultdict

import six
from six import StringIO
import networkx as nx
from networkx.algorithms.traversal.depth_first_search import dfs_edges

import anadama2
import anadama2.slurm
import anadama2.backends
from anadama2.util import find_on_path
from anadama2.util import capture

def bern(p):
    return random.random() < p

PARTITION = os.environ.get("ANASLURM_TEST_PARTITION")
TMPDIR = os.environ.get("ANASLURM_TMPDIR")

srun_exists = all(map(bool, (find_on_path("srun"), PARTITION, TMPDIR) ))


class TestSlurm(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ[anadama2.backends.ENV_VAR] = "/tmp/anadamatest"
    
    @classmethod
    def tearDownClass(cls):
        if os.path.isdir("/tmp/anadamatest"):
            shutil.rmtree("/tmp/anadamatest")


    def setUp(self):
        powerup = anadama2.slurm.SlurmPowerup(PARTITION, TMPDIR)
        self.workdir = "/tmp/anadama_testdir"
        if not os.path.isdir(self.workdir):
            os.mkdir(self.workdir)
        cfg = anadama2.cli.Configuration()
        cfg._directives['output'].default = self.workdir
        self.ctx = anadama2.workflow.Workflow(vars=cfg, grid_powerupt=powerup)


    def tearDown(self):
        if self.ctx._backend:
            self.ctx._backend.close()
            del self.ctx._backend
            self.ctx._backend = None
            anadama2.backends._default_backend = None
            
        if os.path.isdir(self.workdir):
            shutil.rmtree(self.workdir)
        
    @unittest.skipUnless(srun_exists, "requires srun")
    def test_randomgraph_tasks(self):
        G = nx.gn_graph(20)
        targets = defaultdict(dict)
        depends = defaultdict(dict)
        shall_fail = set([ random.choice(G.nodes())
                           for _ in range(int(len(G)**.5)) ])
        nodes = nx.algorithms.dag.topological_sort(G)
        task_nos = [None for _ in range(len(nodes))]
        for n in nodes:
            cmd = "touch /dev/null "
            name = None
            if n in shall_fail:
                cmd += " ;exit 1"
                name = "should fail"
            slurm_add_task = lambda *a, **kw: self.ctx.grid_add_task(mem=50, time=5, cores=1, *a, **kw)
            add_task = self.ctx.add_task if bern(0.5) else slurm_add_task
            t = add_task(
                cmd, name=name,
                depends=[self.ctx.tasks[task_nos[a]] for a in G.predecessors(n)]
            )
            task_nos[n] = t.task_no

        with capture(stderr=StringIO()):
            with self.assertRaises(anadama2.workflow.RunFailed):
                self.ctx.go(grid_jobs=2)
        child_fail = set()
        for n in shall_fail:
            task_no = task_nos[n]
            self.assertIn(task_no, self.ctx.failed_tasks,
                          ("tasks that raise exceptions should be marked"
                           " as failed"))
            self.assertTrue(bool(self.ctx.task_results[task_no].error),
                            "Failed tasks should have errors in task_results")
            for _, succ in dfs_edges(G, n):
                s_no = task_nos[succ]
                child_fail.add(succ)
                self.assertIn(s_no, self.ctx.failed_tasks,
                              "all children of failed tasks should fail")
                self.assertIn("parent task", self.ctx.task_results[s_no].error,
                              ("children of failed tasks should have errors"
                               " in task_results"))
        for n in set(nodes).difference(shall_fail.union(child_fail)):
            task_no = task_nos[n]
            self.assertIn(task_no, self.ctx.completed_tasks)
            self.assertFalse(bool(self.ctx.task_results[task_no].error))
        


    @unittest.skipUnless(srun_exists, "requires srun")
    def test_randomgraph_files(self):
        G = nx.gn_graph(20)
        targets = defaultdict(dict)
        depends = defaultdict(dict)
        allfiles = list()
        for a, b in G.edges():
            f = os.path.join(self.workdir, "{}_{}.txt".format(a,b))
            allfiles.append(f)
            targets[a][b] = depends[b][a] = f
        shall_fail = set([ random.choice(G.nodes())
                           for _ in range(int(len(G)**.5)) ])
        nodes = nx.algorithms.dag.topological_sort(G)
        task_nos = [None for _ in range(len(nodes))]
        for n in nodes:
            cmd = "touch /dev/null "+ " ".join(targets[n].values())
            if n in shall_fail:
                cmd += " ;exit 1"
            slurm_add_task = lambda *a, **kw: self.ctx.grid_add_task(mem=50, time=5, cores=1, *a, **kw)
            add_task = self.ctx.add_task if bern(0.5) else slurm_add_task
            t = add_task(cmd, name=cmd,
                         targets=list(targets[n].values()),
                         depends=list(depends[n].values()))
            task_nos[n] = t.task_no
        self.assertFalse(any(map(os.path.exists, allfiles)))
        with capture(stderr=StringIO()):
            with self.assertRaises(anadama2.workflow.RunFailed):
                self.ctx.go(grid_jobs=2)
        child_fail = set()
        for n in shall_fail:
            task_no = task_nos[n]
            self.assertIn(task_no, self.ctx.failed_tasks,
                          ("tasks that raise exceptions should be marked"
                           " as failed"))
            self.assertTrue(bool(self.ctx.task_results[task_no].error),
                            "Failed tasks should have errors in task_results")
            for _, succ in dfs_edges(G, n):
                s_no = task_nos[succ]
                child_fail.add(succ)
                self.assertIn(s_no, self.ctx.failed_tasks,
                              "all children of failed tasks should fail")
                self.assertIn("parent task", self.ctx.task_results[s_no].error,
                              ("children of failed tasks should have errors"
                               " in task_results"))
        for n in set(nodes).difference(shall_fail.union(child_fail)):
            task_no = task_nos[n]
            self.assertIn(task_no, self.ctx.completed_tasks)
            self.assertFalse(bool(self.ctx.task_results[task_no].error))


    @unittest.skipUnless(srun_exists, "requires srun")
    def test_slurm_do(self):
        self.ctx.grid_do("echo true > @{true.txt}", time=5, mem=50, cores=1)
        self.assertFalse(os.path.exists("true.txt"))
        with capture(stderr=StringIO()):
            self.ctx.go()
        self.assertTrue(os.path.exists("true.txt"))
        os.remove("true.txt")
                                                                    
