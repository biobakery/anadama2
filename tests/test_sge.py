import os
import random
import shutil
import unittest
from cStringIO import StringIO
from collections import defaultdict

import networkx as nx
from networkx.algorithms.traversal.depth_first_search import dfs_edges

import anadama
import anadama.sge
import anadama.backends
from anadama.util import find_on_path

from util import capture

def bern(p):
    return random.random() < p

PARTITION = os.environ.get("ANASGE_QUEUE")
TMPDIR = os.environ.get("ANASGE_TMPDIR")

available = all(map(bool, (anadama.sge.available, PARTITION, TMPDIR) ))


class TestSGE(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ[anadama.backends.ENV_VAR] = "/tmp/anadamatest"
    
    @classmethod
    def tearDownClass(cls):
        if os.path.isdir("/tmp/anadamatest"):
            shutil.rmtree("/tmp/anadamatest")


    def setUp(self):
        self.ctx = anadama.sge.SGEContext(PARTITION, TMPDIR)
        self.workdir = "tmp/anadama_testdir"
        if not os.path.isdir(self.workdir):
            os.mkdir(self.workdir)

    def tearDown(self):
        if self.ctx._backend:
            self.ctx._backend.close()
            del self.ctx._backend
            self.ctx._backend = None
            anadama.backends._default_backend = None
            
        if os.path.isdir(self.workdir):
            shutil.rmtree(self.workdir)
        
    @unittest.skipUnless(available, "requires qsub")
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
            sge_add_task = lambda *a, **kw: self.ctx.sge_add_task(mem=50, time=5, cores=1, *a, **kw)
            add_task = self.ctx.add_task if bern(0.5) else sge_add_task
            t = add_task(
                cmd, name=name,
                depends=[self.ctx.tasks[task_nos[a]] for a in G.predecessors(n)]
            )
            task_nos[n] = t.task_no

        with capture(stderr=StringIO()):
            with self.assertRaises(anadama.runcontext.RunFailed):
                self.ctx.go(n_sge_parallel=1)
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
        


    @unittest.skipUnless(available, "requires qsub")
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
            sge_add_task = lambda *a, **kw: self.ctx.sge_add_task(mem=50, time=5, cores=1, *a, **kw)
            add_task = self.ctx.add_task if bern(0.5) else sge_add_task
            t = add_task(cmd, name=cmd,
                         targets=list(targets[n].values()),
                         depends=list(depends[n].values()))
            task_nos[n] = t.task_no
        # self.ctx.fail_idx = task_nos[G.successors(list(shall_fail)[0])[-1]]
        self.assertFalse(any(map(os.path.exists, allfiles)))
        with capture(stderr=StringIO()):
            with self.assertRaises(anadama.runcontext.RunFailed):
                self.ctx.go(n_sge_parallel=1)
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


    @unittest.skipUnless(available, "requires qsub")
    def test_sge_do(self):
        self.ctx.sge_do("echo true > @{true.txt}", time=5, mem=50, cores=1)
        self.assertFalse(os.path.exists("true.txt"))
        with capture(stderr=StringIO()):
            self.ctx.go()
        anadama.sge._wait_on_file("true.txt", rm=False)
        self.assertTrue(os.path.exists("true.txt"))
        os.remove("true.txt")
                                                                    
